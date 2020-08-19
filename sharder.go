// Copyright 2020 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fluxdb

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/dbin"
	"github.com/dfuse-io/dstore"
	"github.com/golang/protobuf/proto"
	"github.com/minio/highwayhash"
	"go.uber.org/zap"
)

const shardBinaryContentType = "fwr"
const shardBinaryVersion = 1

type Sharder struct {
	mapper      BlockMapper
	shardsStore dstore.Store
	startBlock  uint64
	stopBlock   uint64
	shardCount  int

	// A slice of shards, each shard is itself a slice of WriteRequest, one per block processed in this batch.
	// So, assuming 2 shards with 5 blocks, that would yield `[0][#5, #6, #7, #8, #9], [1][#5, #6, #7, #8, #9]`.
	buffers      []*bytes.Buffer
	dbinEncoders []*dbin.Writer
	statsByShard []stats
}

type stats struct {
	requestCount int
	entriesCount int
	rowsCount    int
}

func NewSharder(shardsStore dstore.Store, shardCount int, startBlock, stopBlock uint64) *Sharder {
	s := &Sharder{
		buffers:      make([]*bytes.Buffer, shardCount),
		dbinEncoders: make([]*dbin.Writer, shardCount),
		statsByShard: make([]stats, shardCount),

		shardCount:  shardCount,
		shardsStore: shardsStore,
		startBlock:  startBlock,
		stopBlock:   stopBlock,
	}

	// FIXME: This is currently all hold in RAM which consumed a lot of RAMs in certain
	//        cases. It would be better if we would write to either a temporary local storage
	//        and then copying over to final destination. Writing directly to storage is probably
	//        not possible for remote backend like GCP or S3 because it might take few minutes to
	//        complete the sharding process and it's likely that this is not possible using
	//        plain HTTP connections that are meant for shorter cycles.
	for i := 0; i < shardCount; i++ {
		buf := bytes.NewBuffer(nil)
		s.buffers[i] = buf
		s.dbinEncoders[i] = dbin.NewWriter(buf)

		// This is coded to never fail, so we safely ignore the `err` return value
		s.dbinEncoders[i].WriteHeader(shardBinaryContentType, shardBinaryVersion)

		s.statsByShard[i] = stats{requestCount: 0, entriesCount: 0, rowsCount: 0}
	}

	return s
}

func (s *Sharder) ProcessBlock(rawBlk *bstream.Block, rawObj interface{}) error {
	if rawBlk.Num()%600 == 0 {
		zlog.Info("processing block (printed each 600 blocks)", zap.Stringer("block", rawBlk))
	}

	fObj := rawObj.(*forkable.ForkableObject)
	if fObj.Step != forkable.StepIrreversible {
		panic("unsupported, received step is not irreversible")
	}

	unshardedRequest := fObj.Obj.(*WriteRequest)
	if unshardedRequest.Height > s.stopBlock {
		err := s.writeShards()
		if err != nil {
			return fmt.Errorf("unable to write shards to store: %w", err)
		}

		return ErrCleanSourceStop
	}

	// Compute the N shard write requests, 1 write request per shard, the slice index is the shard index
	shardedRequests := make([]*WriteRequest, s.shardCount)
	for _, entry := range unshardedRequest.SingletEntries {
		shardIndex := s.goesToShard(KeyForSinglet(entry.Singlet()))

		var shardedRequest *WriteRequest
		if shardedRequest = shardedRequests[shardIndex]; shardedRequest == nil {
			shardedRequest = &WriteRequest{}
			shardedRequests[shardIndex] = shardedRequest
		}

		shardedRequest.SingletEntries = append(shardedRequest.SingletEntries, entry)
	}

	for _, row := range unshardedRequest.TabletRows {
		shardIndex := s.goesToShard(KeyForTablet(row.Tablet()))

		var shardedRequest *WriteRequest
		if shardedRequest = shardedRequests[shardIndex]; shardedRequest == nil {
			shardedRequest = &WriteRequest{}
			shardedRequests[shardIndex] = shardedRequest
		}

		shardedRequest.TabletRows = append(shardedRequest.TabletRows, row)
	}

	// Loop over N shards computed above, and assign them correctly to the global shards slice
	for shardIndex, encoder := range s.dbinEncoders {
		shardedRequest := shardedRequests[shardIndex]
		if shardedRequest == nil {
			shardedRequest = &WriteRequest{}
		}

		shardedRequest.Height = unshardedRequest.Height
		shardedRequest.BlockRef = unshardedRequest.BlockRef

		protoRequest, err := shardedRequest.ToProto()
		if err != nil {
			return fmt.Errorf("request to proto: %w", err)
		}

		message, err := proto.Marshal(protoRequest)
		if err != nil {
			return fmt.Errorf("marshal proto: %w", err)
		}

		if err = encoder.WriteMessage(message); err != nil {
			return fmt.Errorf("encoding message: %w", err)
		}

		s.statsByShard[shardIndex].requestCount++
		s.statsByShard[shardIndex].entriesCount += len(protoRequest.SingletEntries)
		s.statsByShard[shardIndex].rowsCount += len(protoRequest.TabletRows)
	}

	return nil
}

var emptyHashKey [32]byte

func (s *Sharder) goesToShard(key []byte) int {
	bigInt := highwayhash.Sum64(key, emptyHashKey[:])
	elementShard := bigInt % uint64(s.shardCount)
	return int(elementShard)
}

func (s *Sharder) writeShards() error {
	eg := llerrgroup.New(12)
	for shardIndex, buffer := range s.buffers {
		if eg.Stop() {
			break
		}

		shardIndex := shardIndex
		buffer := buffer

		eg.Go(func() error {
			baseName := fmt.Sprintf("%03d/%010d-%010d", shardIndex, s.startBlock, s.stopBlock)

			zlog.Debug("encoding shard",
				zap.String("base_name", baseName),
				zap.Int("shard_index", shardIndex),
				zap.Int("request_count", s.statsByShard[shardIndex].requestCount),
				zap.Int("entry_count", s.statsByShard[shardIndex].entriesCount),
				zap.Int("row_count", s.statsByShard[shardIndex].rowsCount),
			)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			err := s.shardsStore.WriteObject(ctx, baseName, bytes.NewReader(buffer.Bytes()))
			if err != nil {
				return fmt.Errorf("unable to correctly write shard %d: %w", shardIndex, err)
			}

			buffer.Truncate(0)
			return nil
		})
	}
	return eg.Wait()
}

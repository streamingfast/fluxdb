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
	"github.com/abourget/llerrgroup"
	"github.com/minio/highwayhash"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dbin"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path"
	"time"
)

const shardBinaryContentType = "fwr"
const shardBinaryVersion = 1

type Sharder struct {
	mapper           BlockMapper
	shardsStore      dstore.Store
	scratchDirectory string
	startBlock       uint64
	stopBlock        uint64
	shardCount       int

	// A slice of shards, each shard is itself a slice of WriteRequest, one per block processed in this batch.
	// So, assuming 2 shards with 5 blocks, that would yield `[0][#5, #6, #7, #8, #9], [1][#5, #6, #7, #8, #9]`.
	writers      []io.Writer
	dbinEncoders []*dbin.Writer
	statsByShard []stats
}

type stats struct {
	requestCount int
	entriesCount int
	rowsCount    int
	lastBlockRef bstream.BlockRef
	lastHeight   uint64
}

func NewSharder(shardsStore dstore.Store, scratchDirectory string, shardCount int, startBlock, stopBlock uint64) (*Sharder, error) {
	s := &Sharder{
		writers:      make([]io.Writer, shardCount),
		dbinEncoders: make([]*dbin.Writer, shardCount),
		statsByShard: make([]stats, shardCount),

		shardCount:       shardCount,
		shardsStore:      shardsStore,
		scratchDirectory: scratchDirectory,
		startBlock:       startBlock,
		stopBlock:        stopBlock,
	}

	if scratchDirectory != "" {
		if err := os.MkdirAll(scratchDirectory, os.ModePerm); err != nil {
			return nil, fmt.Errorf("unable to create scratch directory: %w", err)
		}
	}

	for i := 0; i < shardCount; i++ {
		var writer io.Writer
		if s.scratchDirectory == "" {
			writer = bytes.NewBuffer(nil)
		} else {
			file, err := os.OpenFile(path.Join(s.scratchDirectory, fmt.Sprintf("shard-%03d-%s.dbin.tmp", i, segmentIdentifier(s.startBlock, s.stopBlock))), os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
			if err != nil {
				return nil, fmt.Errorf("scratch directory for shard %d: %w", i, err)
			}

			writer = file
		}

		s.writers[i] = writer

		// This is coded to never fail, so we safely ignore the `err` return value
		s.dbinEncoders[i] = dbin.NewWriter(writer)
		err := s.dbinEncoders[i].WriteHeader(shardBinaryContentType)
		if err != nil {
			return nil, fmt.Errorf("unable to write header: %w", err)
		}
		s.statsByShard[i] = stats{requestCount: 0, entriesCount: 0, rowsCount: 0, lastBlockRef: bstream.BlockRefEmpty, lastHeight: 0}
	}

	return s, nil
}

func (s *Sharder) ProcessBlock(rawBlk *pbbstream.Block, obj interface{}) error {
	if rawBlk.Number%600 == 0 || tracer.Enabled() {
		zlog.Debug("processing block (printed each 600 blocks)", zap.Stringer("block", rawBlk))
	}

	if !obj.(bstream.Stepable).Step().Matches(bstream.StepIrreversible) {
		panic("unsupported, received step is not irreversible")
	}

	unshardedRequest := obj.(bstream.ObjectWrapper).WrappedObject().(*WriteRequest)
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
		s.statsByShard[shardIndex].lastBlockRef = shardedRequest.BlockRef
		s.statsByShard[shardIndex].lastHeight = shardedRequest.Height
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
	for shardIndex, writer := range s.writers {
		if eg.Stop() {
			break
		}

		shardIndex := shardIndex
		writer := writer

		eg.Go(func() error {
			baseName := path.Join(shardDirectory(shardIndex), segmentIdentifier(s.startBlock, s.stopBlock))

			shardStats := s.statsByShard[shardIndex]
			zlog.Info("encoding shard",
				zap.String("base_name", baseName),
				zap.Int("shard_index", shardIndex),
				zap.Int("request_count", shardStats.requestCount),
				zap.Int("entry_count", shardStats.entriesCount),
				zap.Int("row_count", shardStats.rowsCount),
				zap.Stringer("last_block", shardStats.lastBlockRef),
				zap.Uint64("last_height", shardStats.lastHeight),
			)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			var err error
			if v, ok := writer.(*bytes.Buffer); ok {
				err = s.writeShardRequestsFromMemory(ctx, baseName, v)
			} else if v, ok := writer.(*os.File); ok {
				err = s.writeShardRequestsFromFile(ctx, baseName, v)
			} else {
				panic(fmt.Errorf("don't kown how to handle shard requests writer of type %T", writer))
			}

			if err != nil {
				return fmt.Errorf("unable to correctly write shard %d: %w", shardIndex, err)
			}

			return nil
		})
	}
	return eg.Wait()
}

func (s *Sharder) writeShardRequestsFromMemory(ctx context.Context, name string, buffer *bytes.Buffer) error {
	err := s.shardsStore.WriteObject(ctx, name, bytes.NewReader(buffer.Bytes()))
	if err != nil {
		return err
	}

	buffer.Truncate(0)
	return nil
}

func (s *Sharder) writeShardRequestsFromFile(ctx context.Context, name string, file *os.File) (err error) {
	defer file.Close()

	offset, err := file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("seek file: %w", err)
	}

	if offset != 0 {
		return fmt.Errorf("unable to return to start of file, offset %d received is not 0", offset)
	}

	err = s.shardsStore.WriteObject(ctx, name, file)
	if err != nil {
		return fmt.Errorf("write to store: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	if err := os.Remove(file.Name()); err != nil {
		return fmt.Errorf("remove file: %w", err)
	}

	return nil
}

func shardDirectory(shardIndex int) string {
	return fmt.Sprintf("%03d", shardIndex)
}

func segmentIdentifier(startBlock, stopBlock uint64) string {
	return fmt.Sprintf("%010d-%010d", startBlock, stopBlock)
}

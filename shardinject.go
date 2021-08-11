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
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/streamingfast/dbin"
	"github.com/streamingfast/dstore"
	pbfluxdb "github.com/streamingfast/pbgo/dfuse/fluxdb/v1"
	"github.com/streamingfast/shutter"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

type ShardInjector struct {
	*shutter.Shutter

	shardsStore dstore.Store
	db          *FluxDB
}

func NewShardInjector(shardsStore dstore.Store, db *FluxDB) *ShardInjector {
	return &ShardInjector{
		Shutter:     shutter.New(),
		shardsStore: shardsStore,
		db:          db,
	}
}

func (s *ShardInjector) Run() (err error) {
	ctx, cancelInjector := context.WithCancel(context.Background())
	s.OnTerminating(func(_ error) {
		cancelInjector()
	})

	// FIXME (height): Probably a revisit of the sharding will be required if we move off block to height directly. At the same time,
	//                 it could still be bound to block and still use height
	_, startAfter, err := s.db.FetchLastWrittenCheckpoint(ctx)
	if err != nil {
		return err
	}

	zlog.Info("starting back shard injector", zap.Stringer("block", startAfter))
	startAfterNum := uint64(startAfter.Num())

	// This expects an ordered walking of all files, so it's an important requierements on the backing store
	err = s.shardsStore.Walk(ctx, "", "", func(filename string) error {
		fileFirst, fileLast, err := parseFileName(filename)
		if err != nil {
			return err
		}

		if fileFirst > startAfterNum+1 {
			return fmt.Errorf("file %s starts at block %d, we were expecting to start right after %d, there is a hole in your block range files", filename, fileFirst, startAfter)
		}
		if fileLast <= startAfterNum {
			zlog.Info("skipping shard file", zap.String("filename", filename), zap.Uint64("start_after", startAfterNum))
			return nil
		}

		zlog.Info("processing shard file", zap.String("filename", filename))

		reader, err := s.shardsStore.OpenObject(ctx, filename)
		if err != nil {
			return fmt.Errorf("opening object from shards store %q: %w", filename, err)
		}
		defer reader.Close()

		requests, err := ReadShard(reader, startAfterNum)
		if err != nil {
			return fmt.Errorf("unable to read all write requests in batch %q: %w", filename, err)
		}

		if err := s.db.WriteBatch(ctx, requests); err != nil {
			return fmt.Errorf("write batch %q: %w", filename, err)
		}

		startAfterNum = fileLast
		return nil
	})

	if err != nil {
		return fmt.Errorf("walking shards store: %w", err)
	}

	return nil
}

func parseFileName(filename string) (first, last uint64, err error) {
	vals := strings.Split(filename, "-")
	if len(vals) != 2 {
		err = fmt.Errorf("cannot parse filename %q", filename)
		return
	}

	first64, err := strconv.ParseUint(vals[0], 10, 32)
	if err != nil {
		return 0, 0, err
	}
	first = uint64(first64)

	last64, err := strconv.ParseUint(vals[1], 10, 32)
	if err != nil {
		return 0, 0, err
	}
	last = uint64(last64)

	return
}

func ReadShard(reader io.Reader, startAfter uint64) ([]*WriteRequest, error) {
	dbinDecoder := dbin.NewReader(reader)
	contentType, version, err := dbinDecoder.ReadHeader()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	if contentType != shardBinaryContentType || version != shardBinaryVersion {
		return nil, fmt.Errorf("file with content type %q and version %d is unsupported, supporting %q at version %d", contentType, version, shardBinaryContentType, shardBinaryVersion)
	}

	var requests []*WriteRequest
	for {
		msg, err := dbinDecoder.ReadMessage()
		if msg != nil {
			protoRequest := pbfluxdb.WriteRequest{}
			if proto.Unmarshal(msg, &protoRequest); err != nil {
				return nil, fmt.Errorf("unmarshal request: %w", err)
			}

			if protoRequest.Height <= startAfter {
				continue
			}

			req, err := NewWriteRequestFromProto(&protoRequest)
			if err != nil {
				return nil, fmt.Errorf("request from proto: %w", err)
			}

			requests = append(requests, req)
			continue
		}

		if err == io.EOF {
			return requests, nil
		}

		if err != nil {
			return nil, fmt.Errorf("read write request message: %w", err)
		}
	}
}

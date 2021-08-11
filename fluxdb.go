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

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/fluxdb/store"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type FluxDB struct {
	*shutter.Shutter
	store       store.KVStore
	source      bstream.Source
	blockMapper BlockMapper
	blockFilter func(blk *bstream.Block) error

	idxCache              *indexCache
	disableIndexing       bool
	ignoreIndexRangeStart uint64
	ignoreIndexRangeStop  uint64

	SpeculativeWritesFetcher func(ctx context.Context, headBlockID string, upToHeight uint64) (speculativeWrites []*WriteRequest)
	HeadBlock                func(ctx context.Context) bstream.BlockRef

	shardIndex int
	shardCount int
	stopBlock  uint64

	ready bool
}

func New(kvStore store.KVStore, blockFilter func(blk *bstream.Block) error, blockMapper BlockMapper, disableIndexing bool) *FluxDB {
	return &FluxDB{
		Shutter:         shutter.New(),
		store:           kvStore,
		blockFilter:     blockFilter,
		blockMapper:     blockMapper,
		idxCache:        newIndexCache(),
		disableIndexing: disableIndexing,
	}
}

func (fdb *FluxDB) Launch(disablePipeline bool) {
	fdb.OnTerminating(func(e error) {
		if fdb.source != nil {
			zlog.Info("shutting down fluxdb's source")
			fdb.source.Shutdown(e)
			zlog.Info("source shutdown")
		}
	})

	if disablePipeline {
		zlog.Info("not using a pipeline, waiting forever (serve mode)")
		fdb.SpeculativeWritesFetcher = func(ctx context.Context, headBlockID string, upToHeight uint64) (speculativeWrites []*WriteRequest) {
			return nil
		}

		fdb.HeadBlock = func(ctx context.Context) bstream.BlockRef {
			// FIXME (height): Will need to be revisited here for height support
			_, lastWrittenBlock, err := fdb.FetchLastWrittenCheckpoint(ctx)
			if err != nil {
				fdb.Shutdown(fmt.Errorf("failed fetching the last written block: %w", err))
				return bstream.BlockRefEmpty
			}
			return lastWrittenBlock
		}

		<-fdb.Terminating()
		zlog.Info("fluxdb server completed")

	} else {
		// running the pipeline, this call is blocking
		zlog.Info("starting pipeline")
		fdb.source.Run()
		<-fdb.source.Terminating()

		err := fdb.source.Err()

		zlog.Info("fluxdb source shutdown", zap.Error(err))
		fdb.Shutdown(err)
	}

	return
}

func (fdb *FluxDB) SetSharding(shardIndex, shardCount int) {
	fdb.shardIndex = shardIndex
	fdb.shardCount = shardCount
}

func (fdb *FluxDB) SetStopBlock(stopBlock uint64) {
	fdb.stopBlock = stopBlock
}

func (fdb *FluxDB) SetIgnoreIndexRange(startBlock, stopBlock uint64) {
	fdb.ignoreIndexRangeStart = startBlock
	fdb.ignoreIndexRangeStop = stopBlock
}

func (fdb *FluxDB) IsSharding() bool {
	return fdb.shardCount != 0
}

func (fdb *FluxDB) Close() error {
	return fdb.store.Close()
}

func (fdb *FluxDB) IsReady() bool {
	return fdb.ready
}

// SetReady marks the process as ready, meaning it has crossed the
// "close to real-time" threshold.
func (fdb *FluxDB) SetReady() {
	fdb.ready = true
}

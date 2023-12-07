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
	"errors"
	"fmt"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/stream"
	"github.com/streamingfast/fluxdb/store"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var ErrNotReady = errors.New("fluxdb not ready")
var ErrRequestedBlockNotFound = errors.New("requested block not found")

type FluxDB struct {
	*shutter.Shutter
	store       store.KVStore
	source      *stream.Stream
	blockMapper BlockMapper
	blockFilter func(blk *pbbstream.Block) error

	idxCache                   *indexCache
	disableIndexing            bool
	ignoreIndexRangeStart      uint64
	ignoreIndexRangeStop       uint64
	disableLastCheckpointWrite bool

	speculativeWritesFetcher     func(ctx context.Context, optionalRequestBlock bstream.BlockRef) (speculativeWrites []*WriteRequest, atFinalBlock uint64, err error)
	headBlockFetcher             func(ctx context.Context) bstream.BlockRef
	reversibleBlockWritesFetcher func(ctx context.Context, hash string) (bstream.BlockRef, *WriteRequest)

	shardIndex int
	shardCount int
	stopBlock  uint64

	ready bool
}

type Option func(*FluxDB)

func WithDisableIndexing() Option {
	return func(fdb *FluxDB) {
		fdb.disableIndexing = true
	}
}

func WithSkipLastCheckpointWrite() Option {
	return func(fdb *FluxDB) {
		fdb.disableLastCheckpointWrite = true
	}
}

func New(kvStore store.KVStore, blockFilter func(blk *pbbstream.Block) error, blockMapper BlockMapper, disableIndexing bool, opts ...Option) *FluxDB {
	fdb := &FluxDB{
		Shutter:         shutter.New(),
		store:           kvStore,
		blockFilter:     blockFilter,
		blockMapper:     blockMapper,
		idxCache:        newIndexCache(),
		disableIndexing: disableIndexing,
	}

	for _, opt := range opts {
		opt(fdb)
	}

	return fdb
}

func (fdb *FluxDB) Launch(ctx context.Context, disablePipeline bool) {
	if disablePipeline {
		zlog.Info("not using a pipeline, waiting forever (serve mode)")
		<-fdb.Terminating()
		zlog.Info("fluxdb server completed")
		return

	}
	// running the pipeline, this call is blocking
	zlog.Info("starting pipeline")
	err := fdb.source.Run(ctx)

	zlog.Info("fluxdb source shutdown", zap.Error(err))
	fdb.Shutdown(err)
}

// SpeculativeWrites returns the speculative writes up to optionalRequestBlock (or to HEAD if nil).
//
// * `optionalRequestBlock` may contain only a number (with id="")
// * It also returns atFinalBlock so you know from where you should apply speculativeWrites
// * It should return ErrNotReady if there is currently no headBlock,
// * or ErrRequestedBlockNotFound if it cannot match the optionalRequestBlock.
func (fdb *FluxDB) SpeculativeWrites(ctx context.Context, optionalRequestBlock bstream.BlockRef) (speculativeWrites []*WriteRequest, atFinalBlock uint64, err error) {
	if fdb.speculativeWritesFetcher != nil {
		return fdb.speculativeWritesFetcher(ctx, optionalRequestBlock)
	}
	return fdb.defaultSpeculativeWritesFetcher(ctx, optionalRequestBlock)
}

// HeadBlock simply returns the highest block in the chain
func (fdb *FluxDB) HeadBlock(ctx context.Context) bstream.BlockRef {
	if fdb.headBlockFetcher != nil {
		return fdb.headBlockFetcher(ctx)
	}
	return fdb.defaultHeadBlockFetcher(ctx)
}

// ReversibleBlockWrites is a public function that will return the reference to a reversible
// block that matches the received hash.
//
// If the block is not found or there is no live pipeline configured internally,
// the returned value is `nil`, `nil`.
//
// If the block is found, the speculative write request for this block is returned as the
// second returned value.
//
// If block hash is found in our reversible segment, both returned value will be non `nil`.
func (fdb *FluxDB) ReversibleBlockWrites(ctx context.Context, hash string) (bstream.BlockRef, *WriteRequest) {
	if fdb.reversibleBlockWritesFetcher != nil {
		return fdb.reversibleBlockWritesFetcher(ctx, hash)
	}
	return fdb.defaultReversibleBlockWritesFetcher(ctx, hash)
}

func (fdb *FluxDB) defaultSpeculativeWritesFetcher(ctx context.Context, optionalRequestBlock bstream.BlockRef) (speculativeWrites []*WriteRequest, atFinalBlock uint64, err error) {
	head := fdb.HeadBlock(ctx)
	if head == nil {
		return nil, 0, ErrNotReady
	}
	if optionalRequestBlock != nil {
		if optionalRequestBlock.Num() > head.Num() {
			return nil, 0, ErrRequestedBlockNotFound
		}
		return nil, optionalRequestBlock.Num(), nil
	}
	return nil, 0, nil
}

func (fdb *FluxDB) defaultHeadBlockFetcher(ctx context.Context) bstream.BlockRef {
	// FIXME (height): Will need to be revisited here for height support
	_, lastWrittenBlock, err := fdb.FetchLastWrittenCheckpoint(ctx)
	if err != nil {
		fdb.Shutdown(fmt.Errorf("failed fetching the last written block: %w", err))
		return bstream.BlockRefEmpty
	}
	return lastWrittenBlock
}

func (fdb *FluxDB) defaultReversibleBlockWritesFetcher(ctx context.Context, hash string) (bstream.BlockRef, *WriteRequest) {
	return nil, nil
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

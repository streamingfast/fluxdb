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
	"sync"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/blockstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/bstream/hub"
	"github.com/streamingfast/bstream/stream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/fluxdb/metrics"
	"go.uber.org/zap"
)

var ErrCleanSourceStop = errors.New("clean source stop")

func BuildReprocessingPipeline(
	blockFilter func(blk *bstream.Block) error,
	blockMapper BlockMapper,
	handler bstream.Handler,
	blocksStore dstore.Store,
	startBlock uint64,
) (*stream.Stream, error) {
	fdbPreprocessor := NewPreprocessBlock(blockMapper)
	filePreprocessor := bstream.PreprocessFunc(func(blk *bstream.Block) (interface{}, error) {
		if blockFilter != nil {
			err := blockFilter(blk)
			if err != nil {
				return nil, fmt.Errorf("block filter: %w", err)
			}
		}

		return fdbPreprocessor(blk)
	})

	source := stream.New(nil, blocksStore, nil, int64(startBlock), handler,
		stream.WithPreprocessFuncDefaultThreadNumber(filePreprocessor),
		stream.WithLogger(zlog),
	)

	return source, nil
}

func (fdb *FluxDB) BuildPipeline(
	ctx context.Context,
	getBlockID bstream.EternalSourceStartBackAtBlock,
	handler bstream.Handler,
	blocksStore dstore.Store,
	oneBlocksStore dstore.Store,
	blockStreamAddr string,
	speculativeWritesFetcher func(ctx context.Context, optionalRequestBlock bstream.BlockRef) (speculativeWrites []*WriteRequest, atFinalBlock uint64, err error),
	headBlockFetcher func(ctx context.Context) bstream.BlockRef,
	reversibleBlockWritesFetcher func(ctx context.Context, hash string) (bstream.BlockRef, *WriteRequest),
) error {
	live := blockStreamAddr != ""

	zlog.Info("building pipeline",
		zap.String("block_stream_addr", blockStreamAddr),
		zap.Bool("live", live),
	)

	fdbPreprocessor := NewPreprocessBlock(fdb.blockMapper)
	preprocessor := bstream.PreprocessFunc(func(blk *bstream.Block) (interface{}, error) {
		if fdb.blockFilter != nil {
			err := fdb.blockFilter(blk)
			if err != nil {
				return nil, fmt.Errorf("block filter: %w", err)
			}
		}
		return fdbPreprocessor(blk)
	})

	liveSourceFactory := bstream.SourceFactory(func(h bstream.Handler) bstream.Source {
		return blockstream.NewSource(
			ctx,
			blockStreamAddr,
			1,
			bstream.NewPreprocessor(preprocessor, h),
		)
	})

	oneBlocksSourceFactory := bstream.SourceFromNumFactoryWithSkipFunc(func(num uint64, h bstream.Handler, skipFunc func(string) bool) bstream.Source {
		src, err := bstream.NewOneBlocksSource(num, oneBlocksStore, h, bstream.OneBlocksSourceWithSkipperFunc(skipFunc))
		if err != nil {
			return nil
		}
		return src
	})

	fhub := hub.NewForkableHub(liveSourceFactory, oneBlocksSourceFactory, 500,
		forkable.WithLogger(zlog),
		forkable.WithFilters(bstream.StepNew|bstream.StepIrreversible),
	)
	go func() {
		fhub.Run()
	}()

	if live {
		select {
		case <-fhub.Ready:
		case <-ctx.Done():
			return fmt.Errorf("forkable hub not ready: %w", ctx.Err())
		}
	}

	startBlock, err := getBlockID()
	if err != nil {
		return fmt.Errorf("get block id: %w", err)
	}

	source := stream.New(nil, blocksStore, fhub, int64(startBlock.Num()), handler,
		stream.WithPreprocessFuncDefaultThreadNumber(preprocessor),
		stream.WithLogger(zlog),
		stream.WithCustomStepTypeFilter(bstream.StepNew|bstream.StepIrreversible),
	)

	fdb.source = source
	if reversibleBlockWritesFetcher != nil {
		fdb.reversibleBlockWritesFetcher = reversibleBlockWritesFetcher
	}
	if headBlockFetcher != nil {
		fdb.headBlockFetcher = headBlockFetcher
	}
	if speculativeWritesFetcher != nil {
		fdb.speculativeWritesFetcher = speculativeWritesFetcher
	}
	return nil
}

// FluxDBHandler is a pipeline that writes in FluxDB
type FluxDBHandler struct {
	db  *FluxDB
	ctx context.Context

	writeEnabled                bool
	writeOnEachIrreversibleStep bool
	serverForkDB                *forkable.ForkDB
	headBlock                   bstream.BlockRef
	lib                         bstream.BlockRef

	speculativeWrites    []*WriteRequest
	speculativeReadsLock sync.RWMutex

	batchWrites       []*WriteRequest
	batchOpen         time.Time
	batchClose        time.Time
	batchWritableRows int

	lastBlockIDCheck time.Time
}

func NewHandler(db *FluxDB) *FluxDBHandler {
	return &FluxDBHandler{
		db:        db,
		ctx:       context.Background(),
		headBlock: bstream.BlockRefEmpty,
	}
}

func (p *FluxDBHandler) EnableWrites() {
	p.writeEnabled = true
}

func (p *FluxDBHandler) EnableWriteOnEachIrreversibleStep() {
	p.writeOnEachIrreversibleStep = true
}

func (p *FluxDBHandler) InitializeStartBlockID() (startBlock bstream.BlockRef, err error) {
	_, startBlock, err = p.db.FetchLastWrittenCheckpoint(p.ctx)
	if err != nil {
		return nil, err
	}

	zlog.Info("initializing pipeline forkdb", zap.Stringer("block", startBlock))
	p.serverForkDB = forkable.NewForkDB(forkable.ForkDBWithLogger(zlog))
	if !bstream.EqualsBlockRefs(startBlock, bstream.BlockRefEmpty) {
		// If we are the empty block ref, we are going to initialize the LIB with a manual call
		// to `SetLIB` later on in the pipeline from the first received streamable block directly.
		// We do not because we do not know yet the actual block's hash of the first streamable block
		// of the chain.
		//
		// See comment tagged 69f60031aecb1e0ee5a9b7876ea492f2 (search for it in project)
		p.serverForkDB.InitLIB(startBlock)
	}

	return startBlock, nil
}

func (p *FluxDBHandler) HeadBlock(ctx context.Context) bstream.BlockRef {
	p.speculativeReadsLock.RLock()
	defer p.speculativeReadsLock.RUnlock()

	return p.headBlock
}

func (p *FluxDBHandler) ReversibleBlock(ctx context.Context, hash string) (bstream.BlockRef, *WriteRequest) {
	// ForkDB is already protected, no need to hold any lock here
	blk := p.serverForkDB.BlockForID(hash)
	if blk == nil {
		return nil, nil
	}

	return blk.AsRef(), blk.Object.(*WriteRequest)
}

func (p *FluxDBHandler) FetchSpeculativeWrites(ctx context.Context, optionalRequestBlock bstream.BlockRef) (speculativeWrites []*WriteRequest, atFinalBlock uint64, err error) {
	p.speculativeReadsLock.RLock()
	defer p.speculativeReadsLock.RUnlock()

	atFinalBlock = p.lib.Num()

	head := p.headBlock
	if head == nil {
		return nil, 0, ErrNotReady
	}

	if optionalRequestBlock != nil && optionalRequestBlock.Num() > head.Num() {
		return nil, 0, ErrRequestedBlockNotFound
	}

	if isUpToHead := optionalRequestBlock == nil ||
		optionalRequestBlock.ID() == head.ID() ||
		(optionalRequestBlock.ID() == "" && optionalRequestBlock.Num() == head.Num()); isUpToHead {

		for _, write := range p.speculativeWrites {
			speculativeWrites = append(speculativeWrites, write)
		}
		return
	}

	if upToBlockRef := optionalRequestBlock; upToBlockRef.ID() != "" {
		if writes := p.fetchSpeculativeWritesForBlockRefInCurrentChain(upToBlockRef); writes != nil {
			speculativeWrites = writes
			return
		}

		speculativeWrites, err = p.fetchSpeculativeWritesForBlockRefInForkDB(p.lib, upToBlockRef)
		return
	}

	speculativeWrites = p.fetchSpeculativeWritesForBlockNumInCurrentChain(optionalRequestBlock.Num())
	return
}

func (p *FluxDBHandler) updateSpeculativeWrites(lib bstream.BlockRef, newHeadBlock bstream.BlockRef) {
	newWrites, err := p.fetchSpeculativeWritesForBlockRefInForkDB(lib, newHeadBlock)
	if err != nil {
		panic(err)
	}

	p.speculativeReadsLock.Lock()
	defer p.speculativeReadsLock.Unlock()

	p.speculativeWrites = newWrites
	p.headBlock = newHeadBlock
	p.lib = lib
}

// this lookup method is faster than looking in forkdb if it works
func (p *FluxDBHandler) fetchSpeculativeWritesForBlockRefInCurrentChain(upToBlockRef bstream.BlockRef) []*WriteRequest {
	var speculativeWrites []*WriteRequest
	for _, write := range p.speculativeWrites {
		speculativeWrites = append(speculativeWrites, write)
		if write.BlockRef.ID() == upToBlockRef.ID() {
			return speculativeWrites
		}
	}
	return nil
}

func (p *FluxDBHandler) fetchSpeculativeWritesForBlockNumInCurrentChain(upToBlockNum uint64) (speculativeWrites []*WriteRequest) {
	for _, write := range p.speculativeWrites {
		if write.Height > upToBlockNum {
			return
		}
		speculativeWrites = append(speculativeWrites, write)
	}
	return
}

func (p *FluxDBHandler) fetchSpeculativeWritesForBlockRefInForkDB(lib bstream.BlockRef, blockRef bstream.BlockRef) ([]*WriteRequest, error) {
	// If the requested block is below our LIB, than it means it's a non-speculative block and there is no write requests to process here
	if lib != nil && blockRef.Num() > bstream.GetProtocolFirstStreamableBlock && blockRef.Num() < lib.Num() {
		return nil, ErrRequestedBlockNotFound
	}

	blocks, reachedLIB := p.serverForkDB.ReversibleSegment(blockRef)
	if !reachedLIB {
		return nil, ErrRequestedBlockNotFound
	}

	if len(blocks) == 0 && blockRef.Num() == bstream.GetProtocolFirstStreamableBlock && p.serverForkDB.Exists(blockRef.ID()) {
		blocks = append(blocks, p.serverForkDB.BlockForID(blockRef.ID())) // first streamable block is the LIB, so never appears in ReversibleSegment
	}

	if len(blocks) == 0 {
		return nil, nil
	}

	newWrites := make([]*WriteRequest, len(blocks))
	for i, blk := range blocks {
		newWrites[i] = blk.Object.(*WriteRequest)
	}

	return newWrites, nil
}

func (p *FluxDBHandler) ProcessBlock(rawBlk *bstream.Block, rawObj interface{}) error {
	blkRef := rawBlk.AsRef()
	if rawBlk.Num()%600 == 0 || tracer.Enabled() {
		zlog.Info("processing block (printed each 600 blocks)", zap.Stringer("block", blkRef))
	}

	// TODO: move to bstream's interface that matches this, not the actual ForkableObject, when other step-related fields are exported
	step := rawObj.(bstream.Stepable).Step()
	wrappedObj := rawObj.(bstream.ObjectWrapper).WrappedObject()

	if step.Matches(bstream.StepNew) {
		metrics.HeadBlockTimeDrift.SetBlockTime(rawBlk.Time())
		metrics.HeadBlockNumber.SetUint64(rawBlk.Num())
		if !p.db.IsReady() {
			if isNearRealtime(rawBlk, time.Now()) && !bstream.EqualsBlockRefs(p.HeadBlock(context.Background()), bstream.BlockRefEmpty) {
				zlog.Info("realtime blocks flowing, marking process as ready")
				p.db.SetReady()
			}
		}

		previousID := rawBlk.PreviousID()

		p.serverForkDB.AddLink(blkRef, previousID, wrappedObj.(*WriteRequest))

		// When we starting, if fluxdb internal forkdb has no LIB, we perform a check to determine if we
		// should manually set the LIB:
		//
		// - If the block is the first streamable block, we use it as the LIB
		//
		// See comment tagged 69f60031aecb1e0ee5a9b7876ea492f2 (search for it in project)
		if !p.serverForkDB.HasLIB() && (rawBlk.Num() == bstream.GetProtocolFirstStreamableBlock) {
			zlog.Info("setting internal forkdb LIB to first streamable block")
			p.serverForkDB.SetLIB(rawBlk, previousID, rawBlk.Num())
		}

		lib := bstream.NewBlockRef(p.serverForkDB.LIBID(), p.serverForkDB.LIBNum())
		p.updateSpeculativeWrites(lib, rawBlk.AsRef())
	}

	if step.Matches(bstream.StepIrreversible) {
		now := time.Now()
		if p.writeEnabled {
			if len(p.batchWrites) == 0 {
				p.batchOpen = now
				p.batchClose = now.Add(1 * time.Second) // Always flush at least the previous LIB
			}

			req := wrappedObj.(*WriteRequest)

			p.batchWrites = append(p.batchWrites, req)
			p.batchWritableRows += len(req.SingletEntries) + len(req.TabletRows)

			if p.batchWritableRows > 5000 || now.After(p.batchClose) || p.writeOnEachIrreversibleStep {
				defer func() {
					p.batchWrites = nil
					p.batchWritableRows = 0
				}()

				err := p.db.WriteBatch(p.ctx, p.batchWrites)
				if err != nil {
					return err
				}

				timePerBlock := time.Now().Sub(p.batchOpen) / time.Duration(len(p.batchWrites))
				zlog.Info("wrote irreversible segment of blocks starting here",
					zap.Stringer("block", blkRef),
					zap.Uint64("height", rawBlk.Num()),
					zap.Duration("batch_elapsed", time.Now().Sub(p.batchOpen)),
					zap.Duration("batch_elapsed_per_block", timePerBlock),
					zap.Int("batch_write_count", len(p.batchWrites)),
					zap.Int("batch_writable_row_count", p.batchWritableRows),
				)
			}

			p.serverForkDB.MoveLIB(blkRef)
		} else {
			// Fetch from database, and sync with the writer before truncating the LIB here.
			// Don't ask more than once each 2 seconds..
			if p.lastBlockIDCheck.Before(time.Now().Add(-2 * time.Second)) {
				// FIXME (height): Will need to be revisited here for height support
				_, lastWrittenBlock, err := p.db.FetchLastWrittenCheckpoint(p.ctx)
				if err != nil {
					return err
				}

				if lastWrittenBlock.ID() != p.serverForkDB.LIBID() {
					zlog.Debug("writer's LIB updated, advancing server forkDB in return",
						zap.Stringer("block", lastWrittenBlock),
						zap.Uint64("height", lastWrittenBlock.Num()),
					)

					p.serverForkDB.MoveLIB(lastWrittenBlock)
				}

				p.lastBlockIDCheck = time.Now()
			}
		}
	}

	if !step.Matches(bstream.StepNew) && !step.Matches(bstream.StepIrreversible) {
		panic(fmt.Errorf("unsupported forkable step %q", step))
	}

	return nil
}

func isNearRealtime(blk *bstream.Block, now time.Time) bool {
	return now.Add(-15 * time.Second).Before(blk.Time())
}

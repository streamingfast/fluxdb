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

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/fluxdb/metrics"
	pbblockmeta "github.com/streamingfast/pbgo/dfuse/blockmeta/v1"
	"go.uber.org/zap"
)

var ErrCleanSourceStop = errors.New("clean source stop")

func BuildReprocessingPipeline(
	blockFilter func(blk *bstream.Block) error,
	blockMapper BlockMapper,
	blockMeta pbblockmeta.BlockIDClient,
	startBlockResolver bstream.StartBlockResolver,
	handler bstream.Handler,
	blocksStore dstore.Store,
	startHeight uint64,
) (bstream.Source, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	resolvedStartBlock, previousIrreversibleID, err := startBlockResolver(ctx, startHeight)
	if err != nil {
		return nil, fmt.Errorf("start block resolver for block %d: %w", startHeight, err)
	}

	gate := bstream.NewBlockNumGate(startHeight, bstream.GateInclusive, handler, bstream.GateOptionWithLogger(zlog))

	forkableOptions := []forkable.Option{forkable.WithLogger(zlog), forkable.WithFilters(forkable.StepIrreversible)}
	if previousIrreversibleID != "" {
		irrRef := bstream.NewBlockRef(previousIrreversibleID, resolvedStartBlock)
		zlog.Info("configuring inclusive LIB on forkable handler", zap.Stringer("irr_ref", irrRef))
		forkableOptions = append(forkableOptions, forkable.WithInclusiveLIB(irrRef))
	}

	if blockMeta != nil {
		zlog.Info("configuring irreversibility checker on forkable handler")
		forkableOptions = append(forkableOptions, forkable.WithIrreversibilityChecker(blockMeta, 1*time.Second))
	}

	forkableSource := forkable.New(gate, forkableOptions...)

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

	return bstream.NewFileSource(
		blocksStore,
		resolvedStartBlock,
		2,
		filePreprocessor,
		forkableSource,
		bstream.FileSourceWithLogger(zlog),
	), nil
}

func (fdb *FluxDB) BuildPipeline(
	blockMeta pbblockmeta.BlockIDClient,
	getBlockID bstream.EternalSourceStartBackAtBlock,
	handler bstream.Handler,
	blocksStore dstore.Store,
	blockStreamAddr string,
) {
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

	sf := bstream.SourceFromRefFactory(func(startBlock bstream.BlockRef, h bstream.Handler) bstream.Source {
		forkableOptions := []forkable.Option{forkable.WithLogger(zlog), forkable.WithFilters(forkable.StepNew | forkable.StepIrreversible)}
		if !bstream.EqualsBlockRefs(startBlock, bstream.BlockRefEmpty) {
			// Only when we do **not** start from the beginning (i.e. startBlock is the empty block ref), that the
			// forkable should be initialized with an initial LIB value. Otherwise, when we start fresh, the forkable
			// will automatically set its LIB to the first streamable block of the chain.
			zlog.Info("Setting forkable with exclusive LIB", zap.Stringer("lib", startBlock))
			forkableOptions = append(forkableOptions, forkable.WithExclusiveLIB(startBlock))
		}

		if blockMeta != nil {
			zlog.Info("configuring irreversibility checker on forkable handler")
			forkableOptions = append(forkableOptions, forkable.WithIrreversibilityChecker(blockMeta, 5*time.Second))
		}

		// no need for a gate here, since we are starting with ExclusiveLIB, so at startBlock+1
		forkHandler := forkable.New(h, forkableOptions...)

		liveSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
			return blockstream.NewSource(
				context.Background(),
				blockStreamAddr,
				250,
				bstream.NewPreprocessor(preprocessor, subHandler),
			)
		})

		fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
			fs := bstream.NewFileSource(
				blocksStore,
				startBlock.Num(),
				2,
				preprocessor,
				subHandler,
			)

			return fs
		})

		return bstream.NewJoiningSource(fileSourceFactory, liveSourceFactory, forkHandler,
			bstream.JoiningSourceLogger(zlog),
			bstream.JoiningSourceTargetBlockID(startBlock.ID()),
			bstream.JoiningSourceTargetBlockNum(bstream.GetProtocolFirstStreamableBlock),
		)
	})

	fdb.source = bstream.NewDelegatingEternalSource(sf, getBlockID, handler, bstream.EternalSourceWithLogger(zlog))
}

// FluxDBHandler is a pipeline that writes in FluxDB
type FluxDBHandler struct {
	db  *FluxDB
	ctx context.Context

	writeEnabled                bool
	writeOnEachIrreversibleStep bool
	serverForkDB                *forkable.ForkDB

	speculativeReadsLock sync.RWMutex
	speculativeWrites    []*WriteRequest
	headBlock            bstream.BlockRef

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
		// If we are the empty block ref, we are going to initialize ourselves later on in the pipeline when we
		// receive the first streamable block of the chain.
		p.serverForkDB.InitLIB(startBlock)
	}

	return startBlock, nil
}

func (p *FluxDBHandler) HeadBlock(ctx context.Context) bstream.BlockRef {
	p.speculativeReadsLock.RLock()
	defer p.speculativeReadsLock.RUnlock()

	return p.headBlock
}

func (p *FluxDBHandler) FetchSpeculativeWrites(ctx context.Context, headBlockID string, upToHeight uint64) (speculativeWrites []*WriteRequest) {
	p.speculativeReadsLock.RLock()
	defer p.speculativeReadsLock.RUnlock()

	for _, write := range p.speculativeWrites {
		if write.Height > upToHeight {
			continue
		}
		speculativeWrites = append(speculativeWrites, write)
	}

	return
}

func (p *FluxDBHandler) updateSpeculativeWrites(newHeadBlock bstream.BlockRef) {
	blocks := p.serverForkDB.ReversibleSegment(newHeadBlock)
	if len(blocks) == 0 {
		return
	}

	var newWrites []*WriteRequest
	for _, blk := range blocks {
		req := blk.Object.(*WriteRequest)
		newWrites = append(newWrites, req)
	}

	p.speculativeReadsLock.RLock()
	defer p.speculativeReadsLock.RUnlock()

	p.speculativeWrites = newWrites
	p.headBlock = newHeadBlock
}

func (p *FluxDBHandler) ProcessBlock(rawBlk *bstream.Block, rawObj interface{}) error {
	blkRef := rawBlk.AsRef()
	if rawBlk.Num()%600 == 0 || traceEnabled {
		zlog.Info("processing block (printed each 600 blocks)", zap.Stringer("block", blkRef))
	}

	// TODO: implement based on a Forkable object.. will be quite simpler
	fObj := rawObj.(*forkable.ForkableObject)

	switch fObj.Step {
	case forkable.StepNew:

		metrics.HeadBlockTimeDrift.SetBlockTime(rawBlk.Time())
		metrics.HeadBlockNumber.SetUint64(rawBlk.Num())
		if !p.db.IsReady() {
			if isNearRealtime(rawBlk, time.Now()) && !bstream.EqualsBlockRefs(p.HeadBlock(context.Background()), bstream.BlockRefEmpty) {
				zlog.Info("realtime blocks flowing, marking process as ready")
				p.db.SetReady()
			}
		}

		previousRef := rawBlk.PreviousRef()
		p.serverForkDB.AddLink(blkRef, rawBlk.PreviousRef(), fObj.Obj.(*WriteRequest))

		// When we starting, if fluxdb internal forkdb has no LIB and we are seeing the first block, let's use it as the LIB
		if !p.serverForkDB.HasLIB() && rawBlk.Num() == bstream.GetProtocolFirstStreamableBlock {
			zlog.Info("setting internal forkdb LIB to first streamable block")
			p.serverForkDB.TrySetLIB(rawBlk, previousRef, rawBlk.Num())
		}

		p.updateSpeculativeWrites(rawBlk)

	case forkable.StepIrreversible:
		if fObj.StepCount-1 != fObj.StepIndex { // last irreversible block in multi-block step
			return nil
		}

		now := time.Now()
		if p.writeEnabled {
			if len(p.batchWrites) == 0 {
				p.batchOpen = now
				p.batchClose = now.Add(1 * time.Second) // Always flush at least the previous LIB
			}

			zlog.Debug("accumulating write request from irreversible blocks", zap.Stringer("block", rawBlk), zap.Int("block_count", len(fObj.StepBlocks)))
			for _, newIrrBlk := range fObj.StepBlocks {
				req := newIrrBlk.Obj.(*WriteRequest)

				p.batchWrites = append(p.batchWrites, req)
				p.batchWritableRows += len(req.SingletEntries) + len(req.TabletRows)
			}

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

	default:
		panic(fmt.Errorf("unsupported forkable step %q", fObj.Step))
	}

	return nil
}

func isNearRealtime(blk *bstream.Block, now time.Time) bool {
	return now.Add(-15 * time.Second).Before(blk.Time())
}

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

	"github.com/dfuse-io/dtracing"
	"github.com/dfuse-io/fluxdb/store"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

func (fdb *FluxDB) WriteBatch(ctx context.Context, w []*WriteRequest) error {
	ctx, span := dtracing.StartSpan(ctx, "write batch", "write_request_count", len(w))
	defer span.End()

	if err := fdb.isNextBlock(ctx, w[0].Height); err != nil {
		return fmt.Errorf("next block check: %w", err)
	}

	batch := fdb.store.NewBatch(zlog)

	for _, req := range w {
		if err := fdb.writeBlock(ctx, batch, req); err != nil {
			return fmt.Errorf("write block: %w", err)
		}

		if err := batch.FlushIfFull(ctx); err != nil {
			return fmt.Errorf("flushing if full: %w", err)
		}
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	if sched := fdb.idxCache.IndexingSchedule(); len(sched) != 0 {
		err := fdb.IndexTables(ctx)
		if err != nil {
			return fmt.Errorf("index tables: %w", err)
		}
	}

	return nil
}

func (fdb *FluxDB) VerifyAllShardsWritten(ctx context.Context) (string, error) {
	seen := make(map[string]string)
	if err := fdb.store.ScanLastShardsWrittenCheckpoint(ctx, []byte("shard-"), func(key []byte, value []byte) error {
		// FIXME (height): Will need to revisit that part if we start to migrate to a "height" fluxdb system
		_, block, err := unmarshalCheckpoint(value)
		if err != nil {
			return fmt.Errorf("unable to unmarshal checkpoint: %w", err)
		}

		seen[string(bytes.TrimPrefix(key, []byte("shard-")))] = block.ID()
		return nil
	}); err != nil {
		return "", err
	}

	shardToBlockID := make(map[string]string)

	var referenceBlock string
	for i := 0; i < fdb.shardCount; i++ {
		key := fmt.Sprintf("%03d", i)
		shardToBlockID[key] = seen[key]
		if i == 0 {
			referenceBlock = seen[key]
		}
	}

	var faultyShards []string
	var missingShards []string
	for key, seenBlock := range shardToBlockID {
		if seenBlock == "" {
			missingShards = append(missingShards, key)
		}
		if seenBlock != referenceBlock {
			faultyShards = append(faultyShards, key)
		}
	}

	var err error
	if missingShards != nil {
		err = fmt.Errorf("missing shards: %v", missingShards)
	}

	if faultyShards != nil {
		err = fmt.Errorf("shards not matching reference block %s (shards %v): %w", referenceBlock, faultyShards, err)
	}

	return referenceBlock, err

}

func (fdb *FluxDB) UpdateGlobalLastBlockID(ctx context.Context, blockID string) error {
	batch := fdb.store.NewBatch(zlog)
	batch.SetLastCheckpoint(lastCheckpointRowKey, []byte(blockID))
	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("flushing last block marker: %w", err)
	}

	return nil
}

func (fdb *FluxDB) writeBlock(ctx context.Context, batch store.Batch, w *WriteRequest) (err error) {
	for _, entry := range w.SingletEntries {
		var value []byte
		if !entry.IsDeletion() {
			value, err = entry.MarshalValue()
			if err != nil {
				return fmt.Errorf("singlet to proto: %w", err)
			}
		}

		batch.SetRow(KeyForSingletEntry(entry), value)
	}

	for _, row := range w.TabletRows {
		var value []byte
		if !row.IsDeletion() {
			value, err = row.MarshalValue()
			if err != nil {
				return fmt.Errorf("tablet to proto: %w", err)
			}
		}

		batch.SetRow(KeyForTabletRow(row), value)

		tablet := row.Tablet()
		fdb.idxCache.IncCount(tablet)
		if fdb.idxCache.shouldTriggerIndexing(tablet) {
			fdb.idxCache.ScheduleIndex(tablet, w.Height)
		}
	}

	checkpoint := pbfluxdb.Checkpoint{
		Height: w.Height,
		Block:  &pbbstream.BlockRef{Id: w.BlockRef.ID(), Num: w.BlockRef.Num()},
	}
	last, err := proto.Marshal(&checkpoint)
	if err != nil {
		return fmt.Errorf("unable to marshal checkpoint: %w", err)
	}

	batch.SetLastCheckpoint(fdb.lastCheckpointKey(), last)
	return nil
}

func (fdb *FluxDB) isNextBlock(ctx context.Context, writeHeight uint64) error {
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("checking if is next block", zap.Uint64("height", writeHeight))

	_, lastBlock, err := fdb.FetchLastWrittenCheckpoint(ctx)
	if err != nil {
		return err
	}

	// FIXME (height): This works only for block num, if we move to a "height" structure, we should just check if linear probably
	lastHeight := lastBlock.Num()
	if lastHeight != writeHeight-1 && lastHeight != 0 && lastHeight != 1 {
		return fmt.Errorf("block %d does not follow last block %d in db", writeHeight, lastHeight)
	}

	return nil
}

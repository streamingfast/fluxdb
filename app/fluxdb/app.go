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
	"net/url"
	"path"
	"strings"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dmetrics"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/fluxdb"
	"github.com/streamingfast/fluxdb/metrics"
	"github.com/streamingfast/fluxdb/store"
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	StoreDSN                 string // Storage connection string
	BlockStreamAddr          string // gRPC endpoint to get real-time blocks
	EnableServerMode         bool   // Enables flux server mode, launch a server
	EnableInjectMode         bool   // Enables flux inject mode, writes into kvd
	EnableReprocSharderMode  bool   // Enables flux reproc shard mode, exclusive option, cannot be set if either server, injector or reproc-injector mode is set
	EnableReprocInjectorMode bool   // Enables flux reproc injector mode, exclusive option, cannot be set if either server, injector or reproc-shard mode is set
	BlockStoreURL            string // dbin blocks store

	// Available for reproc mode only (either reproc shard or reproc injector)
	ReprocShardStoreURL string
	ReprocShardCount    uint64

	// Available for reproc-shard only
	ReprocSharderStartBlockNum    uint64
	ReprocSharderStopBlockNum     uint64
	ReprocSharderScratchDirectory string

	// Available for reproc-injector only
	ReprocInjectorShardIndex uint64

	DisableIndexing            bool   // Disables indexing when injecting data in write mode, should never be used in production, present for repair jobs
	DisableShardReconciliation bool   // Do not reconcile all shard last written block to the current active last written block, should never be used in production, present for repair jobs
	DisablePipeline            bool   // Connects to blocks pipeline, can be used to have a development server only fluxdb
	IgnoreIndexRangeStart      uint64 // When indexing a tablet, ignore an existing an index if it's between this range start boundary, both start/stop must be defined to be taken into account
	IgnoreIndexRangeStop       uint64 // When indexing a tablet, ignore an existing an index if it's between this range stop boundary, both start/stop must be defined to be taken into account
	WriteOnEachBlock           bool   // Writes to storage engine at each irreversible block, can be used in development to flush more rapidly to storage
}

type Modules struct {
	// Required dependencies
	BlockMapper        fluxdb.BlockMapper
	OnServerMode       func(db *fluxdb.FluxDB)
	OnInjectMode       func(db *fluxdb.FluxDB)
	StartBlockResolver bstream.StartBlockResolver

	// Optional dependencies
	BlockFilter func(blk *bstream.Block) error
	BlockMeta   pbblockmeta.BlockIDClient
}

type App struct {
	*shutter.Shutter
	config  *Config
	modules *Modules
}

func New(config *Config, modules *Modules) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
		modules: modules,
	}
}

func (a *App) Run() error {
	dmetrics.Register(metrics.MetricSet)

	zlog.Info("running fluxdb", zap.Reflect("config", a.config))
	if err := a.config.Validate(); err != nil {
		return fmt.Errorf("invalid app config: %w", err)
	}

	kvStore, err := fluxdb.NewKVStore(a.config.StoreDSN)
	if err != nil {
		return fmt.Errorf("unable to create store: %w", err)
	}

	blocksStore, err := dstore.NewDBinStore(a.config.BlockStoreURL)
	if err != nil {
		return fmt.Errorf("setting up source blocks store: %w", err)
	}

	if a.config.EnableInjectMode || a.config.EnableServerMode {
		return a.startStandard(blocksStore, kvStore)
	}

	if a.config.EnableReprocSharderMode {
		return a.startReprocSharder(blocksStore)
	}

	if a.config.EnableReprocInjectorMode {
		return a.startReprocInjector(kvStore)
	}

	return errors.New("invalid configuration, don't know what to start for fluxdb")
}

func (a *App) startStandard(blocksStore dstore.Store, kvStore store.KVStore) error {
	db := fluxdb.New(kvStore, a.modules.BlockFilter, a.modules.BlockMapper, a.config.DisableIndexing)
	if a.config.IgnoreIndexRangeStart != 0 && a.config.IgnoreIndexRangeStop != 0 {
		db.SetIgnoreIndexRange(a.config.IgnoreIndexRangeStart, a.config.IgnoreIndexRangeStop)
	}

	zlog.Info("initiating fluxdb handler")
	fluxDBHandler := fluxdb.NewHandler(db)

	db.SpeculativeWritesFetcher = fluxDBHandler.FetchSpeculativeWrites
	db.HeadBlock = fluxDBHandler.HeadBlock

	a.OnTerminating(func(_ error) {
		db.Shutdown(nil)
	})

	db.OnTerminated(a.Shutdown)

	if a.config.EnableInjectMode || !a.config.DisablePipeline {
		db.BuildPipeline(a.modules.BlockMeta, fluxDBHandler.InitializeStartBlockID, fluxDBHandler, blocksStore, a.config.BlockStreamAddr)
	}

	if a.config.EnableInjectMode {
		zlog.Info("setting up injector mode write")
		fluxDBHandler.EnableWrites()
	}

	if a.config.WriteOnEachBlock {
		zlog.Info("setting up injector write on each block")
		fluxDBHandler.EnableWriteOnEachIrreversibleStep()
	}

	if a.config.EnableServerMode {
		zlog.Info("invoking on server mode callback")
		a.modules.OnServerMode(db)
	} else {
		zlog.Info("invoking on inject mode callback")
		a.modules.OnInjectMode(db)
	}

	go db.Launch(a.config.DisablePipeline)

	return nil
}

func (a *App) startReprocSharder(blocksStore dstore.Store) error {
	shardsStore, err := dstore.NewStore(a.config.ReprocShardStoreURL, "shard.zst", "zstd", true)
	if err != nil {
		return fmt.Errorf("unable to create shards store at %s: %w", a.config.ReprocShardStoreURL, err)
	}

	shardingPipe, err := fluxdb.NewSharder(
		shardsStore,
		a.config.ReprocSharderScratchDirectory,
		int(a.config.ReprocShardCount),
		uint64(a.config.ReprocSharderStartBlockNum),
		uint64(a.config.ReprocSharderStopBlockNum),
	)
	if err != nil {
		return fmt.Errorf("unable to create sharder: %w", err)
	}

	source, err := fluxdb.BuildReprocessingPipeline(
		a.modules.BlockFilter,
		a.modules.BlockMapper,
		a.modules.BlockMeta,
		a.modules.StartBlockResolver,
		shardingPipe,
		blocksStore,
		a.config.ReprocSharderStartBlockNum,
	)
	if err != nil {
		return fmt.Errorf("reprocessing pipeline: %w", err)
	}

	a.OnTerminating(func(_ error) {
		source.Shutdown(nil)
	})

	source.OnTerminated(func(err error) {
		// FIXME: This `HasSuffix` is sh**ty, need to replace with a better pattern, `source.Shutdown(nil)` is one of them
		if err != nil && strings.HasSuffix(err.Error(), fluxdb.ErrCleanSourceStop.Error()) {
			err = nil
		}

		a.Shutdown(err)
	})

	source.Run()

	// Wait for either source to complete or the app being killed
	select {
	case <-a.Terminating():
	case <-source.Terminated():
	}

	return nil
}

func appendPath(baseURL string, suffix string) (string, error) {
	storeURL, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	fullPath := storeURL.Path
	storeURL.Path = path.Join(fullPath, suffix)

	return storeURL.String(), nil
}

func (a *App) startReprocInjector(kvStore store.KVStore) error {
	db := fluxdb.New(kvStore, a.modules.BlockFilter, a.modules.BlockMapper, a.config.DisableIndexing)
	if a.config.IgnoreIndexRangeStart != 0 && a.config.IgnoreIndexRangeStop != 0 {
		db.SetIgnoreIndexRange(a.config.IgnoreIndexRangeStart, a.config.IgnoreIndexRangeStop)
	}

	db.SetSharding(int(a.config.ReprocInjectorShardIndex), int(a.config.ReprocShardCount))

	// We allow re-injecting shards when disable shard reconciliation is set to true, which mean we are doing a
	// repair job. Hence when the option is not set, we ensure the database is clean before proceeding.
	if !a.config.DisableShardReconciliation {
		if err := db.CheckCleanDBForSharding(); err != nil {
			return fmt.Errorf("db is not clean before injecting shards: %w", err)
		}
	}

	shardStoreFullURL, err := appendPath(a.config.ReprocShardStoreURL, fmt.Sprintf("%03d", a.config.ReprocInjectorShardIndex))
	if err != nil {
		return fmt.Errorf("invalid URL, cannot append shardindex path: %w", err)
	}
	zlog.Info("using shards url", zap.String("store_url", shardStoreFullURL))

	shardStore, err := dstore.NewStore(shardStoreFullURL, "shard.zst", "zstd", true)
	if err != nil {
		return fmt.Errorf("unable to create shards store at %s: %w", shardStoreFullURL, err)
	}

	shardInjector := fluxdb.NewShardInjector(shardStore, db)

	a.OnTerminating(func(_ error) {
		shardInjector.Shutdown(nil)
	})

	shardInjector.OnTerminated(a.Shutdown)

	if err := shardInjector.Run(); err != nil {
		return fmt.Errorf("injector failed: %w", err)
	}

	ctx := context.Background()
	stats, err := db.VerifyAllShardsWritten(ctx)
	if err != nil {
		zlog.Info("all shards are not done yet, not updating last block", zap.Error(err))
		a.Shutdown(nil)
		return nil
	}

	if a.config.DisableShardReconciliation {
		zlog.Info("all shards done injecting but configured to not reconcile shard checkpoint as last block checkpoint, exiting")
		a.Shutdown(nil)
		return nil
	}

	zlog.Info("all shards done injecting, setting checkpoint to last block", zap.Stringer("last_block", stats.ReferenceBlockRef))
	err = db.WriteShardingFinalCheckpoint(ctx, stats.HighestHeight, stats.ReferenceBlockRef)
	if err != nil {
		return fmt.Errorf("cannot write final checkpoint: %w", err)
	}

	a.Shutdown(nil)
	return nil
}

// Validate inspects itself to determine if the current config is valid according to
// FluxDB rules.
func (config *Config) Validate() error {
	server := config.EnableServerMode
	injector := config.EnableInjectMode
	reprocSharder := config.EnableReprocSharderMode
	reprocInjector := config.EnableReprocInjectorMode

	if !server && !injector && !reprocSharder && !reprocInjector {
		return errors.New("no mode selected, one of enable server, enable injector, enable reproc sharder or enable reproc injector must be set")
	}

	if reprocSharder && (server || injector || reprocInjector) {
		return errors.New("reproc sharder mode is an exclusive option, cannot be set while any of enable server, enable injector or enable reproc injector is set")
	}

	if reprocInjector && (server || injector || reprocSharder) {
		return errors.New("reproc injector mode is an exclusive option, cannot be set while any of enable server, enable injector or enable reproc injector is set")
	}

	if (reprocSharder || reprocInjector) && config.ReprocShardCount <= 0 {
		return errors.New("reproc mode requires you to set a shard count value higher than 0")
	}

	if reprocInjector && config.ReprocInjectorShardIndex >= config.ReprocShardCount {
		return fmt.Errorf("reproc injector mode shard index invalid, got index %d but it's outside possible value for a shard count of %d", config.ReprocInjectorShardIndex, config.ReprocShardCount)
	}

	return nil
}

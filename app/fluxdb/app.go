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
	"strings"

	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/fluxdb"
	"github.com/dfuse-io/fluxdb/store"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	StoreDSN                 string // Storage connection string
	BlockStreamAddr          string // gRPC endpoint to get real-time blocks
	EnableServerMode         bool   // Enables flux server mode, launch a server
	EnableInjectMode         bool   // Enables flux inject mode, writes into kvd
	EnablePipeline           bool   // Connects to blocks pipeline, can be used to have a development server only fluxdb
	EnableReprocSharderMode  bool   // Enables flux reproc shard mode, exclusive option, cannot be set if either server, injector or reproc-injector mode is set
	EnableReprocInjectorMode bool   // Enables flux reproc injector mode, exclusive option, cannot be set if either server, injector or reproc-shard mode is set
	BlockStoreURL            string // dbin blocks store

	// Available for reproc mode only (either reproc shard or reproc injector)
	ReprocShardStoreURL string
	ReprocShardCount    uint64

	// Available for reproc-shard only
	ReprocSharderStartBlockNum uint64
	ReprocSharderStopBlockNum  uint64

	// Available for reproc-injector only
	ReprocInjectorShardIndex uint64
}

type App struct {
	*shutter.Shutter
	config *Config

	mapper       fluxdb.BlockMapper
	onServerMode func(db *fluxdb.FluxDB)
	onInjectMode func(db *fluxdb.FluxDB)
}

func New(config *Config, mapper fluxdb.BlockMapper, onServerMode func(db *fluxdb.FluxDB), onInjectMode func(db *fluxdb.FluxDB)) *App {
	return &App{
		Shutter:      shutter.New(),
		config:       config,
		mapper:       mapper,
		onServerMode: onServerMode,
		onInjectMode: onInjectMode,
	}
}

func (a *App) Run() error {
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
	db := fluxdb.New(kvStore, a.mapper)

	zlog.Info("initiating fluxdb handler")
	fluxDBHandler := fluxdb.NewHandler(db)

	db.SpeculativeWritesFetcher = fluxDBHandler.FetchSpeculativeWrites
	db.HeadBlock = fluxDBHandler.HeadBlock

	a.OnTerminating(func(e error) {
		db.Shutdown(nil)
	})

	db.OnTerminated(a.Shutdown)

	if a.config.EnableInjectMode || a.config.EnablePipeline {
		db.BuildPipeline(fluxDBHandler.InitializeStartBlockID, fluxDBHandler, blocksStore, a.config.BlockStreamAddr, 2)
	}

	if a.config.EnableInjectMode {
		zlog.Info("setting up injector mode write")
		fluxDBHandler.EnableWrites()
	}

	if a.config.EnableServerMode {
		zlog.Info("invoking on server mode callback")
		a.onServerMode(db)
	} else {
		zlog.Info("invoking on inject mode callback")
		a.onInjectMode(db)
	}

	go db.Launch(a.config.EnablePipeline)

	return nil
}

func (a *App) startReprocSharder(blocksStore dstore.Store) error {
	shardsStore, err := dstore.NewStore(a.config.ReprocShardStoreURL, "shard.zst", "zstd", true)
	if err != nil {
		return fmt.Errorf("unable to create shards store at %s: %w", a.config.ReprocShardStoreURL, err)
	}

	shardingPipe := fluxdb.NewSharder(
		shardsStore,
		int(a.config.ReprocShardCount),
		uint64(a.config.ReprocSharderStartBlockNum),
		uint64(a.config.ReprocSharderStopBlockNum),
	)

	// FIXME: We should use the new `DPoSLIBNumAtBlockHeightFromBlockStore` to go back as far as needed!
	source := fluxdb.BuildReprocessingPipeline(a.mapper, shardingPipe, blocksStore, a.config.ReprocSharderStartBlockNum, 400, 2)

	a.OnTerminating(func(e error) {
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

func (a *App) startReprocInjector(kvStore store.KVStore) error {
	db := fluxdb.New(kvStore, a.mapper)

	db.SetSharding(int(a.config.ReprocInjectorShardIndex), int(a.config.ReprocShardCount))
	if err := db.CheckCleanDBForSharding(); err != nil {
		return fmt.Errorf("db is not clean before injecting shards: %w", err)
	}

	shardStoreFullURL := a.config.ReprocShardStoreURL + "/" + fmt.Sprintf("%03d", a.config.ReprocInjectorShardIndex)
	zlog.Info("using shards url", zap.String("store_url", shardStoreFullURL))

	shardStore, err := dstore.NewStore(shardStoreFullURL, "shard.zst", "zstd", true)
	if err != nil {
		return fmt.Errorf("unable to create shards store at %s: %w", shardStoreFullURL, err)
	}

	shardInjector := fluxdb.NewShardInjector(shardStore, db)

	a.OnTerminating(func(e error) {
		shardInjector.Shutdown(nil)
	})

	shardInjector.OnTerminated(a.Shutdown)

	if err := shardInjector.Run(); err != nil {
		return fmt.Errorf("injector failed: %w", err)
	}

	ctx := context.Background()
	height, lastBlock, err := db.VerifyAllShardsWritten(ctx)
	if err != nil {
		zlog.Info("all shards are not done yet, not updating last block", zap.Error(err))
		a.Shutdown(nil)
		return nil
	}

	zlog.Info("all shards done injecting, setting checkpoint to last block", zap.Stringer("last_block", lastBlock))
	err = db.WriteShardingFinalCheckpoint(ctx, height, lastBlock)
	if err != nil {
		return fmt.Errorf("cannot write final checkpoint: %w", err)
	}

	a.Shutdown(nil)
	return nil
}

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

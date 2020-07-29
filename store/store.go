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

package store

import (
	"context"
	"encoding/hex"
	"errors"

	"go.uber.org/zap"
)

// BreakScan error can be used on scanning function to notify termination of scanning
var BreakScan = errors.New("break scan")

var ErrNotFound = errors.New("not found")

type Key []byte

func (k Key) String() string {
	return hex.EncodeToString(k)
}

type Batch interface {
	Flush(ctx context.Context) error
	FlushIfFull(ctx context.Context) error

	// FIXME: Maybe the batch "adder/setter" should not event care about the key and compute
	//        it straight? Since this is per storage engine, it would be a good place since
	//        all saved element would pass through those methods...
	SetRow(key []byte, value []byte)
	SetLastCheckpoint(key []byte, value []byte)
	SetIndex(key []byte, value []byte)

	Reset()
}

type OnKeyValue func(key []byte, value []byte) error

// KVStore represents the abstraction needed by FluxDB to correctly use different
// underlying KV storage engine.
type KVStore interface {
	Close() error

	NewBatch(logger *zap.Logger) Batch

	FetchIndex(ctx context.Context, tableKey, prefixKey, keyStart []byte) (rowKey []byte, rawIndex []byte, err error)

	HasTabletRow(ctx context.Context, tabletKey []byte) (exists bool, err error)

	FetchTabletRow(ctx context.Context, key []byte) (value []byte, err error)

	FetchTabletRows(ctx context.Context, keys [][]byte, onKeyValue OnKeyValue) error

	FetchSingletEntry(ctx context.Context, keyStart, keyEnd []byte) (key []byte, value []byte, err error)

	ScanTabletRows(ctx context.Context, keyStart, keyEnd []byte, onKeyValue OnKeyValue) error

	// FetchLastWrittenCheckpoint returns the latest written checkpoint reference that was correctly
	// committed to the storage system.
	//
	// If nothing was ever written yet, this must return `nil, ErrNotFound`.
	FetchLastWrittenCheckpoint(ctx context.Context, key []byte) (value []byte, err error)

	ScanLastShardsWrittenCheckpoint(ctx context.Context, keyPrefix []byte, onKeyValue OnKeyValue) error
}

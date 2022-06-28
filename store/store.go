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
	FlushIfFull(ctx context.Context) (flushed bool, err error)

	// PurgeRow is used to completely delete an element for the database.
	//
	// **Important** If a tablet row/singlet entry was deleted, you should use
	//               instead `SetRow(key, nil)` which is the correct way to represent
	//               a deleted row in FluxDB system.
	PurgeRow(key []byte)

	// FIXME: Maybe the batch "adder/setter" should not event care about the key and compute
	//        it straight? Since this is per storage engine, it would be a good place since
	//        all saved element would pass through those methods...
	SetRow(key []byte, value []byte)
	SetLastCheckpoint(key []byte, value []byte)

	Reset()
}

type OnKey func(key []byte) error

type OnKeyValue func(key []byte, value []byte) error

// KVStore represents the abstraction needed by FluxDB to correctly use different
// underlying KV storage engine.
type KVStore interface {
	Close() error

	NewBatch(logger *zap.Logger) Batch

	HasTabletRow(ctx context.Context, keyStart, keyEnd []byte) (exists bool, err error)

	FetchTabletRow(ctx context.Context, key []byte) (value []byte, err error)

	FetchTabletRows(ctx context.Context, keys [][]byte, onKeyValue OnKeyValue) error

	// FetchSingletEntry reads a single singlet entry for the given Singlet range. The range must include block
	// boundaries to ensure we match only element from this singlet and not a next following.
	//
	// If the entry is found, the `key` and `value` will be set and `err` will be `nil`. If the entry is
	// found withing the range, `key`, `value` and `err` will be `nil`. Finally, if an error is encountered
	// while fetching the singlet entry, `key` and `value` will be `nil` and `err` will be set to the
	// error hit.
	FetchSingletEntry(ctx context.Context, keyStart, keyEnd []byte) (key []byte, value []byte, err error)

	// FetchSingletEntries reads all single singlet entries for the given Singlet range. The range must include block
	// boundaries to ensure we match only element from this singlet and not a next following.
	FetchSingletEntries(ctx context.Context, keyStart, keyEnd []byte) (keys [][]byte, values [][]byte, err error)

	ScanTabletRows(ctx context.Context, keyStart, keyEnd []byte, onKeyValue OnKeyValue) error

	ScanIndexKeys(ctx context.Context, prefix []byte, onKey OnKey) error

	// FetchLastWrittenCheckpoint returns the latest written checkpoint reference that was correctly
	// committed to the storage system.
	//
	// If nothing was ever written yet, this must return `nil, ErrNotFound`.
	FetchLastWrittenCheckpoint(ctx context.Context, key []byte) (value []byte, err error)

	ScanLastShardsWrittenCheckpoint(ctx context.Context, keyPrefix []byte, onKeyValue OnKeyValue) error

	DeleteShardsCheckpoint(ctx context.Context, keyPrefix []byte) error
}

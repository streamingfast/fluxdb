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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dfuse-io/bstream"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/golang/protobuf/proto"
)

var collections = map[string]bool{}

// Tablet is a block-aware temporal table containing all the rows at any given
// block height. Let's assume you have a token contract where the token and
// there is multiple accounts owning this token. You could track the historical
// values of balances at any block height using a Tablet implementation. The tablet
// key would be `<contract>:<token>` while the rows would be each of the account
// owning the token. The primary key of the row would be the account while the
// value stored in the row would be the balance.
//
// By using the Tablet implementation and fluxdb library, you would then be able
// to retrieve, at any block height, all accounts and their respective balance.
//
// A Tablet always contain 0 to N rows, we maintain the state of each row
// independently. If a row mutates each block, we will have a total B versions
// of this exact row in the database, B being the total count of blocks seen so
// far.
type Tablet interface {
	NewRowFromKV(key string, value []byte) (TabletRow, error)

	Key() string
	KeyAt(height uint64) string
	KeyForRowAt(height uint64, primaryKey string) string

	IndexableTablet

	String() string
}

type IndexableTablet interface {
	IndexMapper() TabletIndexMapper
}

type TabletIndex struct {
	AtHeight           uint64
	SquelchCount       uint64
	PrimaryKeyToHeight map[string]uint64
}

func NewTabletIndex() *TabletIndex {
	return &TabletIndex{
		PrimaryKeyToHeight: map[string]uint64{},
	}
}

func (i *TabletIndex) RowCount() uint64 {
	if i == nil {
		return 0
	}

	return uint64(len(i.PrimaryKeyToHeight))
}

type TabletIndexMapper interface {
	EncodeIndex(squelchCount uint64, primaryKeyToBlockNum map[string]uint64) ([]byte, error)
	DecodeIndex(buffer []byte) (squelchCount uint64, primaryKeyToBlockNum map[string]uint64, err error)
}

var GenericTabletIndexMapper TabletIndexMapper = &genericTabletIndexMapper{}

type genericTabletIndexMapper struct {
}

func (m *genericTabletIndexMapper) EncodeIndex(squelchCount uint64, primaryKeyToBlockNum map[string]uint64) ([]byte, error) {
	index := &pbfluxdb.Index{}
	index.SquelchedCount = squelchCount
	index.PrimaryKeyToHeight = primaryKeyToBlockNum

	return proto.Marshal(index)
}

func (m *genericTabletIndexMapper) DecodeIndex(buffer []byte) (squelchCount uint64, primaryKeyToBlockNum map[string]uint64, err error) {
	index := &pbfluxdb.Index{}
	if err = proto.Unmarshal(buffer, index); err != nil {
		err = fmt.Errorf("unable to unmarshal index: %w", err)
		return
	}

	return index.SquelchedCount, index.PrimaryKeyToHeight, nil
}

func ExplodeTabletKey(key string) (collection, tablet string, err error) {
	parts := strings.Split(key, "/")
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}

	err = fmt.Errorf("tablet key should have 2 segments separated by '/' (`<collection/tablet>`), got %d segments", len(parts))
	return
}

type TabletFactory = func(row *pbfluxdb.Row) Tablet

var tabletFactories = map[string]TabletFactory{}

func RegisterTabletFactory(collection string, factory TabletFactory) {
	if collections[collection] {
		panic(fmt.Errorf("collections %q is already registered, they all must be unique among registered ones", collection))
	}

	tabletFactories[collection] = factory
}

type TabletRow interface {
	Key() string
	Value() []byte

	Tablet() Tablet
	Height() uint64
	PrimaryKey() string
}

func ExplodeTabletRowKey(key string) (collection, tablet, height, primaryKey string, err error) {
	parts := strings.Split(key, "/")
	if len(parts) == 4 {
		return parts[0], parts[1], parts[2], parts[3], nil
	}

	err = fmt.Errorf("row key should have 4 segments separated by '/' (`<collection/tablet/height/primaryKey>`), got %d segments", len(parts))
	return
}

type BaseTabletRow struct {
	pbfluxdb.Row
}

func NewBaseTabletRow(tabletKey string, height uint64, primaryKey string, payload []byte) (out BaseTabletRow, err error) {
	collection, tabletKey, err := ExplodeTabletKey(tabletKey)
	if err != nil {
		return out, fmt.Errorf("invalid tablet key: %w", err)
	}

	out.Collection = collection
	out.TabletKey = tabletKey
	out.HeightKey = HexHeight(height)
	out.PrimKey = primaryKey
	out.Payload = payload

	return
}

func NewBaseTabletRowFromKV(rowKey string, rowValue []byte) (out BaseTabletRow, err error) {
	collection, tabletKey, heightKey, primaryKey, err := ExplodeTabletRowKey(rowKey)
	if err != nil {
		return out, fmt.Errorf("unable to explode tablet row key %q: %w", rowKey, err)
	}

	out.Collection = collection
	out.TabletKey = tabletKey
	out.HeightKey = heightKey
	out.PrimKey = primaryKey
	out.Payload = rowValue

	return
}

func (r *BaseTabletRow) Height() uint64 {
	value, err := strconv.ParseUint(r.HeightKey, 16, 64)
	if err != nil {
		panic(fmt.Errorf("value %q is not a valid block num uint64 value: %w", r.HeightKey, err))
	}

	return value
}

func (r *BaseTabletRow) Key() string {
	return r.Collection + "/" + r.TabletKey + "/" + r.HeightKey + "/" + r.PrimKey
}

func (r *BaseTabletRow) PrimaryKey() string {
	return r.PrimKey
}

func (r *BaseTabletRow) Tablet() Tablet {
	factory := tabletFactories[r.Collection]
	if factory == nil {
		panic(fmt.Errorf(`no known tablet factory for collection %s, register factories through a 'RegisterTabletFactory("prefix", func (...) { ... })' call`, r.Collection))
	}

	return factory(&r.Row)
}

func (r *BaseTabletRow) Value() []byte {
	return r.Payload
}

func isDeletionRow(row TabletRow) bool {
	return len(row.Value()) == 0
}

// Singlet is a block-aware container for a single piece of information, for
// example an account's balance.
//
// A Singlet always contain a single row key but stored at any block height.
type Singlet interface {
	Key() string
	KeyAt(height uint64) string

	NewEntryFromKV(entryKey string, value []byte) (SingletEntry, error)

	String() string
}

func ExplodeSingletKey(key string) (collection, singlet string, err error) {
	parts := strings.Split(key, "/")
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}

	err = fmt.Errorf("singlet key should have 2 segments separated by '/' (`<collection/singlet>`), got %d segments", len(parts))
	return
}

type SingletEntry interface {
	Key() string
	Value() []byte

	Singlet() Singlet
	Height() uint64
}

func ExplodeSingletEntryKey(key string) (collection, tablet, height string, err error) {
	parts := strings.Split(key, "/")
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2], nil
	}

	err = fmt.Errorf("singlet entry key should have 3 segments separated by '/' (`<collection/singlet/height>`), got %d segments", len(parts))
	return
}

type SingletFactory = func(row *pbfluxdb.Row) Singlet

var singletFactories = map[string]SingletFactory{}

func RegisterSingletFactory(collection string, factory SingletFactory) {
	if collections[collection] {
		panic(fmt.Errorf("collection %q is already registered, they all must be unique among registered ones", collection))
	}

	singletFactories[collection] = factory
}

type BaseSingletEntry struct {
	pbfluxdb.Row
}

func NewBaseSingletEntry(singletKey string, height uint64, payload []byte) (out BaseSingletEntry, err error) {
	collection, singletKey, err := ExplodeSingletKey(singletKey)
	if err != nil {
		return out, fmt.Errorf("invalid singlet key: %w", err)
	}

	out.Collection = collection
	out.TabletKey = singletKey
	out.HeightKey = HexRevHeight(height)
	out.Payload = payload

	return
}

func NewBaseSingleEntryFromKV(rowKey string, rowValue []byte) (out BaseSingletEntry, err error) {
	collection, tabletKey, heightKey, err := ExplodeSingletEntryKey(rowKey)
	if err != nil {
		return out, fmt.Errorf("unable to explode singlet row key %q: %w", rowKey, err)
	}

	out.Collection = collection
	out.TabletKey = tabletKey
	out.HeightKey = heightKey
	out.Payload = rowValue

	return
}

func (r *BaseSingletEntry) Height() uint64 {
	value, err := strconv.ParseUint(r.HeightKey, 16, 64)
	if err != nil {
		panic(fmt.Errorf("value %q is not a valid block num uint64 value: %w", r.HeightKey, err))
	}

	return math.MaxUint64 - value
}

func (r *BaseSingletEntry) Key() string {
	return r.Collection + "/" + r.TabletKey + "/" + r.HeightKey
}

func (r *BaseSingletEntry) Singlet() Singlet {
	factory := singletFactories[r.Collection]
	if factory == nil {
		panic(fmt.Errorf(`no known singlet factory for collection %s, register factories through a 'RegisterSingletFactory("prefix", func (...) { ... })' call`, r.Collection))
	}

	return factory(&r.Row)
}

func (r *BaseSingletEntry) Value() []byte {
	return r.Payload
}

func isDeletionEntry(entry SingletEntry) bool {
	return len(entry.Value()) == 0
}

type BlockMapper interface {
	Map(rawBlk *bstream.Block) (*WriteRequest, error)
}

type WriteRequest struct {
	SingletEntries []SingletEntry
	TabletRows     []TabletRow

	Height   uint64
	BlockRef bstream.BlockRef
}

func (r *WriteRequest) AppendSingletEntry(entry SingletEntry) {
	r.SingletEntries = append(r.SingletEntries, entry)
}

func (r *WriteRequest) AppendTabletRow(row TabletRow) {
	r.TabletRows = append(r.TabletRows, row)
}

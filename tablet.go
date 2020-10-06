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
	"encoding/hex"
	"fmt"
	"math"
)

// TabletFactory accepts a tablet identifier bytes and convert it into a valid Tablet
// concrete implementation.
//
// The received identifier value **never* contains the collection prefix, so you can start
// parsing the key right away.
//
// **Important** The amout of bytes received in the identifier could be bigger than the actual
// amount required to form a valid tablet identifier (i.e. len(identifier) != len(tablet.Identifier())).
// Your factory implementation must not check maximal length and instead deal with a minimal length. This
// way, it's easy to create a tablet instance from a full key as stored in the underlying engine.
type TabletFactory func(identifier []byte) (Tablet, error)

var tabletFactories = map[uint16]TabletFactory{}

// RegisterSingletFactory accepts a collection (and its name) as well as a TabletFactory for
// this Tablet type and register it in the system so it's known to FluxDB internal components.
func RegisterTabletFactory(collection uint16, collectionName string, factory TabletFactory) {
	if collection >= math.MaxUint16-16 {
		panic(fmt.Errorf("collections identifier 0x%04X is reserved, libraries identifiers must be within 0x0000 and 0xEEEF", collection))
	}

	if actual, found := collections[collection]; found {
		panic(fmt.Errorf("collections identifier %d is already registered for %q, they all must be unique among registered ones", collection, actual.Name))
	}

	registerTabletFactory(collection, collectionName, factory)
}

func registerTabletFactory(collection uint16, collectionName string, factory TabletFactory) {
	collections[collection] = Collection{Identifier: collection, Name: collectionName}
	tabletFactories[collection] = factory
}

// Tablet is a height-aware temporal table containing all the rows at any given
// height. Let's assume you have a token contract where the token and
// there is multiple accounts owning this token. You could track the historical
// values of balances at any height using a Tablet implementation. The tablet
// key would be `<contract>:<token>` while the rows would be each of the account
// owning the token. The primary key of the row would be the account while the
// value stored in the row would be the balance.
//
// By using the Tablet implementation and fluxdb library, you would then be able
// to retrieve, at any height, all accounts and their respective balance.
//
// A Tablet always contain 0 to N rows, we maintain the state of each row
// independently. If a row mutates each height, we will have a total B versions
// of this exact row in the database, B being the total count of heights seen so
// far.
//
// In lots of blockchain system, the height is simply the block number value.
type Tablet interface {
	// Collection to which this tablet is bound to.
	Collection() uint16

	// Identifier uniquely representing this Tablet instance within its collection.
	Identifier() []byte

	// Row constructs a concrete TabletRow implementation for this Tablet
	// at the specified height, for the given primary key associated to this value
	// bytes. The row usually unmarshals the value and ensure it's valid.
	Row(height uint64, primaryKey []byte, value []byte) (TabletRow, error)

	// String should turn the Tablet into a human readable form. If the
	// identifier is composed of multiple part, use `:` to delimit them in the string.
	//
	// You **should** add the collection name to the tablet identifier, it ease recognition
	// of which tablet collection you are seeing.
	//
	// This method is used by the various `Key*#String` helpers, they all expect this implementation
	// to have the collection name appended to them, so you should respect this soft constraint
	// and the `:` delimiter.
	String() string
}

// NewTablet constructs a new Tablet implementation from the tablet key as stored
// in the underlying storage engine.
//
// The key received here will always contain the collection prefix (2 bytes) as well as enough
// bytes to contain the tablet identifier.
//
// **Important** The received key might contain more bytes, like the reminder of a full row key,
// all code paths called here must not expected a maximum length and only deal with just enough
// bytes to read the data needed from the key.
func NewTablet(tabletKey []byte) (tablet Tablet, err error) {
	if len(tabletKey) <= 2 {
		return nil, fmt.Errorf("invalid key length, expected at least 3 bytes, got %d", len(tabletKey))
	}

	tabletFactory, foundFactory := tabletFactories[collectionFromKey(tabletKey)]
	if !foundFactory {
		return nil, fmt.Errorf("unknown collection 0x%04X", collectionFromKey(tabletKey))
	}

	tablet, err = tabletFactory(tabletKey[2:])
	if err != nil {
		return nil, fmt.Errorf("invalid tablet key: %w", err)
	}

	return
}

// TabletEqual returns wheter two Tablet instances are considered equal by
// comparing their respective collection and identifier.
//
// Two nil values are considered equal.
func TabletEqual(left, right Tablet) bool {
	if left == nil && right == nil {
		return true
	}

	if left == nil || right == nil {
		return false
	}

	return left.Collection() == right.Collection() && bytes.Equal(left.Identifier(), right.Identifier())
}

// TabletKey represents the storage key for a Tablet, contains the collection as
// well as the Tablet's identifier in byte form.
type TabletKey []byte

func KeyForTablet(tablet Tablet) (out TabletKey) {
	collection := tablet.Collection()
	identifier := tablet.Identifier()

	identifierBytes := len(identifier)

	out = make([]byte, collectionBytes+identifierBytes)
	copyCollection(out, collection)
	copy(out[collectionBytes:], identifier)
	return
}

func (k TabletKey) String() string {
	tablet, err := NewTablet(k)
	if err != nil {
		return fmt.Sprintf("#Error<Invalid tablet key %q: %s>", Key(k), err)
	}

	return tablet.String()
}

// TabletKeyAt represents the storage key for a Tablet at a given height, contains
// the collection, the Tablet's identifier as well as the height in byte form.
type TabletKeyAt []byte

func KeyForTabletAt(tablet Tablet, height uint64) (out TabletKeyAt) {
	collection := tablet.Collection()
	identifier := tablet.Identifier()

	identifierBytes := len(identifier)

	out = make([]byte, collectionBytes+identifierBytes+heightBytes)
	copyCollection(out, collection)
	copy(out[collectionBytes:], identifier)
	copyHeight(out[collectionBytes+identifierBytes:], height)
	return
}

func (k TabletKeyAt) String() string {
	if len(k) <= 2+8 {
		return fmt.Sprintf("#Error<Invalid tablet key %q: invalid key length, expected at least 11 bytes, got %d>", Key(k), len(k))
	}

	tablet, err := NewTablet(k)
	if err != nil {
		return fmt.Sprintf("#Error<Invalid tablet key %q: %s>", Key(k), err)
	}

	return tablet.String() + ":" + hex.EncodeToString(k[len(k)-8:])
}

type TabletRow interface {
	Tablet() Tablet
	Height() uint64
	PrimaryKey() []byte
	IsDeletion() bool

	MarshalValue() ([]byte, error)

	String() string
}

// NewTabletRow constructs a new TabletRow implementation from the key as stored
// in the underlying storage engine.
//
// The key received here will always contain the collection prefix (2 bytes) as well as enough
// bytes to contain the tablet identifier, the height and the primary key.
func NewTabletRow(tablet Tablet, key []byte, value []byte) (TabletRow, error) {
	tabletIdentifier := tablet.Identifier()
	tabletIdentifierBytes := len(tabletIdentifier)

	heightOffset := collectionBytes + tabletIdentifierBytes
	primaryKeyOffset := heightOffset + 8

	if primaryKeyOffset >= len(key) {
		return nil, fmt.Errorf("invalid key length, expected at least %d bytes, got %d", primaryKeyOffset+1, len(key))
	}

	collection := collectionFromKey(key)
	if tablet.Collection() != collection {
		return nil, fmt.Errorf("key from different collection, expected collection 0x%04X, got 0x%04X", tablet.Collection(), collection)
	}

	tabletIdentifierEnd := collectionBytes + tabletIdentifierBytes
	if !bytes.Equal(key[collectionBytes:tabletIdentifierEnd], tabletIdentifier) {
		return nil, fmt.Errorf("key from different tablet, expected tablet identifier %q, got %q", Key(tabletIdentifier), Key(key[collectionBytes:tabletIdentifierEnd]))
	}

	height := bigEndian.Uint64(key[heightOffset:])
	primaryKey := key[primaryKeyOffset:]

	return tablet.Row(height, primaryKey, value)
}

func NewTabletRowFromStorage(key []byte, value []byte) (TabletRow, error) {
	tablet, err := NewTablet(key)
	if err != nil {
		return nil, fmt.Errorf("new tablet: %w", err)
	}

	return NewTabletRow(tablet, key, value)
}

// TabletRowKey represents a fully well-formed tablet row key as written to the underlying
// storage engine. This key is always represented in the same but variable format:
//
// ```
// <collection (2 bytes)><tablet identifier (N bytes)><height (8 bytes)><row primary key (N bytes)>
// ```
//
// Only the tablet implementation knows how to turn its series of bytes into the correct implementation.
type TabletRowKey []byte

func KeyForTabletRow(row TabletRow) (out TabletRowKey) {
	return KeyForTabletRowFromParts(row.Tablet(), row.Height(), row.PrimaryKey())
}

func KeyForTabletRowFromParts(tablet Tablet, height uint64, primaryKey []byte) (out TabletRowKey) {
	collection := tablet.Collection()
	tabletIdentifier := tablet.Identifier()

	tabletIdentifierBytes := len(tabletIdentifier)
	primaryKeyBytes := len(primaryKey)

	out = make([]byte, collectionBytes+tabletIdentifierBytes+heightBytes+primaryKeyBytes)
	copyCollection(out, collection)
	copy(out[collectionBytes:], tabletIdentifier)
	copyHeight(out[collectionBytes+tabletIdentifierBytes:], height)
	copy(out[collectionBytes+tabletIdentifierBytes+heightBytes:], primaryKey)
	return
}

func (k TabletRowKey) String() string {
	// We pass a nil value because we are only interested by the key formatting here
	row, err := NewTabletRowFromStorage(k, nil)
	if err != nil {
		return fmt.Sprintf("#Error<Invalid tablet row %q: %s>", Key(k), err)
	}

	return row.String()
}

// TabletRowPrimaryKey represents a specific primary key for a given TabletRow. It's used
// mainly for reading a specific tablet row providing easy human readable version for the
// concrete type of TabletRow.
type TabletRowPrimaryKey interface {
	Bytes() []byte
	String() string
}

type BaseTabletRow struct {
	tablet     Tablet
	height     uint64
	primaryKey []byte
	value      []byte
}

func NewBaseTabletRow(tablet Tablet, height uint64, primaryKey []byte, value []byte) (out BaseTabletRow) {
	out.tablet = tablet
	out.height = height
	out.primaryKey = primaryKey
	out.value = value
	return
}

func (b BaseTabletRow) Tablet() Tablet {
	return b.tablet
}

func (b BaseTabletRow) Height() uint64 {
	return b.height
}

func (b BaseTabletRow) PrimaryKey() []byte {
	return b.primaryKey
}

func (b BaseTabletRow) IsDeletion() bool {
	return len(b.value) <= 0
}

func (b BaseTabletRow) MarshalValue() ([]byte, error) {
	return b.value, nil
}

func (b BaseTabletRow) Value() []byte {
	return b.value
}

func (b BaseTabletRow) Stringify(primaryKey string) string {
	return fmt.Sprintf("%s:%016x:%s", b.tablet, b.height, primaryKey)
}

type TabletIndex struct {
	AtHeight           uint64
	SquelchCount       uint64
	PrimaryKeyToHeight *primaryKeyToHeightMap
}

func NewTabletIndex() *TabletIndex {
	return &TabletIndex{
		PrimaryKeyToHeight: newPrimaryKeyToHeightMap(8),
	}
}

func (i *TabletIndex) RowCount() uint64 {
	if i == nil {
		return 0
	}

	return uint64(i.PrimaryKeyToHeight.len())
}

// Rows return all the rows contained in the index for this tablet without actually
// hydrating the value of each row (which means that row retrieved using this method
// will all have `nil` as their value).
//
// This is usef mainly for printing purposes.
func (i *TabletIndex) Rows(tablet Tablet) (rows []TabletRow, err error) {
	count := i.RowCount()
	if count <= 0 {
		return nil, nil
	}

	index := 0
	rows = make([]TabletRow, count)
	for primaryKey, height := range i.PrimaryKeyToHeight.mappings {
		key := KeyForTabletRowFromParts(tablet, height.(uint64), []byte(primaryKey))
		rows[index], err = NewTabletRowFromStorage(key, nil)
		if err != nil {
			return nil, fmt.Errorf("new row: %w", err)
		}

		index++
	}

	return
}

type primaryKeyToTabletRowMap struct {
	*bytesMap
}

func newPrimaryKeyToTabletRowMap(length int) *primaryKeyToTabletRowMap {
	return &primaryKeyToTabletRowMap{
		bytesMap: &bytesMap{
			mappings: make(map[string]interface{}, length),
		},
	}
}

func (m *primaryKeyToTabletRowMap) put(k []byte, v TabletRow) { m._put(k, v) }
func (m *primaryKeyToTabletRowMap) get(k []byte) (TabletRow, bool) {
	v, f := m._get(k)
	return v.(TabletRow), f
}

func (m *primaryKeyToTabletRowMap) delete(k []byte) {
	m.bytesMap.delete(k)
}

func (m *primaryKeyToTabletRowMap) values() (out []TabletRow) {
	i := 0
	out = make([]TabletRow, m.len())
	for _, row := range m.mappings {
		out[i] = row.(TabletRow)
		i++
	}
	return
}

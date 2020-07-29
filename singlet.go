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

// SingletFactory accepts a singlet identifier bytes and convert it into a valid Singlet
// concrete implementation.
//
// The received identifier value **never* contains the collection prefix, so you can start
// parsing the key right away.
//
// **Important** The amout of bytes received in the identifier could be bigger than the actual
// amount required to form a valid singlet identifier (i.e. len(identifier) != len(singlet.Identifier())).
// Your factory implementation must not check maximal length and instead deal with a minimal length. This
// way, it's easy to create a singlet instance from a full key as stored in the underlying engine.
type SingletFactory func(identifier []byte) (Singlet, error)

var singletFactories = map[uint16]SingletFactory{}

// RegisterSingletFactory accepts a collection (and its name) as well as a SingletFactory for
// this Singlet type and register it in the system so it's known to FluxDB internal components.
func RegisterSingletFactory(collection uint16, collectionName string, factory SingletFactory) {
	if collection >= math.MaxUint16-16 {
		panic(fmt.Errorf("collections identifier 0x%04X is reserved, libraries identifiers must be within 0x0000 and 0xEEEF", collection))
	}

	if actual, found := collections[collection]; found {
		panic(fmt.Errorf("collections identifier %d is already registered for %q, they all must be unique among registered ones", collection, actual.Name))
	}

	registerSingletFactory(collection, collectionName, factory)
}

func registerSingletFactory(collection uint16, collectionName string, factory SingletFactory) {
	collections[collection] = Collection{Identifier: collection, Name: collectionName}
	singletFactories[collection] = factory
}

// Singlet is a height-aware container for a single piece of information, for
// example an account's balance.
//
// A Singlet always contain a single row key but stored at any height.
type Singlet interface {
	// Collection to which this singlet is bound to.
	Collection() uint16

	// Identifier uniquely representing this Singlet instance within its collection.
	Identifier() []byte

	// Entry constructs a concrete SingletEntry implementation for this Singlet
	// at the specified height and associated to this value bytes. The entry usually
	// unmarshals the value and ensure it's valid.
	Entry(height uint64, value []byte) (SingletEntry, error)

	// String should turn the Singlet into a human readable form. If the
	// identifier is composed of multiple part, use `:` to delimit them in the string.
	//
	// You **should** add the collection name to the singlet identifier, it ease recognition
	// of which singlet collection you are seeing.
	//
	// This method is used by the various `Key*#String` helpers, they all expect this implementation
	// to have the collection name appended to them, so you should respect this soft constraint
	// and the `:` delimiter.
	String() string
}

// NewSinglet constructs a new Singlet implementation from the singlet key as stored
// in the underlying storage engine.
//
// The key received here will always contain the collection prefix (2 bytes) as well as enough
// bytes to contain the singlet identifier.
//
// **Important** The received key might contain more bytes, like the reminder of a full entry key,
// all code paths called here must not expected a maximum length and only deal with just enough
// bytes to read the data needed from the key.
func NewSinglet(singletKey []byte) (singlet Singlet, err error) {
	if len(singletKey) <= 2 {
		return nil, fmt.Errorf("invalid key length, expected at least 2 bytes, got %d", len(singletKey))
	}

	collectionKey := collectionFromKey(singletKey)

	singletFactory, foundFactory := singletFactories[collectionKey]
	if !foundFactory {
		return nil, fmt.Errorf("unknown collection 0x%04X", collectionKey)
	}

	singlet, err = singletFactory(singletKey[2:])
	if err != nil {
		return nil, fmt.Errorf("invalid singlet key: %w", err)
	}

	return
}

// SingletEqual returns wheter two Singlet instances are considered equal by
// comparing their respective collection and identifier.
//
// Two nil values are considered equal.
func SingletEqual(left, right Singlet) bool {
	if left == nil && right == nil {
		return true
	}

	if left == nil || right == nil {
		return false
	}

	return left.Collection() == right.Collection() && bytes.Equal(left.Identifier(), right.Identifier())
}

// SingletKey represents the storage key for a Singlet, contains the collection as
// well as the Singlet's identifier in byte form.
type SingletKey []byte

func (k SingletKeyAt) String() string {
	if len(k) <= 2+8 {
		return fmt.Sprintf("#Error<Invalid singlet entry key %q: invalid key length, expected at least 10 bytes, got %d>", Key(k), len(k))
	}

	singlet, err := NewSinglet([]byte(k))
	if err != nil {
		return fmt.Sprintf("#Error<Invalid singlet key at %q: %s>", Key(k), err)
	}

	return singlet.String() + ":" + hex.EncodeToString(k[len(k)-8:])
}

func (k SingletKey) String() string {
	singlet, err := NewSinglet([]byte(k))
	if err != nil {
		return fmt.Sprintf("#Error<Invalid singlet key %q: %s>", Key(k), err)
	}

	return singlet.String()
}

// SingletKeyAt represents the storage key for a Singlet at a given height, contains
// the collection, the Singlet's identifier as well as the height in byte form.
type SingletKeyAt []byte

func KeyForSingletAt(singlet Singlet, height uint64) (out SingletKeyAt) {
	identifier := singlet.Identifier()
	identifierBytes := len(identifier)

	out = make([]byte, collectionBytes+identifierBytes+heightBytes)
	copyCollection(out, singlet.Collection())
	copy(out[collectionBytes:], identifier)
	copyRevHeight(out[collectionBytes+identifierBytes:], height)
	return
}

func KeyForSinglet(singlet Singlet) (out SingletKey) {
	identifier := singlet.Identifier()
	identifierBytes := len(identifier)

	out = make([]byte, collectionBytes+identifierBytes)
	copyCollection(out, singlet.Collection())
	copy(out[collectionBytes:], identifier)
	return
}

type SingletEntry interface {
	Singlet() Singlet
	Height() uint64
	IsDeletion() bool

	MarshalValue() ([]byte, error)

	String() string
}

func NewSingletEntry(singlet Singlet, key []byte, value []byte) (SingletEntry, error) {
	singletIdentifier := singlet.Identifier()
	singletIdentifierBytes := len(singletIdentifier)

	heightOffset := collectionBytes + singletIdentifierBytes
	if heightOffset >= len(key) {
		return nil, fmt.Errorf("invalid key length, expected at least %d bytes, got %d", heightOffset, len(key))
	}

	collection := collectionFromKey(key)
	if singlet.Collection() != collection {
		return nil, fmt.Errorf("key from different collection, expected collection 0x%04X, got 0x%04X", singlet.Collection(), collection)
	}

	singletIdentifierEnd := collectionBytes + singletIdentifierBytes
	if !bytes.Equal(key[collectionBytes:singletIdentifierEnd], singletIdentifier) {
		return nil, fmt.Errorf("key from different tablet, expected tablet identifier %q, got %q", Key(singletIdentifier), Key(key[collectionBytes:singletIdentifierEnd]))
	}

	return singlet.Entry(math.MaxUint64-bigEndian.Uint64(key[heightOffset:]), value)
}

// SingletEntryKey represents a fully well-formed singlet entry key as written to the underlying
// storage engine. This key is always represented in the same but variable format:
//
// ```
// <collection (2 bytes)><singlet identifier (N bytes)><height (8 bytes)>
// ```
//
// Only the singlet implementation knows how to turn its series of bytes into the correct implementation.
type SingletEntryKey []byte

func KeyForSingletEntry(entry SingletEntry) (out SingletEntryKey) {
	singlet := entry.Singlet()
	singletIdentifier := singlet.Identifier()
	singletIdentifierBytes := len(singletIdentifier)

	out = make([]byte, collectionBytes+singletIdentifierBytes+heightBytes)
	copyCollection(out, singlet.Collection())
	copy(out[collectionBytes:], singletIdentifier)
	copyRevHeight(out[collectionBytes+singletIdentifierBytes:], entry.Height())
	return
}

func (k SingletEntryKey) String() string {
	singlet, err := NewSinglet(k)
	if err != nil {
		return fmt.Sprintf("#Error<Invalid singlet key %q: %s>", Key(k), err)
	}

	// We pass a nil value because we are only interested by the key formatting here
	entry, err := NewSingletEntry(singlet, []byte(k), nil)
	if err != nil {
		return fmt.Sprintf("#Error<Invalid singlet entry %q: %s>", Key(k), err)
	}

	return entry.String()
}

type BaseSingletEntry struct {
	singlet    Singlet
	height     uint64
	isDeletion bool
}

func NewBaseSingletEntry(singlet Singlet, height uint64, isDeletion bool) (out BaseSingletEntry) {
	out.singlet = singlet
	out.height = height
	out.isDeletion = isDeletion
	return
}

func (b BaseSingletEntry) Singlet() Singlet {
	return b.singlet
}

func (b BaseSingletEntry) Height() uint64 {
	return b.height
}

func (b BaseSingletEntry) IsDeletion() bool {
	return b.isDeletion
}

func (b BaseSingletEntry) String() string {
	return fmt.Sprintf("%s:%016x", b.singlet, math.MaxUint64-b.height)
}

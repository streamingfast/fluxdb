package fluxdb

import (
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyForSinglet(t *testing.T) {
	tests := []struct {
		name        string
		singlet     Singlet
		expectedHex string
	}{
		{"standard", testSinglet("abc"), "fff1616263"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedHex, hex.EncodeToString(KeyForSinglet(test.singlet)))
		})
	}
}

func TestKeyForSingletAt(t *testing.T) {
	tests := []struct {
		name        string
		singlet     Singlet
		height      uint64
		expectedHex string
	}{
		{"zero", testSinglet("abc"), 0, "fff1616263ffffffffffffffff"},
		{"standard", testSinglet("abc"), 10, "fff1616263fffffffffffffff5"},
		{"max", testSinglet("abc"), math.MaxUint64, "fff16162630000000000000000"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedHex, hex.EncodeToString(KeyForSingletAt(test.singlet, test.height)))
		})
	}
}

func TestKeyForSingletRow(t *testing.T) {
	tests := []struct {
		name        string
		entry       SingletEntry
		expectedHex string
	}{
		{"zero", testSinglet("abc").entry(t, 0, "ghi"), "fff1616263ffffffffffffffff"},
		{"standard", testSinglet("abc").entry(t, 10, "ghi"), "fff1616263fffffffffffffff5"},
		{"max", testSinglet("abc").entry(t, math.MaxUint64, "ghi"), "fff16162630000000000000000"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedHex, hex.EncodeToString(KeyForSingletEntry(test.entry)))
		})
	}
}

func TestRegisterSingletFactory(t *testing.T) {
	tests := []struct {
		name             string
		inCollection     uint16
		inCollectionName string
		expectedPanic    error
	}{
		// Beware, some collection are already injected by the system, so be sure to not use one of them.
		// - IndexSinglet 0xFFFF
		// - testSinglet 0xFFF1
		// - testSinglet 0xFFF2
		{"non-reserved, smallest", 0x0000, "smallest", nil},
		{"non-reserved, highest", 0xEEEF, "highest", nil},
		{"reserved, smallest", 0xFFF0, "reserved_smallest", errors.New("collections identifier 0xFFF0 is reserved, libraries identifiers must be within 0x0000 and 0xEEEF")},
		{"reserved, highest", 0xFFFF, "reserved_highest", errors.New("collections identifier 0xFFFF is reserved, libraries identifiers must be within 0x0000 and 0xEEEF")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				delete(collections, test.inCollection)
				delete(singletFactories, test.inCollection)
			}()

			factory := func(identifier []byte) (Singlet, error) {
				return testSinglet(string(identifier)), nil
			}

			if test.expectedPanic != nil {
				panicked, value := didPanic(func() { RegisterSingletFactory(test.inCollection, test.inCollectionName, factory) })
				require.True(t, panicked, "The func should have panic but did not")
				require.Equal(t, test.expectedPanic, value)

				// The collection 0xFFFF is already registered, so we cannnot check if it's not present anymore
				if test.inCollection != 0xFFFF {
					assert.NotContains(t, collections, test.inCollection, "collections")
					assert.NotContains(t, singletFactories, test.inCollection, "singlet factories")
				}
				return
			}

			RegisterSingletFactory(test.inCollection, test.inCollectionName, factory)
			assert.Equal(t, Collection{Identifier: test.inCollection, Name: test.inCollectionName}, collections[test.inCollection])

			singlet, err := singletFactories[test.inCollection]([]byte("test"))
			require.NoError(t, err)
			assert.Equal(t, testSinglet("test"), singlet)
		})
	}
}

var testSingletCollection uint16 = 0xFFF1
var testSingletCollectionName string = "sts"

type testSinglet string

func init() {
	registerSingletFactory(testSingletCollection, "sts", func(identifier []byte) (Singlet, error) {
		return newTestSinglet(string(identifier[0:3])), nil
	})
}

func newTestSinglet(elementName string) testSinglet {
	if len(elementName) != 3 {
		panic("test singlet name must always be 3 characters long")
	}

	return testSinglet(elementName)
}

func (s testSinglet) Collection() uint16 {
	return testSingletCollection
}

func (s testSinglet) Identifier() []byte {
	return []byte(s)
}

func (s testSinglet) Entry(height uint64, data []byte) (SingletEntry, error) {
	return testSingletEntry{NewBaseSingletEntry(s, height, data)}, nil
}

func (s testSinglet) entry(tt *testing.T, height uint64, data string) SingletEntry {
	return testSingletEntry{NewBaseSingletEntry(s, height, []byte(data))}
}

func (s testSinglet) String() string {
	return testSingletCollectionName + ":" + string(s)
}

type testSingletEntry struct {
	BaseSingletEntry
}

func (e testSingletEntry) data() string {
	return string(e.value)
}

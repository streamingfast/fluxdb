package fluxdb

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTablet(t *testing.T) {
	tests := []struct {
		name           string
		tabletKey      string
		expectedTablet Tablet
		expectedErr    string
	}{
		{"only table", "fff2616263", testTablet("abc"), noError},
		{"with height", "fff26162630000000000000001", testTablet("abc"), noError},
		{"with height and primary key", "fff26162630000000000000001676869", testTablet("abc"), noError},

		{"enough bytes, unknown tablet", "fff061", nil, "unknown collection 0xFFF0"},

		{"not enough bytes, empty", "", nil, "invalid key length, expected at least 3 bytes, got 0"},
		{"not enough bytes, just collection", "fff2", nil, "invalid key length, expected at least 3 bytes, got 2"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tablet, err := NewTablet(B(test.tabletKey))

			if test.expectedErr != "" {
				require.EqualError(t, err, test.expectedErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedTablet, tablet)
			}
		})
	}
}

func TestNewTabletRow(t *testing.T) {
	tests := []struct {
		name               string
		tablet             Tablet
		rowKey             string
		expectedHeight     uint64
		expectedPrimaryKey string
		expectedErr        string
	}{
		{"full key", testTablet("abc"), "fff26162630000000000000001676869", 1, "ghi", noError},

		{"enough bytes, collection mistmatch", testTablet("abc"), "fff06162630000000000000001676869", 0, "", "key from different collection, expected collection 0xFFF2, got 0xFFF0"},
		{"enough bytes, tablet mistmatch", testTablet("abc"), "fff26162640000000000000001676869", 0, "", `key from different tablet, expected tablet identifier "616263", got "616264"`},

		{"not enough bytes, empty", testTablet("abc"), "", 0, "", "invalid key length, expected at least 14 bytes, got 0"},
		{"not enough bytes, collection only", testTablet("abc"), "fff2", 0, "", "invalid key length, expected at least 14 bytes, got 2"},
		{"not enough bytes, collection and tablet only", testTablet("abc"), "fff2616263", 0, "", "invalid key length, expected at least 14 bytes, got 5"},
		{"not enough bytes, collection, tablet and height", testTablet("abc"), "fff26162630000000000000001", 0, "", "invalid key length, expected at least 14 bytes, got 13"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row, err := NewTabletRow(test.tablet, B(test.rowKey), nil)

			if test.expectedErr != "" {
				require.EqualError(t, err, test.expectedErr)
			} else {
				require.NoError(t, err)

				expectedRow, err := test.tablet.Row(test.expectedHeight, []byte(test.expectedPrimaryKey), nil)
				require.NoError(t, err)

				assert.Equal(t, expectedRow, row)
			}
		})
	}
}

func TestKeyForTablet(t *testing.T) {
	tests := []struct {
		name        string
		tablet      Tablet
		expectedHex string
	}{
		{"standard", testTablet("abc"), "fff2616263"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedHex, hex.EncodeToString(KeyForTablet(test.tablet)))
		})
	}
}

func TestKeyForTabletAt(t *testing.T) {
	tests := []struct {
		name        string
		tablet      Tablet
		height      uint64
		expectedHex string
	}{
		{"zero", testTablet("abc"), 0, "fff26162630000000000000000"},
		{"standard", testTablet("abc"), 10, "fff2616263000000000000000a"},
		{"max", testTablet("abc"), math.MaxUint64, "fff2616263ffffffffffffffff"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedHex, hex.EncodeToString(KeyForTabletAt(test.tablet, test.height)))
		})
	}
}

func TestKeyForTabletRow(t *testing.T) {
	tests := []struct {
		name        string
		row         TabletRow
		expectedHex string
	}{
		{"zero", testTablet("abc").row(t, 0, "ghi", ""), "fff26162630000000000000000676869"},
		{"standard", testTablet("abc").row(t, 10, "ghi", ""), "fff2616263000000000000000a676869"},
		{"max", testTablet("abc").row(t, math.MaxUint64, "ghi", ""), "fff2616263ffffffffffffffff676869"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedHex, hex.EncodeToString(KeyForTabletRow(test.row)))
		})
	}
}

func TestKeyForTabletRowParts(t *testing.T) {
	tests := []struct {
		name        string
		tablet      Tablet
		height      uint64
		primaryKey  []byte
		expectedHex string
	}{
		{"zero", testTablet("abc"), 0, []byte("ghi"), "fff26162630000000000000000676869"},
		{"standard", testTablet("abc"), 10, []byte("ghi"), "fff2616263000000000000000a676869"},
		{"max", testTablet("abc"), math.MaxUint64, []byte("ghi"), "fff2616263ffffffffffffffff676869"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedHex, hex.EncodeToString(KeyForTabletRowParts(test.tablet, test.height, test.primaryKey)))
		})
	}
}

func TestRegisterTabletFactory(t *testing.T) {
	tests := []struct {
		name             string
		inCollection     uint16
		inCollectionName string
		expectedPanic    error
	}{
		// Beware, some collection are already injected by the system, so be sure to not use one of them.
		// - IndexSinglet 0xFFFF
		// - testSinglet 0xFFF1
		// - testTablet 0xFFF2
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

			factory := func(identifier []byte) (Tablet, error) {
				return testTablet(string(identifier)), nil
			}

			if test.expectedPanic != nil {
				panicked, value := didPanic(func() { RegisterTabletFactory(test.inCollection, test.inCollectionName, factory) })
				require.True(t, panicked, "The func should have panic but did not")
				require.Equal(t, test.expectedPanic, value)

				// The collection 0xFFFF is already registered, so we cannnot check if it's not present anymore
				if test.inCollection != 0xFFFF {
					assert.NotContains(t, collections, test.inCollection, "collections")
					assert.NotContains(t, tabletFactories, test.inCollection, "tablet factories")
				}
				return
			}

			RegisterTabletFactory(test.inCollection, test.inCollectionName, factory)
			assert.Equal(t, Collection{Identifier: test.inCollection, Name: test.inCollectionName}, collections[test.inCollection])

			tablet, err := tabletFactories[test.inCollection]([]byte("test"))
			require.NoError(t, err)
			assert.Equal(t, testTablet("test"), tablet)
		})
	}
}

var testTabletCollection uint16 = 0xFFF2
var testTabletCollectionName string = "tst"

type testTablet string

func init() {
	registerTabletFactory(testTabletCollection, testTabletCollectionName, func(identifier []byte) (Tablet, error) {
		return newTestTablet(string(identifier[0:3])), nil
	})
}

func newTestTablet(tableName string) testTablet {
	if len(tableName) != 3 {
		panic("test tablet name must always be 3 characters long")
	}

	return testTablet(tableName)
}

func (t testTablet) Collection() uint16 { return testTabletCollection }

func (t testTablet) Identifier() []byte { return []byte(t) }

func (t testTablet) Row(height uint64, primaryKey []byte, value []byte) (TabletRow, error) {
	if len(primaryKey) != 3 {
		return nil, fmt.Errorf("test tablet row primary key must always contain 3 bytes")
	}

	return testTabletRow{NewBaseTabletRow(t, height, primaryKey, value)}, nil
}

func (t testTablet) row(tt *testing.T, height uint64, primaryKey string, value string) TabletRow {
	require.Len(tt, primaryKey, 3, "test tablet row primary key must always contain 3 bytes")

	return testTabletRow{NewBaseTabletRow(t, height, []byte(primaryKey), []byte(value))}
}

func (t testTablet) String() string {
	return testTabletCollectionName + ":" + string(t)
}

type testTabletRow struct {
	BaseTabletRow
}

func (r testTabletRow) PrimaryKey() []byte {
	return []byte(r.primaryKey)
}

func (r testTabletRow) String() string {
	return r.Stringify(string(r.PrimaryKey()))
}

func (r testTabletRow) data() string {
	return string(r.value)
}

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexSinglet_MarshalUnmarshalBinary(t *testing.T) {
	tests := []struct {
		name   string
		tablet Tablet
		index  *TabletIndex
	}{
		{
			"no_rows",
			testTablet(""),
			&TabletIndex{
				AtHeight:           6,
				SquelchCount:       2,
				PrimaryKeyToHeight: &primaryKeyToHeightMap{bytesMap: &bytesMap{}},
			},
		},
		{
			"multi_rows",
			testTablet(""),
			&TabletIndex{
				AtHeight:     6,
				SquelchCount: 2,
				PrimaryKeyToHeight: &primaryKeyToHeightMap{bytesMap: &bytesMap{
					mappings: map[string]interface{}{
						"0000000000000002": uint64(4),
						"0000000000000003": uint64(5),
					},
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := test.index
			singlet := newIndexSinglet(test.tablet)

			entry := newIndexSingletEntry(singlet, test.index)

			binary, err := entry.MarshalValue()
			require.NoError(t, err)

			newEntry, err := singlet.Entry(test.index.AtHeight, binary)
			require.NoError(t, err)

			actual := newEntry.(indexSingletEntry).index

			assert.Equal(t, index.SquelchCount, actual.SquelchCount)
			assert.Equal(t, index.PrimaryKeyToHeight.mappings, actual.PrimaryKeyToHeight.mappings)
		})
	}
}

func TestShouldTriggerIndexing(t *testing.T) {
	tests := []struct {
		label          string
		indexRowCount  int
		mutationsCount int
		expect         bool
	}{
		{
			label:          "no indexing",
			mutationsCount: 1,
			expect:         false,
		},
		{
			label:          "flush on 999",
			mutationsCount: 999,
			expect:         false,
		},
		{
			label:          "flush on 1000",
			mutationsCount: 1000,
			expect:         true,
		},
		{
			label:          "single row table, 1500 mutations",
			indexRowCount:  1,
			mutationsCount: 1500,
			expect:         true,
		},
		{
			label:          "55000 row table, 1500 mutations",
			indexRowCount:  55000,
			mutationsCount: 1500,
			expect:         false,
		},
		{
			label:          "55000 row table, 5500 mutations",
			indexRowCount:  55000,
			mutationsCount: 5500,
			expect:         true,
		},
		{
			label:          "55000 row table, 3000 mutations",
			indexRowCount:  75000,
			mutationsCount: 3000,
			expect:         false,
		},
		{
			label:          "110000 row table, 8000 mutations",
			indexRowCount:  110000,
			mutationsCount: 8000,
			expect:         false,
		},
		{
			label:          "110000 row table, 11000 mutations",
			indexRowCount:  110000,
			mutationsCount: 11000,
			expect:         true,
		},
	}

	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			tablet := testTablet("a")
			tabletKey := KeyForTablet(tablet)

			cache := &indexCache{
				lastCounters: make(map[string]int),
				lastIndexes:  make(map[string]*TabletIndex),
			}
			cache.lastCounters[string(tabletKey)] = test.mutationsCount
			if test.indexRowCount != 0 {
				t := &TabletIndex{PrimaryKeyToHeight: newPrimaryKeyToHeightMap(8)}
				for i := 0; i < test.indexRowCount; i++ {
					t.PrimaryKeyToHeight.put([]byte(fmt.Sprintf("%016x", i)), 0)
				}
				cache.lastIndexes[string(tabletKey)] = t
			}
			res := cache.shouldTriggerIndexing(tabletKey)
			assert.Equal(t, test.expect, res)
		})
	}
}

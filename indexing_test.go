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
				AtHeight:     6,
				SquelchCount: 2,
				PrimaryKeyToHeight: &primaryKeyToHeightMap{bytesMap: &bytesMap{
					mappings: map[string]interface{}{}, // empty map, not nil map
				}},
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
			label:          "no previous index, 1 mutation, skipped",
			mutationsCount: 1,
			expect:         false,
		},
		{
			label:          "no previous index, below first boundary (25K), skipped",
			mutationsCount: 24999,
			expect:         false,
		},
		{
			label:          "no previous index, above first boundary (25K), indexing",
			mutationsCount: 35000,
			expect:         true,
		},

		{
			label:          "small table, 1 mutation, skipped",
			indexRowCount:  12500,
			mutationsCount: 1,
			expect:         false,
		},
		{
			label:          "small table, below first boundary (25K), skipped",
			indexRowCount:  12500,
			mutationsCount: 24999,
			expect:         false,
		},
		{
			label:          "small table, above first boundary (25K), indexing",
			indexRowCount:  12500,
			mutationsCount: 35000,
			expect:         true,
		},

		{
			label:          "medium table low end, 1 mutation, skipped",
			indexRowCount:  50001,
			mutationsCount: 1,
			expect:         false,
		},
		{
			label:          "medium table low end, below first boundary (25K), skipped",
			indexRowCount:  50001,
			mutationsCount: 24999,
			expect:         false,
		},
		{
			label:          "medium table low end, above first boundary (25K), below half row, skipped",
			indexRowCount:  50001,
			mutationsCount: 25000,
			expect:         false,
		},
		{
			label:          "medium table low end, above first boundary (25K), above half row, indexing",
			indexRowCount:  50001,
			mutationsCount: 35000,
			expect:         true,
		},

		{
			label:          "medium table high end, 1 mutation, skipped",
			indexRowCount:  200000,
			mutationsCount: 1,
			expect:         false,
		},
		{
			label:          "medium table high end, below first boundary (25K), skipped",
			indexRowCount:  200000,
			mutationsCount: 24999,
			expect:         false,
		},
		{
			label:          "medium table high end, above first boundary (25K), below half row, skipped",
			indexRowCount:  200000,
			mutationsCount: 75000,
			expect:         false,
		},
		{
			label:          "medium table high end, above first boundary (25K), above half row, indexing",
			indexRowCount:  200000,
			mutationsCount: 100001,
			expect:         true,
		},

		{
			label:          "big table, 1 mutation, skipped",
			indexRowCount:  200001,
			mutationsCount: 1,
			expect:         false,
		},
		{
			label:          "big table, below first boundary (25K), skipped",
			indexRowCount:  200001,
			mutationsCount: 24999,
			expect:         false,
		},
		{
			label:          "big table, above first boundary (25K), below 100K row, skipped",
			indexRowCount:  200001,
			mutationsCount: 99999,
			expect:         false,
		},
		{
			label:          "big table, above first boundary (25K), above 100K row, indexing",
			indexRowCount:  200001,
			mutationsCount: 100001,
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

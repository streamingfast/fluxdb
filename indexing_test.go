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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTabletIndex_MarshalUnmarshalBinary(t *testing.T) {
	type expected struct {
		tableIndex *TableIndex
		err        error
	}

	tests := []struct {
		name     string
		tablet   Tablet
		atHeight uint64
		buffer   []byte
		expected expected
	}{
		{
			"no_rows",
			testTablet(""),
			6,
			[]byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // Squelched count
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Reserved
			},
			expected{&TableIndex{
				AtHeight:  6,
				Squelched: 2,
				Map:       map[string]uint64{},
			}, nil},
		},
		{
			"multi_rows",
			testTablet(""),
			6,
			[]byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // Squelched count
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Reserved
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // Table row mapping 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // Table row mapping 2
			},
			expected{&TableIndex{
				AtHeight:  6,
				Squelched: 2,
				Map: map[string]uint64{
					"0000000000000002": 4,
					"0000000000000003": 5,
				},
			}, nil},
		},
		{
			"misalign",
			testTablet(""),
			0,
			[]byte{0x00},
			expected{nil, errors.New("unable to unmarshal table index: 16 bytes alignment + 16 bytes metadata is off (has 1 bytes)")},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableIndex, err := NewTableIndexFromBinary(ctx, test.tablet, test.atHeight, test.buffer)

			require.Equal(t, test.expected.err, err)
			if test.expected.err == nil {
				assert.Equal(t, test.expected.tableIndex, tableIndex)

				bytes, err := tableIndex.MarshalBinary(ctx, test.tablet)
				require.NoError(t, err)

				tableIndexFromBytes, err := NewTableIndexFromBinary(ctx, test.tablet, test.atHeight, bytes)
				require.NoError(t, err)
				assert.Equal(t, test.expected.tableIndex, tableIndexFromBytes)
			}
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

			cache := &indexCache{
				lastCounters: make(map[Tablet]int),
				lastIndexes:  make(map[Tablet]*TableIndex),
			}
			cache.lastCounters[tablet] = test.mutationsCount
			if test.indexRowCount != 0 {
				t := &TableIndex{Map: make(map[string]uint64)}
				for i := 0; i < test.indexRowCount; i++ {
					t.Map[fmt.Sprintf("%08x", i)] = 0
				}
				cache.lastIndexes[tablet] = t
			}
			res := cache.shouldTriggerIndexing(tablet)
			assert.Equal(t, test.expect, res)
		})
	}
}

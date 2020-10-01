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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/dfuse-io/derr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"
)

func TestReadTabletAt_WithSpeculative(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	tablet := newTestTablet("tbl")

	writeBatchOfRequests(t, db,
		&WriteRequest{TabletRows: []TabletRow{tablet.row(t, height, "002", "abc")}},
	)

	speculativeWrites := []*WriteRequest{
		tabletRows(height+1, tablet.row(t, height+1, "002", "")),
	}

	rows, err := db.ReadTabletAt(context.Background(), height+1, tablet, speculativeWrites)

	require.NoError(t, err)
	require.Len(t, rows, 0)
}

func TestReadTabletAt_OnlySpeculative(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	tablet := newTestTablet("tbl")

	speculativeWrites := []*WriteRequest{
		tabletRows(height+1, tablet.row(t, height+1, "002", "abc")),
	}

	rows, err := db.ReadTabletAt(context.Background(), height+1, tablet, speculativeWrites)

	require.NoError(t, err)
	assert.Equal(t, []TabletRow{tablet.row(t, height+1, "002", "abc")}, rows)
}

func TestReadTabletAt_WithIndex(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	tablet := newTestTablet("tbl")
	index := NewTabletIndex()
	index.AtHeight = height
	index.SquelchCount = 1
	index.PrimaryKeyToHeight.put([]byte("002"), height)

	writeBatchOfRequests(t, db,
		&WriteRequest{TabletRows: []TabletRow{tablet.row(t, height, "002", "abc")}},
		&WriteRequest{SingletEntries: []SingletEntry{newIndexSingletEntry(newIndexSinglet(tablet), index)}},
	)

	rows, err := db.ReadTabletAt(context.Background(), height+1, tablet, nil)

	require.NoError(t, err)
	require.Equal(t, []TabletRow{tablet.row(t, height, "002", "abc")}, rows)
}

func TestReadTabletAt_IndexThenDeleted(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	tablet := newTestTablet("tbl")
	index := NewTabletIndex()
	index.AtHeight = height
	index.SquelchCount = 1
	index.PrimaryKeyToHeight.put([]byte("002"), height)

	writeBatchOfRequests(t, db,
		&WriteRequest{TabletRows: []TabletRow{tablet.row(t, height, "002", "abc")}},
		&WriteRequest{SingletEntries: []SingletEntry{newIndexSingletEntry(newIndexSinglet(tablet), index)}},
	)

	writeBatchOfRequests(t, db,
		&WriteRequest{TabletRows: []TabletRow{tablet.row(t, height+1, "002", "")}},
	)

	rows, err := db.ReadTabletAt(context.Background(), height+2, tablet, nil)

	require.NoError(t, err)
	require.Len(t, rows, 0)
}

func TestReadTabletAt_IndexThenDeletedThenSpeculativeInserted(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	tablet := newTestTablet("tbl")
	index := NewTabletIndex()
	index.AtHeight = height
	index.SquelchCount = 1
	index.PrimaryKeyToHeight.put([]byte("002"), height)

	writeBatchOfRequests(t, db,
		&WriteRequest{TabletRows: []TabletRow{tablet.row(t, height, "002", "abc")}},
		&WriteRequest{SingletEntries: []SingletEntry{newIndexSingletEntry(newIndexSinglet(tablet), index)}},
	)

	writeBatchOfRequests(t, db,
		&WriteRequest{TabletRows: []TabletRow{tablet.row(t, height+1, "002", "")}},
	)

	speculativeWrites := []*WriteRequest{
		tabletRows(height, tablet.row(t, height+2, "002", "def")),
	}

	rows, err := db.ReadTabletAt(context.Background(), height+2, tablet, speculativeWrites)

	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, tablet.row(t, height+2, "002", "def"), rows[0])
}

func TestReadTabletRowAt_OnlyFromIndex(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	tablet := newTestTablet("tbl")
	index := NewTabletIndex()
	index.AtHeight = height
	index.SquelchCount = 1
	index.PrimaryKeyToHeight.put([]byte("002"), uint64(100))

	writeBatchOfRequests(t, db,
		&WriteRequest{TabletRows: []TabletRow{tablet.row(t, 100, "002", "abc")}},
		&WriteRequest{SingletEntries: []SingletEntry{newIndexSingletEntry(newIndexSinglet(tablet), index)}},
	)

	row, err := db.ReadTabletRowAt(context.Background(), height+1, tablet, testTabletRowPrimaryKey([]byte("002")), nil)

	require.NoError(t, err)
	require.Equal(t, tablet.row(t, 100, "002", "abc"), row)
}

func TestReadSingletAt(t *testing.T) {
	tests := []struct {
		name           string
		entries        []uint64
		fetchForHeight uint64
		expectedEntry  string
		expectedNil    bool
	}{
		{
			name: "fetch after last",
			entries: []uint64{
				3, 5,
			},
			fetchForHeight: 6,
			expectedEntry:  `5`,
		},
		{
			name: "fetch between two",
			entries: []uint64{
				3, 5,
			},
			fetchForHeight: 4,
			expectedEntry:  `3`,
		},
		{
			name: "fetch on the betweener",
			entries: []uint64{
				3, 4, 5,
			},
			fetchForHeight: 4,
			expectedEntry:  `4`,
		},
		{
			name: "fetch on last",
			entries: []uint64{
				3, 5,
			},
			fetchForHeight: 5,
			expectedEntry:  `5`,
		},
		{
			name: "fetch on first",
			entries: []uint64{
				3, 5,
			},
			fetchForHeight: 3,
			expectedEntry:  `3`,
		},
		{
			name: "fetch before first",
			entries: []uint64{
				3, 5,
			},
			fetchForHeight: 2,
			expectedNil:    true,
		},

		{
			name: "returns nil, nil when entry not found and no error",
			entries: []uint64{
				3, 5,
			},
			fetchForHeight: 2,
			expectedNil:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, closer := NewTestDB(t)
			defer closer()

			singlet := newTestSinglet("tst")
			for _, height := range test.entries {
				writeBatchOfRequests(t, db,
					&WriteRequest{SingletEntries: []SingletEntry{singlet.entry(t, height, fmt.Sprintf("%d", height))}},
				)
			}

			entry, err := db.ReadSingletEntryAt(context.Background(), singlet, test.fetchForHeight, nil)
			if test.expectedNil {
				require.Nil(t, entry, "entry is not nil")
			} else {
				require.NoError(t, err)
				require.NotNil(t, entry, "entry is nil")
				assert.Equal(t, test.expectedEntry, entry.(testSingletEntry).data())
			}
		})
	}
}

func TestReadSingletAt_WithSpeculative(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	singlet := newTestSinglet("tst")

	writeBatchOfRequests(t, db,
		&WriteRequest{SingletEntries: []SingletEntry{singlet.entry(t, height, "001")}},
	)

	speculativeWrites := []*WriteRequest{
		singletEntries(height+1, singlet.entry(t, height+1, "002")),
	}

	entry, err := db.ReadSingletEntryAt(context.Background(), singlet, height+1, speculativeWrites)

	require.NoError(t, err)
	assert.Equal(t, singlet.entry(t, height+1, "002"), entry)
}

func TestReadSingletAt_OnlyInSpeculative(t *testing.T) {
	db, closer := NewTestDB(t)
	defer closer()

	height := uint64(123)
	singlet := newTestSinglet("tst")

	speculativeWrites := []*WriteRequest{
		singletEntries(height+1, singlet.entry(t, height+1, "002")),
	}

	entry, err := db.ReadSingletEntryAt(context.Background(), singlet, height+1, speculativeWrites)

	require.NoError(t, err)
	assert.Equal(t, singlet.entry(t, height+1, "002"), entry)
}

func assertError(t *testing.T, expected error, actual error) {
	require.Error(t, actual)

	switch v := expected.(type) {
	case *derr.ErrorResponse:
		assertErrorResponse(t, v, actual)
	default:
		assert.Equal(t, expected, actual)
	}
}

func assertErrorResponse(t *testing.T, expected *derr.ErrorResponse, actual error) {
	v, ok := actual.(*derr.ErrorResponse)
	require.True(t, ok, "actual value must be a *derr.ErrorResponse type")

	assert.Equal(t, expected, v)
}

func fixedTraceID(hexInput string) (out trace.TraceID) {
	rawTraceID, _ := hex.DecodeString(hexInput)
	copy(out[:], rawTraceID)

	return
}

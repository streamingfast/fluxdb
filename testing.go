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
	"testing"

	"github.com/dfuse-io/bstream"
	"github.com/stretchr/testify/require"
)

func writeBatchOfRequests(t *testing.T, db *FluxDB, requests ...*WriteRequest) {
	for _, request := range requests {
		if request.BlockRef == nil {
			request.BlockRef = bstream.BlockRefEmpty
		}
	}

	require.NoError(t, db.WriteBatch(context.Background(), requests))
}

func singletEntries(height uint64, entries ...SingletEntry) *WriteRequest {
	return &WriteRequest{
		Height:         height,
		SingletEntries: entries,
	}
}

func tabletRows(height uint64, rows ...TabletRow) *WriteRequest {
	return &WriteRequest{
		Height:     height,
		TabletRows: rows,
	}
}

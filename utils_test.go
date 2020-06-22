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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dfuse-io/fluxdb/store/kv"
	_ "github.com/dfuse-io/kvdb/store/badger"
	_ "github.com/dfuse-io/kvdb/store/bigkv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_chunkKeyRevHeight(t *testing.T) {
	tests := []struct {
		key           string
		prefixKey     string
		expected      uint64
		expectedError error
	}{
		{"ts:0000:ffffffffffffebd1", "ts:0000:", 5166, nil},
		{"ts:0000:ffffffffffffebd1:00000000:000000", "ts:0000:", 5166, nil},

		{"ta:0000:fffffffffffffebd", "ts:0000:", 0, errors.New("key ta:0000:fffffffffffffebd should start with prefix key ts:0000:")},
		{"ts:0000:ffffffffffffebd", "ts:0000:", 0, errors.New("key ts:0000:ffffffffffffebd is too small too contains block num, should have at least 16 characters more than prefix")},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actual, err := chunkKeyRevHeight(test.key, test.prefixKey)
			if test.expectedError == nil {
				require.NoError(t, err)
				assert.Equal(t, test.expected, actual)
			} else {
				require.Equal(t, test.expectedError, err)
			}
		})
	}
}

func NewTestDB(t *testing.T) (*FluxDB, func()) {
	tmp, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	kvStore, err := kv.NewStore(fmt.Sprintf("badger://%s/test.db?createTables=true", tmp))
	require.NoError(t, err)

	db := New(kvStore, nil)
	closer := func() {
		db.Close()
		os.RemoveAll(tmp)
	}

	return db, closer
}

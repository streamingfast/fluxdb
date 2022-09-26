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
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/streamingfast/fluxdb/store/kv"
	"github.com/streamingfast/jsonpb"
	_ "github.com/streamingfast/kvdb/store/badger"
	_ "github.com/streamingfast/kvdb/store/bigkv"
	pbfluxdb "github.com/streamingfast/pbgo/sf/fluxdb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func NewTestDB(t *testing.T) (*FluxDB, func()) {
	tmp, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	kvStore, err := kv.NewStore(fmt.Sprintf("badger://%s/test.db?createTables=true", tmp))
	require.NoError(t, err)

	db := New(kvStore, nil, nil, false)
	closer := func() {
		db.Close()
		os.RemoveAll(tmp)
	}

	return db, closer
}

func TestCheckpoint_Unmarshal(t *testing.T) {
	hexString := "08c1c3f21a124708c1c3f21a124030333563613163316564376562303335346362643131303033366433656636663630383830623265643562643833666562626431616136663239616332346564"
	data, err := hex.DecodeString(hexString)
	require.NoError(t, err)

	checkpoint := &pbfluxdb.Checkpoint{}
	err = proto.Unmarshal(data, checkpoint)
	require.NoError(t, err)

	_, err = jsonpb.MarshalIndentToString(checkpoint, "  ")
	require.NoError(t, err)
}

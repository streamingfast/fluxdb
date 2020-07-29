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

	"github.com/dfuse-io/bstream"
)

const collectionBytes = 2
const heightBytes = 8

var collections = map[uint16]Collection{}

func collectionFromKey(k []byte) uint16 {
	return bigEndian.Uint16(k)
}

type Collection struct {
	Identifier uint16
	Name       string
}

type BlockMapper interface {
	Map(rawBlk *bstream.Block) (*WriteRequest, error)
}

type WriteRequest struct {
	SingletEntries []SingletEntry
	TabletRows     []TabletRow

	Height   uint64
	BlockRef bstream.BlockRef
}

func (r *WriteRequest) AppendSingletEntry(entry SingletEntry) {
	r.SingletEntries = append(r.SingletEntries, entry)
}

func (r *WriteRequest) AppendTabletRow(row TabletRow) {
	r.TabletRows = append(r.TabletRows, row)
}

type bytesMap struct {
	mappings map[string]interface{}
}

func newBytesMap() *bytesMap {
	return &bytesMap{mappings: make(map[string]interface{})}
}

func (m *bytesMap) _put(key []byte, value interface{}) {
	m.mappings[string(key)] = value
}

func (m *bytesMap) _get(key []byte) (value interface{}, found bool) {
	value, found = m.mappings[string(key)]
	return
}

func (m *bytesMap) has(key []byte) bool {
	_, found := m.mappings[string(key)]
	return found
}

func (m *bytesMap) delete(key []byte) {
	delete(m.mappings, string(key))
}

func (m *bytesMap) len() int {
	return len(m.mappings)
}

type Key []byte

func (k Key) String() string {
	return hex.EncodeToString(k)
}

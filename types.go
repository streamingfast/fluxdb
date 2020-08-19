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

	"github.com/dfuse-io/bstream"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
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

func NewWriteRequestFromProto(request *pbfluxdb.WriteRequest) (*WriteRequest, error) {
	r := &WriteRequest{
		SingletEntries: make([]SingletEntry, len(request.SingletEntries)),
		TabletRows:     make([]TabletRow, len(request.TabletRows)),
		Height:         request.Height,
		BlockRef:       bstream.NewBlockRef(request.Block.Id, request.Block.Num),
	}

	var err error
	for i, entry := range request.SingletEntries {
		if r.SingletEntries[i], err = NewSingletEntryFromStorage(entry.Key, entry.Value); err != nil {
			return nil, fmt.Errorf("singlet entry: %w", err)
		}
	}

	for i, row := range request.TabletRows {
		if r.TabletRows[i], err = NewTabletRowFromStorage(row.Key, row.Value); err != nil {
			return nil, fmt.Errorf("tablet row: %w", err)
		}
	}

	return r, nil
}

func (r *WriteRequest) AppendSingletEntry(entry SingletEntry) {
	r.SingletEntries = append(r.SingletEntries, entry)
}

func (r *WriteRequest) AppendTabletRow(row TabletRow) {
	r.TabletRows = append(r.TabletRows, row)
}

func (r *WriteRequest) ToProto() (*pbfluxdb.WriteRequest, error) {
	request := &pbfluxdb.WriteRequest{
		SingletEntries: make([]*pbfluxdb.WriteEntry, len(r.SingletEntries)),
		TabletRows:     make([]*pbfluxdb.WriteEntry, len(r.TabletRows)),
		Height:         r.Height,
		Block:          &pbbstream.BlockRef{Num: r.BlockRef.Num(), Id: r.BlockRef.ID()},
	}

	var err error
	for i, entry := range r.SingletEntries {
		if request.SingletEntries[i], err = singletEntryToProto(entry); err != nil {
			return nil, fmt.Errorf("singlet entry: %w", err)
		}
	}

	for i, row := range r.TabletRows {
		if request.TabletRows[i], err = tabletRowToProto(row); err != nil {
			return nil, fmt.Errorf("table row: %w", err)
		}
	}

	return request, nil
}

func singletEntryToProto(entry SingletEntry) (*pbfluxdb.WriteEntry, error) {
	return genericElementToProto(KeyForSingletEntry(entry), entry)
}

func tabletRowToProto(row TabletRow) (*pbfluxdb.WriteEntry, error) {
	return genericElementToProto(KeyForTabletRow(row), row)
}

type marshallerValue interface {
	MarshalValue() ([]byte, error)
}

func genericElementToProto(key []byte, marshaller marshallerValue) (*pbfluxdb.WriteEntry, error) {
	value, err := marshaller.MarshalValue()
	if err != nil {
		return nil, fmt.Errorf("marshal value: %w", err)
	}

	return &pbfluxdb.WriteEntry{
		Key:   key,
		Value: value,
	}, nil
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

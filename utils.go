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
	"math"
	"strconv"
	"strings"
)

func HexHeight(height uint64) string {
	return fmt.Sprintf("%016x", height)
}

func HexRevHeight(height uint64) string {
	return HexHeight(math.MaxUint64 - height)
}

func copyCollection(dst []byte, collection uint16) {
	bigEndian.PutUint16(dst, collection)
}

func copyHeight(dst []byte, height uint64) {
	bigEndian.PutUint64(dst, height)
}

func copyRevHeight(dst []byte, height uint64) {
	copyHeight(dst, math.MaxUint64-height)
}

// chunkKeyRevHeight returns the actual block num out of a
// reverse-encoded block num
func chunkKeyRevHeight(key string, prefixKey string) (height uint64, err error) {
	height, err = chunkKeyHeight(key, prefixKey)
	if err != nil {
		return 0, err
	}

	return math.MaxUint64 - height, nil
}

func chunkKeyHeight(key string, prefixKey string) (height uint64, err error) {
	if !strings.HasPrefix(key, prefixKey) {
		return 0, fmt.Errorf("key %s should start with prefix key %s", key, prefixKey)
	}

	if len(key) < len(prefixKey)+16 {
		return 0, fmt.Errorf("key %s is too small too contains block num, should have at least 16 characters more than prefix", key)
	}

	revHeight := key[len(prefixKey) : len(prefixKey)+16]
	if len(revHeight) != 16 {
		return 0, fmt.Errorf("revHeight %s should have a length of 16", revHeight)
	}

	val, err := strconv.ParseUint(revHeight, 16, 64)
	if err != nil {
		return 0, err
	}

	return val, nil
}

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
	"encoding/binary"
	"fmt"
	"math"
)

var bigEndian = binary.BigEndian

func copyCollection(dst []byte, collection uint16) {
	bigEndian.PutUint16(dst, collection)
}

func copyHeight(dst []byte, height uint64) {
	bigEndian.PutUint64(dst, height)
}

func copyRevHeight(dst []byte, height uint64) {
	copyHeight(dst, math.MaxUint64-height)
}

func ErrInvalidKeyLength(tag string, expected, actual int) error {
	return fmt.Errorf("invalid %s length, expected %d bytes, got %d", tag, expected, actual)
}

func ErrInvalidKeyLengthAtLeast(tag string, expected, actual int) error {
	return fmt.Errorf("invalid %s length, expected at least %d bytes, got %d", tag, expected, actual)
}

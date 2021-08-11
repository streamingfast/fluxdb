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
	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

func NewPreprocessBlock(mapper BlockMapper) bstream.PreprocessFunc {
	return func(rawBlk *bstream.Block) (interface{}, error) {
		if rawBlk.Num()%600 == 0 {
			zlog.Info("pre-processing block (printed each 600 blocks)", zap.Stringer("block", rawBlk))
		}

		return mapper.Map(rawBlk)
	}
}

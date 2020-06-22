package fluxdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/dfuse-io/logging"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	RegisterTabletFactory("tt", func(row *pbfluxdb.Row) Tablet {
		return newTestTablet(row.TabletKey)
	})

	RegisterSingletFactory("st", func(row *pbfluxdb.Row) Singlet {
		return newTestSinglet(row.TabletKey)
	})

	if os.Getenv("DEBUG") != "" {
		logger, _ := zap.NewDevelopment()
		logging.Override(logger)
	}
}

type testTablet string

func newTestTablet(tableName string) testTablet {
	return testTablet("tt/" + tableName)
}

func (t testTablet) NewRowFromKV(key string, value []byte) (TabletRow, error) {
	base, err := NewBaseTabletRowFromKV(key, value)
	if err != nil {
		return nil, err
	}

	return &testRow{BaseTabletRow: base}, nil
}

func (t testTablet) Key() string {
	return string(t)
}

func (t testTablet) KeyAt(height uint64) string {
	return string(t) + "/" + HexHeight(height)
}

func (t testTablet) KeyForRowAt(height uint64, primaryKey string) string {
	return t.KeyAt(height) + "/" + primaryKey
}

func (t testTablet) PrimaryKeyByteCount() int {
	return 8
}

func (t testTablet) EncodePrimaryKey(buffer []byte, primaryKey string) error {
	value, err := strconv.ParseUint(primaryKey, 16, 64)
	if err != nil {
		return fmt.Errorf("invalid primary key, not a valid 16 bytes hexadecimal value: %w", err)
	}

	binary.BigEndian.PutUint64(buffer, value)
	return nil
}

func (t testTablet) DecodePrimaryKey(buffer []byte) (primaryKey string, err error) {
	value := binary.BigEndian.Uint64(buffer)
	return fmt.Sprintf("%016x", value), nil
}

func (t testTablet) String() string {
	return string(t)
}

func (t testTablet) newRow(tt *testing.T, height uint64, primaryKey string, data []byte, isDeletion bool) *testRow {
	var payload []byte
	if !isDeletion {
		payload = data
	}

	out, err := NewBaseTabletRow(string(t), height, primaryKey, payload)
	require.NoError(tt, err)

	return &testRow{BaseTabletRow: out}
}

type testRow struct {
	BaseTabletRow
}

type testSinglet string

func newTestSinglet(elementName string) testSinglet {
	return testSinglet("st/" + elementName)
}

func (t testSinglet) NewEntryFromKV(key string, value []byte) (SingletEntry, error) {
	base, err := NewBaseSingleEntryFromKV(key, value)
	if err != nil {
		return nil, err
	}

	return &testEntry{BaseSingletEntry: base}, nil
}

func (t testSinglet) Key() string {
	return string(t)
}

func (t testSinglet) KeyAt(height uint64) string {
	return string(t) + "/" + HexRevHeight(height)
}

func (t testSinglet) KeyForRowAt(height uint64, primaryKey string) string {
	return t.KeyAt(height) + "/" + primaryKey
}

func (t testSinglet) String() string {
	return string(t)
}

func (t testSinglet) newEntry(tt *testing.T, height uint64, data []byte, isDeletion bool) *testEntry {
	var payload []byte
	if !isDeletion {
		payload = data
	}

	out, err := NewBaseSingletEntry(string(t), height, payload)
	require.NoError(tt, err)

	return &testEntry{BaseSingletEntry: out}
}

type testEntry struct {
	BaseSingletEntry
}

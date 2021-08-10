package fluxdb

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var shardsStore = os.Getenv("FLUXDB_SHARDING_STORE_PATH")
var shardsScratchDirectory = os.Getenv("FLUXDB_SHARDING_SCRATCH_DIR")

func TestSharding_InMemory(t *testing.T) {
	runTests(t, "")
}

func TestSharding_ScratchDirectory(t *testing.T) {
	dir, cleanup := createTempDir(t, shardsScratchDirectory)
	defer cleanup()

	runTests(t, dir)
}

func runTests(t *testing.T, scratchDirectory string) {
	ctx := context.Background()

	storeDir, cleanup := createTempDir(t, shardsStore)
	defer cleanup()

	storeURL, err := url.Parse(storeDir)
	require.NoError(t, err, "Invalid storeURL", storeDir)

	shardsStore, err := dstore.NewLocalStore(storeURL, "", "", true)
	require.NoError(t, err)

	shardCount := 2
	sharder, err := NewSharder(shardsStore, scratchDirectory, shardCount, 1, 3)
	require.NoError(t, err)

	tablet1 := newTestTablet("tb1")
	tablet2 := newTestTablet("tb2")

	singlet1 := newTestSinglet("sg1")
	singlet2 := newTestSinglet("sg2")

	streamBlock(t, sharder, "00000001aa", "", writeRequest(
		[]SingletEntry{singlet1.entry(t, 1, "s1 e #1")},
		[]TabletRow{tablet1.row(t, 1, "001", "t1 r1 #1"), tablet1.row(t, 1, "002", "t1 r2 #1")}),
	)

	streamBlock(t, sharder, "00000002aa", "", writeRequest(
		[]SingletEntry{singlet2.entry(t, 2, "s2 e #2")},
		[]TabletRow{tablet2.row(t, 2, "001", "t2 r1 #2"), tablet2.row(t, 2, "002", "t2 r2 #2")}),
	)

	streamBlock(t, sharder, "00000003aa", "", writeRequest(
		[]SingletEntry{singlet1.entry(t, 3, "s1 e #3"), singlet2.entry(t, 3, "s2 e #3")},
		[]TabletRow{
			tablet1.row(t, 3, "002", "t1 r2 #3"),

			tablet2.row(t, 3, "001", "t2 r1 #3"),
		}),
	)

	endBlock(t, sharder, "00000004aa")

	db, closer := NewTestDB(t)
	defer closer()

	db.shardCount = shardCount

	// Injection of each shard is done individually, each store pointing into the shard directory directly
	var verifyErrors []error
	var lastShardProgressStats *shardProgressStats
	wroteFinalCheckpoint := false
	for i := 0; i < shardCount; i++ {
		db.shardIndex = i

		storeURL, err := url.Parse(path.Join(storeDir, fmt.Sprintf("%03d", i)))
		require.NoError(t, err, "Invalid storeURL")

		specificShardStore, err := dstore.NewLocalStore(storeURL, "", "", false)
		require.NoError(t, err)

		injector := NewShardInjector(specificShardStore, db)
		err = injector.Run()
		require.NoError(t, err, "Unable to reinject all shards correctly for shard index %03d", i)

		stats, err := db.VerifyAllShardsWritten(ctx)
		require.Equal(t, uint64(3), stats.HighestHeight)
		require.Equal(t, uint64(3), stats.ReferenceBlockRef.Num())
		require.Equal(t, "00000003aa", stats.ReferenceBlockRef.ID())
		lastShardProgressStats = stats

		if err == nil {
			zlog.Info("all shards done injecting, setting checkpoint to last block", zap.Stringer("last_block", stats.ReferenceBlockRef))
			err = db.WriteShardingFinalCheckpoint(ctx, stats.HighestHeight, stats.ReferenceBlockRef)
			require.NoError(t, err)

			wroteFinalCheckpoint = true
		} else {
			verifyErrors = append(verifyErrors, err)
		}
	}

	if !wroteFinalCheckpoint && lastShardProgressStats != nil {
		for shardIndex, shardBlock := range lastShardProgressStats.BlockRefByShard {
			zlog.Debug("latest shard block", zap.Int("shard_index", shardIndex), zap.Stringer("last_block", shardBlock))
		}
	}

	require.True(t, wroteFinalCheckpoint, "Should have written final checkpoint but did not: (count: %d, errors: %s)", len(verifyErrors), strings.Join(errorsToStrings(verifyErrors), ", "))

	// Act like a standard (non-sharding) instance from this point
	db.shardCount = 0
	db.shardIndex = 0

	height, blockRef, err := db.FetchLastWrittenCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), height)
	require.Equal(t, uint64(3), blockRef.Num())
	require.Equal(t, "00000003aa", blockRef.ID())

	singlet1Entry, err := db.ReadSingletEntryAt(ctx, singlet1, 3, nil)
	assert.Equal(t, singlet1.entry(t, 3, "s1 e #3"), singlet1Entry)

	singlet2Entry, err := db.ReadSingletEntryAt(ctx, singlet2, 3, nil)
	assert.Equal(t, singlet2.entry(t, 3, "s2 e #3"), singlet2Entry)

	tablet1Rows, err := db.ReadTabletAt(ctx, 3, tablet1, nil)
	assert.Equal(t, []TabletRow{tablet1.row(t, 1, "001", "t1 r1 #1"), tablet1.row(t, 3, "002", "t1 r2 #3")}, tablet1Rows)

	tablet2Rows, err := db.ReadTabletAt(ctx, 3, tablet2, nil)
	assert.Equal(t, []TabletRow{tablet2.row(t, 3, "001", "t2 r1 #3"), tablet2.row(t, 2, "002", "t2 r2 #2")}, tablet2Rows)
}

func errorsToStrings(errs []error) (out []string) {
	out = make([]string, len(errs))
	for i, err := range errs {
		out[i] = err.Error()
	}

	return out
}

func streamBlock(t *testing.T, sharder *Sharder, id, libID string, request *WriteRequest) {
	blk := bblock(id, libID)
	request.Height = blk.Num()
	request.BlockRef = blk.AsRef()

	err := sharder.ProcessBlock(blk, fObj(request))
	require.NoError(t, err)
}

func endBlock(t *testing.T, sharder *Sharder, id string) {
	blk := bblock(id, "")
	req := &WriteRequest{
		Height:   blk.Num(),
		BlockRef: blk.AsRef(),
	}

	err := sharder.ProcessBlock(blk, fObj(req))
	require.Equal(t, ErrCleanSourceStop, err)
}

func writeRequest(entries []SingletEntry, rows []TabletRow) *WriteRequest {
	return &WriteRequest{
		SingletEntries: entries,
		TabletRows:     rows,
	}
}

func bblock(id, libID string) *bstream.Block {
	ref := bstream.NewBlockRefFromID(id)
	fork := id[8:]

	libNum := ref.Num() - 1
	if libID != "" {
		libNum = bstream.NewBlockRefFromID(id).Num()
	}

	return &bstream.Block{
		Id:         ref.ID(),
		Number:     ref.Num(),
		LibNum:     libNum,
		PreviousId: fmt.Sprintf("%08x%s", uint32(ref.Num()-1), fork),
		Timestamp:  time.Now(),
	}
}

func fObj(request *WriteRequest) *forkable.ForkableObject {
	return &forkable.ForkableObject{Step: forkable.StepIrreversible, Obj: request}
}

func createTempDir(t *testing.T, input string) (string, func()) {
	if input == "" {
		dir, err := ioutil.TempDir("", "fluxdb-sharding-tests-store")
		require.NoError(t, err)

		return dir, func() {
			os.RemoveAll(dir)
		}
	}

	// If you provided a valid input, we delete after first, so it's possible to
	// inspect the final generated files after the tests have run.
	os.RemoveAll(input)
	return input, func() {}
}

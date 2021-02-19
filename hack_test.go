package fluxdb

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHackExcludeRange(t *testing.T) {
	tests := []struct {
		name             string
		collection       uint16
		identifier       string
		expectedExcluded bool
	}{
		{"collection before start range", 0xA000, "5530ea033482a60032114d4f380000003d3a7a74f4e8c410", false},
		{"included just before start", 0xB000, "5530ea033482a60032114d4f380000003d3a7a74f4e8c409", false},
		{"excluded exact start", 0xB000, "5530ea033482a60032114d4f380000003d3a7a74f4e8c410", true},
		{"excluded exact end", 0xB000, "5530ea033482a60032114d4f380000003d3d356d5784e9c0", true},
		{"included just after exact end", 0xB000, "5530ea033482a60032114d4f380000003d3d356d5784e9c1", false},
		{"collection after end range", 0xC000, "5530ea033482a60032114d4f380000003d3d356d5784e9c0", false},

		{"random other 1", 0xB000, "5530ea033482a60042114d4f380000003d3d356d5784e9c0", false},
		{"random other 2", 0xB000, "5530ea033482a60022114d4f380000003d3d356d5784e9c0", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tablet := &hackTablet{test.collection, test.identifier}

			isExcluded := hackIsRestrictedTablet(tablet)
			if test.expectedExcluded {
				assert.True(t, isExcluded, "Should have been excluded but it was included (%s)", tablet)
			} else {
				assert.False(t, isExcluded, "Should have been included but it was excluded (%s)", tablet)
			}
		})
	}
}

type hackTablet struct {
	collection uint16
	identifier string
}

func (t *hackTablet) Collection() uint16 {
	return t.collection
}

func (t *hackTablet) Identifier() []byte {
	out, _ := hex.DecodeString(t.identifier)
	return out
}

func (t *hackTablet) Row(height uint64, primaryKey []byte, value []byte) (TabletRow, error) {
	return nil, nil
}

func (t *hackTablet) String() string {
	return fmt.Sprintf("%x%s", t.collection, t.identifier)
}

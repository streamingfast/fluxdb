package fluxdb

import (
	"bytes"
	"encoding/hex"
	"errors"
)

var ErrTemporarlyExcludedRange = errors.New("excluded range")

var hackNationExcludeTabletKeyStart, _ = hex.DecodeString("B0005530EA033482A60032114D4F380000003D3A7A74F4E8C410")
var hackNationExcludeTabletKeyEnd, _ = hex.DecodeString("B0005530EA033482A60032114D4F380000003D3D356D5784E9C0")

func hackIsRestrictedTablet(tablet Tablet) bool {
	tabletKey := KeyForTablet(tablet)
	// If tablet >= excludedStart && tablet <= excludedEnd
	if bytes.Compare(tabletKey, hackNationExcludeTabletKeyStart) >= 0 && bytes.Compare(tabletKey, hackNationExcludeTabletKeyEnd) <= 0 {
		return true
	}

	return false
}

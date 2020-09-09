package fluxdb

import (
	"encoding/hex"

	"github.com/dfuse-io/logging"
	"github.com/stretchr/testify/assert"
)

var noError = ""

func init() {
	logging.TestingOverride()
}

func B(data string) []byte {
	out, err := hex.DecodeString(data)
	if err != nil {
		panic(err)
	}

	return out
}

func didPanic(f assert.PanicTestFunc) (bool, interface{}) {
	didPanic := false
	var message interface{}
	func() {
		defer func() {
			if message = recover(); message != nil {
				didPanic = true
			}
		}()

		f()
	}()

	return didPanic, message
}

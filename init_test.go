package fluxdb

import (
	"encoding/hex"
	"os"

	"github.com/dfuse-io/logging"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var noError = ""

func init() {
	if os.Getenv("DEBUG") != "" {
		logger, _ := zap.NewDevelopment()
		logging.Override(logger)
	}
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

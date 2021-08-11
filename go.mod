module github.com/streamingfast/fluxdb

require (
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/abourget/llerrgroup v0.2.0
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dfuse-io/bstream v0.0.2-0.20210811160908-fc6cb0861d48
	github.com/dfuse-io/dgrpc v0.0.0-20210810125735-867cc255530c // indirect
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/golang/protobuf v1.4.2
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/minio/highwayhash v1.0.0
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/prometheus/client_model v0.1.0 // indirect
	github.com/streamingfast/blockmeta v0.0.2-0.20210810190006-1daeff8bc987 // indirect
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20210810022442-32249850a4fb
	github.com/streamingfast/dgrpc v0.0.0-20210811160823-a39dfa7fff0d // indirect
	github.com/streamingfast/dmetrics v0.0.0-20210810172555-564d623b116f
	github.com/streamingfast/dstore v0.1.1-0.20210810110932-928f221474e4
	github.com/streamingfast/dtracing v0.0.0-20210810040633-7c6259bea4a7
	github.com/streamingfast/jsonpb v0.0.0-20210811021341-3670f0aa02d0 // indirect
	github.com/streamingfast/kvdb v0.0.2-0.20210809203849-c1762028eb64
	github.com/streamingfast/pbgo v0.0.6-0.20210811160400-7c146c2db8cc // indirect
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.22.3
	go.uber.org/zap v1.15.0
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

go 1.13

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8

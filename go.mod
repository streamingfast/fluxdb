module github.com/streamingfast/fluxdb

require (
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/abourget/llerrgroup v0.2.0
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dfuse-io/bstream v0.0.2-0.20200831174806-3a465dd49cb4
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb
	github.com/dfuse-io/dmetrics v0.0.0-20200508170817-3b8cb01fee68
	github.com/dfuse-io/dstore v0.1.1-0.20200819043022-7cdd92b6d8a9
	github.com/dfuse-io/jsonpb v0.0.0-20200602171045-28535c4016a2
	github.com/dfuse-io/logging v0.0.0-20201125153217-f29c382faa42
	github.com/dfuse-io/pbgo v0.0.6-0.20200819050623-1bfd94a6868d
	github.com/dfuse-io/shutter v1.4.1-0.20200407040739-f908f9ab727f
	github.com/golang/protobuf v1.4.2
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/minio/highwayhash v1.0.0
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/prometheus/client_model v0.1.0 // indirect
	github.com/streamingfast/derr v0.0.0-20210810022442-32249850a4fb
	github.com/streamingfast/dtracing v0.0.0-20210810040633-7c6259bea4a7
	github.com/streamingfast/kvdb v0.0.2-0.20210809203849-c1762028eb64
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.3
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.15.0
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

go 1.13

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8

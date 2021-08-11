module github.com/streamingfast/fluxdb

require (
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/abourget/llerrgroup v0.2.0
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dfuse-io/bstream v0.0.2-0.20210810184055-243c376da8d5 // indirect
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb // indirect
	github.com/dfuse-io/dgrpc v0.0.0-20210810125735-867cc255530c // indirect
	github.com/dfuse-io/dhammer v0.0.0-20201127174908-667b90585063 // indirect
	github.com/dfuse-io/dstore v0.1.0 // indirect
	github.com/dfuse-io/jsonpb v0.0.0-20200406211248-c5cf83f0e0c0 // indirect
	github.com/dfuse-io/opaque v0.0.0-20210108174126-bc02ec905d48 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/minio/highwayhash v1.0.0
	github.com/rs/xid v1.2.1 // indirect
	github.com/streamingfast/blockmeta v0.0.2-0.20210811194956-90dc4202afda // indirect
	github.com/streamingfast/bstream v0.0.2-0.20210811181043-4c1920a7e3e3 // indirect
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20210811180100-9138d738bcec
	github.com/streamingfast/dgrpc v0.0.0-20210811180351-8646818518b2 // indirect
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20210811180812-4db13e99cc22
	github.com/streamingfast/dtracing v0.0.0-20210811175635-d55665d3622a
	github.com/streamingfast/jsonpb v0.0.0-20210811021341-3670f0aa02d0 // indirect
	github.com/streamingfast/kvdb v0.0.2-0.20210811194032-09bf862bd2e3
	github.com/streamingfast/logging v0.0.0-20210811175431-f3b44b61606a // indirect
	github.com/streamingfast/pbgo v0.0.6-0.20210811160400-7c146c2db8cc // indirect
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/gjson v1.5.0 // indirect
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.15.0
)

go 1.13

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8

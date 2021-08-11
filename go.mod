module github.com/streamingfast/fluxdb

require (
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/abourget/llerrgroup v0.2.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/minio/highwayhash v1.0.0
	github.com/streamingfast/bstream v0.0.2-0.20210811181043-4c1920a7e3e3
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20210811180100-9138d738bcec
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20210811180812-4db13e99cc22
	github.com/streamingfast/dtracing v0.0.0-20210811175635-d55665d3622a
	github.com/streamingfast/jsonpb v0.0.0-20210811021341-3670f0aa02d0
	github.com/streamingfast/kvdb v0.0.2-0.20210811194032-09bf862bd2e3
	github.com/streamingfast/logging v0.0.0-20210811175431-f3b44b61606a
	github.com/streamingfast/pbgo v0.0.6-0.20210811160400-7c146c2db8cc
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.15.0
)

go 1.13

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8

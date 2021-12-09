module github.com/streamingfast/fluxdb

require (
	contrib.go.opencensus.io/exporter/stackdriver v0.13.8 // indirect
	github.com/Azure/go-autorest/autorest v0.9.0 // indirect
	github.com/abourget/llerrgroup v0.2.0
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/minio/highwayhash v1.0.0
	github.com/streamingfast/atm v0.0.0-20211005171144-f2f3bbfda2f6 // indirect
	github.com/streamingfast/bstream v0.0.2-0.20211209202442-73053c07cf54 // indirect
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20210811180100-9138d738bcec
	github.com/streamingfast/dgrpc v0.0.0-20210901144702-c57c3701768b // indirect
	github.com/streamingfast/dhammer v0.0.0-20210811180702-456c4cf0a840 // indirect
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20211012134319-16e840827e38
	github.com/streamingfast/dtracing v0.0.0-20210811175635-d55665d3622a
	github.com/streamingfast/jsonpb v0.0.0-20210811021341-3670f0aa02d0
	github.com/streamingfast/kvdb v0.0.2-0.20210811194032-09bf862bd2e3
	github.com/streamingfast/logging v0.0.0-20210908162127-bdc5856d5341
	github.com/streamingfast/opaque v0.0.0-20210811180740-0c01d37ea308 // indirect
	github.com/streamingfast/pbgo v0.0.6-0.20211209181937-06ef048bad0c
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.19.1
	google.golang.org/api v0.53.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)

go 1.13

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8

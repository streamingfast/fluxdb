module github.com/dfuse-io/fluxdb

require (
	cloud.google.com/go/bigtable v1.2.0
	contrib.go.opencensus.io/exporter/stackdriver v0.12.6
	github.com/abourget/llerrgroup v0.2.0
	github.com/abourget/viperbind v0.1.0
	github.com/coreos/bbolt v1.3.2
	github.com/dfuse-io/bstream v0.0.0-20200602201235-217b145d1844
	github.com/dfuse-io/derr v0.0.0-20200417132224-d333cfd0e9a0
	github.com/dfuse-io/dfuse-eosio v0.1.1-docker
	github.com/dfuse-io/dmetrics v0.0.0-20200508170817-3b8cb01fee68
	github.com/dfuse-io/dstore v0.1.0
	github.com/dfuse-io/dtracing v0.0.0-20200417133307-c09302668d0c
	github.com/dfuse-io/jsonpb v0.0.0-20200612204658-6541604ace82
	github.com/dfuse-io/kvdb v0.0.0-20200619171121-937464bfbae5
	github.com/dfuse-io/logging v0.0.0-20200417143534-5e26069a5e39
	github.com/dfuse-io/pbgo v0.0.6-0.20200619193216-9bbf0c9fb1f8
	github.com/dfuse-io/shutter v1.4.1-0.20200407040739-f908f9ab727f
	github.com/eoscanada/bstream v1.7.1-0.20200326121139-a9fcd944fb9e
	github.com/eoscanada/derr v1.0.2-0.20200322005543-aaeeedb97c88
	github.com/eoscanada/dmetrics v0.1.6-0.20200322010505-8c9d46bde785
	github.com/eoscanada/dstore v1.0.4-0.20200326124223-48347227b3f4
	github.com/eoscanada/dtracing v1.0.1-0.20200322010529-5f2be5469d38
	github.com/eoscanada/eos-go v0.9.1-0.20200507122837-271bfac45a78
	github.com/eoscanada/jsonpb v0.0.0-20191003191457-98439e8ce04b
	github.com/eoscanada/kvdb v0.0.12-0.20200402172913-4932bdf8c65e
	github.com/eoscanada/logging v1.1.2-0.20200322010944-42e4cc316510
	github.com/eoscanada/validator v0.4.1-0.20190329230755-16888100b0ba
	github.com/francoispqt/gojay v1.2.13
	github.com/gavv/httpexpect/v2 v2.0.3
	github.com/golang/protobuf v1.4.2
	github.com/gorilla/mux v1.7.3
	github.com/hidal-go/hidalgo v0.0.0-20190814174001-42e03f3b5eaa
	github.com/minio/highwayhash v1.0.0
	github.com/spf13/cobra v0.0.7
	github.com/spf13/viper v1.6.2
	github.com/stretchr/testify v1.4.0
	github.com/thedevsaddam/govalidator v1.9.9
	go.opencensus.io v0.22.3
	go.uber.org/zap v1.15.0
	google.golang.org/api v0.15.0
	google.golang.org/grpc v1.26.0
	gotest.tools v2.2.0+incompatible
)

go 1.13

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8
replace github.com/dfuse-io/pbgo => /Users/maoueh/work/dfuse/pbgo

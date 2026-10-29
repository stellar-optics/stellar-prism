module github.com/stellar-optics/stellar-prism

go 1.26.0

require (
	cloud.google.com/go/pubsub/v2 v2.6.0
	github.com/aws/aws-sdk-go-v2 v1.47.3
	github.com/aws/aws-sdk-go-v2/config v1.33.9
	github.com/aws/aws-sdk-go-v2/service/sns v1.47.5
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.5
	github.com/fsnotify/fsnotify v1.10.1
	github.com/go-zeromq/zmq4 v0.17.0
	github.com/gorilla/websocket v1.5.3
	github.com/jackc/pgx/v5 v5.11.0
	github.com/marcboeker/go-duckdb/v2 v2.4.3
	github.com/nats-io/nats.go v1.47.0
	github.com/parquet-go/parquet-go v0.32.0
	github.com/rabbitmq/amqp091-go v1.16.0
	github.com/segmentio/kafka-go v0.4.51
	github.com/spf13/cobra v1.10.2
	github.com/spf13/viper v1.20.1
	github.com/stellar-optics/stellar-xdr-lens v0.2.0
	github.com/stellar/go-stellar-sdk v0.7.3
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.11.0 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/apache/arrow-go/v18 v18.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.9 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.4 // indirect
	github.com/aws/smithy-go v1.28.5 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.6 // indirect
	github.com/creachadair/jrpc2 v1.2.0 // indirect
	github.com/creachadair/mds v0.13.4 // indirect
	github.com/duckdb/duckdb-go-bindings v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/darwin-amd64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/darwin-arm64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/linux-amd64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/linux-arm64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/windows-amd64 v0.1.21 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/go-zeromq/goczmq/v4 v4.2.2 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.17 // indirect
	github.com/googleapis/gax-go/v2 v2.23.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/marcboeker/go-duckdb/arrowmapping v0.0.21 // indirect
	github.com/marcboeker/go-duckdb/mapping v0.0.21 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sagikazarmark/locafero v0.7.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.12.0 // indirect
	github.com/spf13/cast v1.7.1 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/stellar/go-xdr v0.0.0-20260806060815-dc590f17552a // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/telemetry v0.0.0-20260924152758-ed294f943157 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/api v0.287.1 // indirect
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

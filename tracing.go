package fluxdb

import "go.opentelemetry.io/otel"

var ttracer = otel.Tracer("fluxdb")

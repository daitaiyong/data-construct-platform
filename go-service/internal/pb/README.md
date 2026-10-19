# Template engine gRPC contract

The source of truth is `../../../python-engine/proto/template_engine.proto`.
The generated Go protobuf messages and gRPC client/server stubs in this
directory are produced with `protoc`; regenerate them using the command in the
repository README when the contract changes. The Go worker calls
`TemplateEngineService.GenerateData`, and the client also exposes
`ValidateTemplate`.

from concurrent import futures
import os

import grpc
import template_engine_pb2
import template_engine_pb2_grpc

from generator import generate


class TemplateEngine(template_engine_pb2_grpc.TemplateEngineServicer):
    def Render(self, request, context):
        try:
            records = generate(request.template, request.variables, request.record_count)
        except (ValueError, KeyError) as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        return template_engine_pb2.RenderResponse(records=records)


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    template_engine_pb2_grpc.add_TemplateEngineServicer_to_server(TemplateEngine(), server)
    address = os.environ.get("GRPC_ADDR", "[::]:50051")
    if server.add_insecure_port(address) == 0:
        raise RuntimeError(f"unable to bind gRPC server to {address}")
    server.start()
    print(f"template engine listening on {address}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

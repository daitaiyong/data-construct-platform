from concurrent import futures
import json
import os

import grpc
import template_engine_pb2
import template_engine_pb2_grpc

from generator import MAX_RECORDS_PER_REQUEST, generate, validate_template


class TemplateEngine(template_engine_pb2_grpc.TemplateEngineServiceServicer):
    def GenerateData(self, request, context):
        try:
            template = json.loads(request.template_json)
            context_data = json.loads(request.context_json or "{}")
            if not isinstance(template, dict) or not isinstance(context_data, dict):
                raise ValueError("template and context must be JSON objects")
            if request.count < 0 or request.count > MAX_RECORDS_PER_REQUEST:
                raise ValueError(f"count must be between 0 and {MAX_RECORDS_PER_REQUEST}")
            rows = generate(template, context_data, request.count)
            return template_engine_pb2.GenerateResponse(
                success=True,
                rows_json=[json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows],
                total=len(rows),
            )
        except (ValueError, TypeError, KeyError) as error:
            return template_engine_pb2.GenerateResponse(success=False, error_msg=str(error))

    def ValidateTemplate(self, request, context):
        try:
            template = json.loads(request.template_json)
            if not isinstance(template, dict):
                raise ValueError("template must be a JSON object")
            errors = validate_template(template)
        except (ValueError, TypeError) as error:
            errors = [str(error)]
        return template_engine_pb2.ValidateResponse(ok=not errors, errors=errors)


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    template_engine_pb2_grpc.add_TemplateEngineServiceServicer_to_server(TemplateEngine(), server)
    address = os.environ.get("GRPC_ADDR", "[::]:50051")
    if server.add_insecure_port(address) == 0:
        raise RuntimeError(f"unable to bind gRPC server to {address}")
    server.start()
    print(f"template engine listening on {address}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

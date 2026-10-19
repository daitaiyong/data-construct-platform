import importlib
import json
import sys
import unittest
from concurrent import futures
from pathlib import Path

import grpc
from grpc_tools import protoc

service_dir = Path(__file__).resolve().parent
proto_dir = service_dir.parent / "proto"
if not (service_dir / "template_engine_pb2.py").exists():
    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{proto_dir}",
            f"--python_out={service_dir}",
            f"--grpc_python_out={service_dir}",
            str(proto_dir / "template_engine.proto"),
        ]
    )
    if result != 0:
        raise RuntimeError(f"failed to generate test gRPC stubs: protoc exited {result}")
sys.path.insert(0, str(service_dir))

template_engine_pb2 = importlib.import_module("template_engine_pb2")
template_engine_pb2_grpc = importlib.import_module("template_engine_pb2_grpc")
TemplateEngine = importlib.import_module("server").TemplateEngine


class TemplateEngineGRPCTests(unittest.TestCase):
    def test_generate_data_generates_rows(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        template_engine_pb2_grpc.add_TemplateEngineServiceServicer_to_server(TemplateEngine(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                response = template_engine_pb2_grpc.TemplateEngineServiceStub(channel).GenerateData(
                    template_engine_pb2.GenerateRequest(
                        template_json=json.dumps(
                            {
                                "fields": [
                                    {"name": "id", "type": "snowflake", "rule": "snowflake()"},
                                    {"name": "status", "type": "int", "rule": "enum(0,1)"},
                                ]
                            }
                        ),
                        context_json="{}",
                        count=2,
                    ),
                    timeout=5,
                )
            rows = [json.loads(row) for row in response.rows_json]
            self.assertTrue(response.success)
            self.assertEqual(response.total, 2)
            self.assertEqual(len(rows), 2)
            self.assertIsInstance(rows[0]["id"], str)
            self.assertIn(rows[0]["status"], (0, 1))
        finally:
            server.stop(0).wait()

    def test_generate_data_reports_generation_errors(self):
        engine = TemplateEngine()
        response = engine.GenerateData(
            template_engine_pb2.GenerateRequest(
                template_json='{"fields":[{"name":"value","type":"string","rule":"invalid()"}]}',
                context_json="{}",
                count=1,
            ),
            None,
        )
        self.assertFalse(response.success)
        self.assertIn("unsupported rule", response.error_msg)
        self.assertEqual(response.total, 0)

    def test_validate_template_checks_rules(self):
        engine = TemplateEngine()
        valid = engine.ValidateTemplate(
            template_engine_pb2.ValidateRequest(
                template_json='{"fields":[{"name":"status","type":"int","rule":"enum(0,1)"}]}'
            ),
            None,
        )
        invalid = engine.ValidateTemplate(
            template_engine_pb2.ValidateRequest(
                template_json='{"fields":[{"name":"status","type":"int","rule":"enum(active)"}]}'
            ),
            None,
        )
        self.assertTrue(valid.ok)
        self.assertFalse(invalid.ok)
        self.assertTrue(invalid.errors)


if __name__ == "__main__":
    unittest.main()

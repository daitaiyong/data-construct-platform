import unittest

from generator import generate


class GeneratorTests(unittest.TestCase):
    def test_generates_rule_values_and_snowflake_ids_as_strings(self):
        template = {
            "fields": [
                {"name": "id", "type": "snowflake", "rule": "snowflake()", "nullable": False},
                {"name": "status", "type": "int", "rule": "enum(0,1)", "nullable": False},
                {"name": "name", "type": "string", "rule": "name()", "nullable": True},
            ]
        }
        rows = generate(template, {}, 2)
        self.assertEqual(len(rows), 2)
        self.assertIsInstance(rows[0]["id"], str)
        self.assertIn(rows[0]["status"], (0, 1))
        self.assertTrue(rows[0]["name"])

    def test_rejects_unsupported_rule(self):
        with self.assertRaisesRegex(ValueError, "unsupported rule"):
            generate({"fields": [{"name": "value", "type": "string", "rule": "eval()"}]}, {}, 1)

    def test_rejects_duplicate_field_names(self):
        with self.assertRaisesRegex(ValueError, "duplicate field"):
            generate(
                {"fields": [{"name": "value", "type": "string"}, {"name": "value", "type": "string"}]},
                {},
                1,
            )


if __name__ == "__main__":
    unittest.main()

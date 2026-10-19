import unittest

from parser import render


class RenderTests(unittest.TestCase):
    def test_renders_named_variables(self):
        self.assertEqual(render("hello {name}", {"name": "world"}), "hello world")

    def test_rejects_missing_variables(self):
        with self.assertRaisesRegex(ValueError, "missing template variables"):
            render("hello {name}", {})

    def test_rejects_attribute_traversal(self):
        with self.assertRaisesRegex(ValueError, "simple variable placeholders"):
            render("{name.__class__}", {"name": "world"})


if __name__ == "__main__":
    unittest.main()

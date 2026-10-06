import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import licenses


class LicensePolicyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "licenses").mkdir()
        (self.root / "LICENSE").write_text("project license\n")
        (self.root / "NOTICE").write_text("project attribution\n")
        self.dependency = self.root / "dependency"
        self.dependency.mkdir()
        (self.dependency / "LICENSE").write_text("upstream license\n")
        self.modules = [{"Path": "example.org/project", "Main": True},
                        {"Path": "example.org/dependency", "Version": "v1.0.0"}]
        self.module_file = {"Go": "1.27.1"}
        self.policy = {
            "go_version": "1.27.1",
            "project_license_sha256": hashlib.sha256((self.root / "LICENSE").read_bytes()).hexdigest(),
            "modules": {"example.org/dependency": {
                "version": "v1.0.0", "license": "MIT", "source": "https://example.org/dependency",
                "files": {"LICENSE": hashlib.sha256((self.dependency / "LICENSE").read_bytes()).hexdigest()},
            }},
            "supplemental": [],
        }
        (self.root / "licenses/policy.json").write_text(json.dumps(self.policy))
        self.root_patch = patch.object(licenses, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.go_patch = patch.object(licenses, "go", side_effect=self.go)
        self.go_patch.start()
        self.addCleanup(self.go_patch.stop)

    def go(self, *args):
        if args == ("mod", "edit", "-json"):
            return json.dumps(self.module_file)
        if args == ("env", "GOVERSION"):
            return "go1.27.1\n"
        if args == ("list", "-m", "-json", "all"):
            return "\n".join(json.dumps(m) for m in self.modules)
        if args == ("mod", "download", "-json", "example.org/dependency@v1.0.0"):
            return json.dumps({"Dir": str(self.dependency)})
        if args == ("mod", "verify"):
            return "all modules verified\n"
        raise AssertionError(args)

    def test_report_retains_upstream_text_and_rejects_stale_output(self):
        licenses.check(write=True)
        licenses.check()
        report = self.root / "licenses/THIRD_PARTY_NOTICES.txt"
        self.assertIn("upstream license\n", report.read_text())
        report.write_text("outdated report\n")
        with self.assertRaisesRegex(ValueError, "stale"):
            licenses.check()

    def test_unknown_dependency_cannot_be_approved_by_regenerating(self):
        self.modules.append({"Path": "example.org/new", "Version": "v1.0.0"})
        with self.assertRaisesRegex(ValueError, "unreviewed=.*example.org/new"):
            licenses.check(write=True)

    def test_version_changes_require_review(self):
        self.modules[1]["Version"] = "v2.0.0"
        with self.assertRaisesRegex(ValueError, "version changed"):
            licenses.check(write=True)

    def test_local_replacements_are_rejected(self):
        self.module_file["Replace"] = [{"Old": {"Path": "example.org/dependency"}}]
        with self.assertRaisesRegex(ValueError, "without replace"):
            licenses.check()

    def test_changed_upstream_license_is_rejected(self):
        (self.dependency / "LICENSE").write_text("different terms\n")
        with self.assertRaisesRegex(ValueError, "legal text changed"):
            licenses.check(write=True)

    def test_new_nested_notice_requires_review(self):
        (self.dependency / "vendor").mkdir()
        (self.dependency / "vendor/NOTICE").write_text("additional attribution\n")
        with self.assertRaisesRegex(ValueError, "inventory changed"):
            licenses.check(write=True)


if __name__ == "__main__":
    unittest.main()

import json
from pathlib import Path
import tempfile
import unittest

import npm_packages


class NpmPackageTests(unittest.TestCase):
    def test_version_tags(self):
        self.assertEqual(npm_packages.npm_version("v1.2.3"), "1.2.3")
        self.assertEqual(npm_packages.npm_version("1.2.3"), "1.2.3")
        with self.assertRaises(ValueError):
            npm_packages.npm_version("v")
        with self.assertRaises(ValueError):
            npm_packages.npm_version("v1..3")

    def test_archive_and_package_names(self):
        self.assertEqual(npm_packages.archive_name("1.2.3", "linux", "amd64"), "micro-agent_1.2.3_linux_amd64.tar.gz")
        self.assertEqual(npm_packages.archive_name("1.2.3", "windows", "arm64"), "micro-agent_1.2.3_windows_arm64.zip")
        self.assertEqual(npm_packages.platform_package("windows", "amd64"), "@brokkai/micro-agent-win32-x64")
        self.assertEqual(npm_packages.platform_package("darwin", "arm64"), "@brokkai/micro-agent-darwin-arm64")
        self.assertEqual(len(npm_packages.TARGETS), 6)

    def test_dist_tags(self):
        self.assertEqual(npm_packages.npm_tag("1.2.3"), "latest")
        self.assertEqual(npm_packages.npm_tag("1.3.0-rc.1"), "next")

    def test_pack_record_accepts_both_npm_shapes(self):
        record = {"name": "p", "version": "1.0.0", "integrity": "sha512-x", "filename": "p-1.0.0.tgz"}
        self.assertEqual(npm_packages.pack_record(json.dumps([record]), "p", "1.0.0"), record)
        self.assertEqual(npm_packages.pack_record(json.dumps({"p": record}), "p", "1.0.0"), record)
        with self.assertRaises(ValueError):
            npm_packages.pack_record(json.dumps([record, record]), "p", "1.0.0")
        with self.assertRaises(ValueError):
            npm_packages.pack_record(json.dumps([dict(record, version="2.0.0")]), "p", "1.0.0")
        with self.assertRaises(ValueError):
            npm_packages.pack_record(json.dumps([dict(record, filename="../escape.tgz")]), "p", "1.0.0")

    def test_missing_assets_fail_before_packing(self):
        with tempfile.TemporaryDirectory() as temporary:
            assets = Path(temporary)
            (assets / "checksums.txt").write_text("")
            with self.assertRaisesRegex(ValueError, "missing"):
                npm_packages.package("1.0.0", assets, assets / "out")


if __name__ == "__main__":
    unittest.main()

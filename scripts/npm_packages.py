#!/usr/bin/env python3
"""Build and publish the @brokkai/micro-agent npm packages from release assets."""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parent.parent
LAUNCHER = "@brokkai/micro-agent"
REPOSITORY = "git+https://github.com/BrokkAi/micro-agent.git"
LEGAL_FILES = ("LICENSE", "NOTICE", "licenses/THIRD_PARTY_NOTICES.txt")
CONFLICT = ("E409", "EPUBLISHCONFLICT", "cannot publish over", "previously staged version")

# GoReleaser targets mapped onto npm's platform and architecture names.
TARGETS = {
    ("linux", "amd64"): ("linux", "x64"),
    ("linux", "arm64"): ("linux", "arm64"),
    ("darwin", "amd64"): ("darwin", "x64"),
    ("darwin", "arm64"): ("darwin", "arm64"),
    ("windows", "amd64"): ("win32", "x64"),
    ("windows", "arm64"): ("win32", "arm64"),
}


def npm_version(tag):
    version = tag[1:] if tag.startswith("v") else tag
    if not version or any(part == "" for part in version.split(".")):
        raise ValueError(f"tag {tag!r} is not a version tag")
    return version


def archive_name(version, goos, goarch):
    suffix = "zip" if goos == "windows" else "tar.gz"
    return f"micro-agent_{version}_{goos}_{goarch}.{suffix}"


def platform_package(goos, goarch):
    system, arch = TARGETS[(goos, goarch)]
    return f"{LAUNCHER}-{system}-{arch}"


def verify_assets(assets, version):
    """Verify every archive against checksums.txt and return them by target."""
    checksums = {}
    for line in (assets / "checksums.txt").read_text().splitlines():
        digest, _, name = line.partition("  ")
        if digest and name:
            checksums[name] = digest
    found = {}
    for target in TARGETS:
        name = archive_name(version, *target)
        path = assets / name
        if name not in checksums or not path.is_file():
            raise ValueError(f"release asset {name} is missing")
        if hashlib.sha256(path.read_bytes()).hexdigest() != checksums[name]:
            raise ValueError(f"release asset {name} does not match checksums.txt")
        found[target] = path
    return found


def archive_binary(path, goos):
    """Extract the micro-agent executable from a release archive."""
    name = "micro-agent.exe" if goos == "windows" else "micro-agent"
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as bundle:
            return bundle.read(name)
    with tarfile.open(path, "r:gz") as bundle:
        member = bundle.extractfile(name)
        if member is None:
            raise ValueError(f"{path.name} does not contain {name}")
        return member.read()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def npm_build_environment(directory):
    # Packing must not inherit developer or CI account settings.
    env = {k: v for k, v in os.environ.items() if not k.lower().startswith("npm_config_")}
    return dict(env, npm_config_userconfig=str(directory / "user.npmrc"),
                npm_config_globalconfig=str(directory / "global.npmrc"),
                npm_config_cache=str(directory / "npm-cache"))


def pack_record(output, name, version):
    records = json.loads(output)
    # npm 12 keys pack output by package name; npm 11 returns an array.
    if isinstance(records, dict) and set(records) == {name}:
        records = [records[name]]
    if not isinstance(records, list) or len(records) != 1:
        raise ValueError("npm pack must report exactly one package")
    info = records[0]
    if (info.get("name") != name or info.get("version") != version
            or not info.get("integrity") or not info.get("filename")
            or Path(info["filename"]).name != info["filename"]):
        raise ValueError("npm pack metadata does not match the requested package")
    return info


def check_tarball(tarball):
    """Every package must carry the project and third-party license material."""
    with tarfile.open(tarball, "r:gz") as bundle:
        names = {member.name.removeprefix("package/") for member in bundle.getmembers()}
    missing = [name for name in LEGAL_FILES if name not in names]
    if missing:
        raise ValueError(f"{tarball.name} is missing {', '.join(missing)}")


def package(version, assets, out):
    """Build every npm package from verified release assets into out."""
    archives = verify_assets(assets, version)
    if out.exists() and any(out.iterdir()):
        raise ValueError(f"{out} must be empty")
    out.mkdir(parents=True, exist_ok=True)
    base = {
        "version": version,
        "license": "MIT",
        "repository": {"type": "git", "url": REPOSITORY},
        "publishConfig": {"access": "public"},
    }
    packages = []
    with tempfile.TemporaryDirectory() as temporary:
        staging = Path(temporary)

        def npm_pack(name, fields, files):
            directory = staging / name.split("/")[-1]
            directory.mkdir()
            write_json(directory / "package.json", dict(base, name=name, **fields))
            for filename, data in files.items():
                path = directory / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                path.chmod(0o755 if filename.startswith("bin/") else 0o644)
            result = subprocess.check_output([
                "npm", "pack", "--ignore-scripts", "--json", "--pack-destination", str(out.resolve()),
            ], cwd=directory, env=npm_build_environment(staging))
            info = pack_record(result, name, version)
            tarball = out / info["filename"]
            check_tarball(tarball)
            packages.append({"name": name, "version": version, "filename": tarball.name,
                             "sha256": hashlib.sha256(tarball.read_bytes()).hexdigest(),
                             "integrity": info["integrity"]})

        files = {name: (ROOT / name).read_bytes() for name in (*LEGAL_FILES, "README.md")}
        dependencies = {}
        for (goos, goarch), path in sorted(archives.items()):
            system, arch = TARGETS[(goos, goarch)]
            name = platform_package(goos, goarch)
            dependencies[name] = version
            binary = "micro-agent.exe" if goos == "windows" else "micro-agent"
            npm_pack(name, {
                "os": [system], "cpu": [arch],
                "description": f"micro-agent native binary for {system}/{arch}",
            }, {f"bin/{binary}": archive_binary(path, goos), **files})
        npm_pack(LAUNCHER, {
            "description": "Minimal Agent Client Protocol coding agent by Brokk, backed by OpenRouter",
            "homepage": "https://github.com/BrokkAi/micro-agent",
            "keywords": ["acp", "agent-client-protocol", "coding-agent", "openrouter"],
            "bin": {"micro-agent": "bin/micro-agent.cjs"},
            "engines": {"node": ">=18"},
            "os": ["linux", "darwin", "win32"], "cpu": ["x64", "arm64"],
            "optionalDependencies": dependencies,
        }, {"bin/micro-agent.cjs": (ROOT / "npm/micro-agent.cjs").read_bytes(), **files})
        write_json(out / "manifest.json", {"version": version, "packages": packages})
    print(f"Built {len(packages)} npm packages for {version}")
    return packages


def npm_tag(version):
    return "next" if "-" in version else "latest"


def submit(package_record, out, provenance):
    """Publish one package. An existing name and version is done, not an error."""
    tarball = (out / package_record["filename"]).resolve()
    command = ["npm", "publish", str(tarball), "--access", "public",
               "--registry", "https://registry.npmjs.org", "--tag", npm_tag(package_record["version"])]
    if provenance:
        command.append("--provenance")
    result = subprocess.run(command, capture_output=True, text=True)
    output = (result.stdout or "") + (result.stderr or "")
    print(f"--- {package_record['name']}\n{output}", flush=True)
    if result.returncode == 0:
        return
    if any(marker.lower() in output.lower() for marker in CONFLICT):
        print(f"{package_record['name']} {package_record['version']} is already published; skipping", flush=True)
        return
    raise subprocess.CalledProcessError(result.returncode, command, output)


def publish(out, provenance):
    manifest = json.loads((out / "manifest.json").read_text())
    launcher = [p for p in manifest["packages"] if p["name"] == LAUNCHER]
    platforms = [p for p in manifest["packages"] if p["name"] != LAUNCHER]
    if len(launcher) != 1 or len(platforms) != len(TARGETS):
        raise ValueError("manifest must contain the launcher and every platform package")
    # The launcher pins the platform packages as optional dependencies, and npm
    # treats a missing optional dependency as a successful install, so the
    # launcher goes last.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(platforms)) as pool:
        for future in [pool.submit(submit, package, out, provenance) for package in platforms]:
            future.result()
    submit(launcher[0], out, provenance)
    print("Published npm packages; registry visibility may lag behind accepted uploads")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="release version, for example 0.1.0")
    parser.add_argument("--assets", type=Path, required=True, help="directory holding the release archives and checksums.txt")
    parser.add_argument("--out", type=Path, required=True, help="directory for the packed npm tarballs")
    parser.add_argument("--publish", action="store_true", help="publish the packed tarballs to npm")
    parser.add_argument("--provenance", action="store_true", help="request a provenance attestation when publishing")
    args = parser.parse_args()
    package(args.version, args.assets, args.out)
    if args.publish:
        publish(args.out, args.provenance)


if __name__ == "__main__":
    main()

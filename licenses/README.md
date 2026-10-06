# Licensing and third-party notices

micro-agent uses the [MIT License](../LICENSE); [NOTICE](../NOTICE) identifies
the project. The license text is kept unmodified.

[THIRD_PARTY_NOTICES.txt](THIRD_PARTY_NOTICES.txt) contains the full license
texts, notices, and patent grants of everything a released binary embeds or
links: the complete selected Go module graph and the reviewed Go runtime and
Unicode data notices. MCP servers, agent CLIs, Node.js, and Python
interpreters are installed separately and are not bundled.

## Review and regeneration

Use the Go version pinned in `go.mod` and Python 3:

```sh
python3 scripts/licenses.py --write
python3 scripts/licenses.py
python3 -m unittest discover -s scripts -p '*_test.py'
```

`policy.json` is a deny-by-default inventory of the complete selected module
graph, including dependencies selected for other platforms. Every module is
reviewed at an exact version, with its license expression, source location,
and SHA-256 hashes of all license, notice, copying, and patent files. The
checker rejects additions, removals, version changes, replacements, changed
legal files, stale reports, and unreviewed Go toolchain updates. It uses Go's
module checksum verification; it does not automatically classify licenses or
approve dependency changes.

When changing dependencies, review upstream source and generated or vendored
material as well as the top-level license, update the policy deliberately,
regenerate the report, and review its diff. Review Go runtime and
standard-library notices when upgrading the pinned Go version. CI runs the
same check, and release archives carry `LICENSE`, `NOTICE`, and this
directory's notices.

## Distribution

Keep `LICENSE`, `NOTICE`, and `licenses/` with the source and in any
redistribution of compiled binaries.

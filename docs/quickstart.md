# Offline quickstart

The R8 preview supports source inspection, a synthetic loopback demonstration, and the documented core test suite. These commands do not start the production AM service or connect to a real Feishu tenant.

Verified environment: macOS arm64, Python 3.12.13. The package declares Python 3.11 or later, but this release's recorded checks used 3.12. Linux and Windows have not been validated here; some host modules use POSIX APIs.

## Install the test environment

From a fresh clone:

```bash
git clone https://github.com/Catcherog/am-project-assistant.git
cd am-project-assistant
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-test.txt
.venv/bin/python -m pip install -e runtime/am-grok-realtime
```

The dependency pins reproduce the environment used for the recorded checks. They are not a promise of compatibility with every future dependency version.

## Run a local synthetic flow

```bash
.venv/bin/python runtime/am-grok-realtime/tools/run_loopback_demo.py \
  --chat-type p2p --output outputs/loopback-p2p.json
.venv/bin/python runtime/am-grok-realtime/tools/run_loopback_demo.py \
  --chat-type group --output outputs/loopback-group.json
```

Both commands should report `PASS`, `simulation: true`, `real_grok_calls: 0`, and `real_feishu_sends: 0`. The demo uses local HTTP, temporary SQLite state and a synthetic sender. It does not validate the current assistant's full live workflow, model quality, phone display, or production response time. Reports remain in the ignored `outputs` directory.

## Run core regression checks

```bash
cd runtime/am-grok-realtime
../../.venv/bin/python -m pytest -q tests
```

The public snapshot and its private fixed baseline each passed 1807 tests. The test command is intentionally scoped to the gateway's core suite. Additional companion modules and their historical test files are included as source, but the release does not claim a new full acceptance run for every companion feature.

To inspect the installed diagnostic entry point without opening a database:

```bash
../../.venv/bin/python -m am_gateway --help
```

## Before connecting real services

The source examples use synthetic identities, placeholder resource IDs and disabled/candidate settings. Do not use their values as real credentials. Bring your own configuration through the development project's reviewed integration process. CLI login files, API keys, tokens, tenant resources, model weights, machine-local launch overlays and databases are not distributed.

See [the source-release notes](source-release.md) for known issues, and [the deployment checklist](release-checklist.md) before enabling a live consumer or external writes.

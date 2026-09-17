#!/usr/bin/env python3
"""Post-install check for a qpipe-rs wheel; run it with the interpreter the
wheel was installed into (CI runs it once per verification venv).

Checks that the installer picked the wheel this interpreter is meant to get,
that the native extension imports, that a free-threaded interpreter is still
free-threaded after the import (an extension without the free-threading
declaration silently re-enables the GIL), and that the bundled binaries landed
on the environment's scripts path as executables.

Environment:
  EXPECT_FREE_THREADED  "1"/"0": assert the interpreter is (not) a free-threaded
                        build, so a mis-resolved interpreter fails loudly instead
                        of quietly weakening the check. Unset: no assertion.
  BIN_NAMES             binaries that must be present (comma/space separated);
                        defaults to "orchestrator". Same repo variable the inject
                        and packaging scripts honor.
"""
from __future__ import annotations

import importlib.metadata as md
import os
import re
import sys
import sysconfig


def fail(msg: str) -> None:
    sys.exit(f"FAIL: {msg}")


def main() -> None:
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    expect = os.environ.get("EXPECT_FREE_THREADED")
    if expect in ("0", "1") and free_threaded != (expect == "1"):
        fail(
            f"{sys.executable} is {'a' if free_threaded else 'not a'} free-threaded "
            f"build, but EXPECT_FREE_THREADED={expect}"
        )

    import qpipe  # the import under test

    dist = md.distribution("qpipe-rs")
    tags = [
        line.split(":", 1)[1].strip()
        for line in (dist.read_text("WHEEL") or "").splitlines()
        if line.startswith("Tag:")
    ]
    abis = {t.split("-")[1] for t in tags}  # {"abi3"} or {"abi3", "abi3t"}
    print(
        f"python {sys.version.split()[0]} free-threaded={free_threaded} | "
        f"qpipe-rs {dist.version} | wheel tags: {', '.join(tags) or '(none)'}"
    )
    print(f"extension: {qpipe._qpipe.__file__}")

    if free_threaded:
        if "abi3t" not in abis:
            fail(f"a free-threaded interpreter must get the abi3t wheel, got tags {tags}")
        if sys._is_gil_enabled():
            fail("importing qpipe re-enabled the GIL: the extension lacks the free-threading declaration")
    elif sys.version_info < (3, 15):
        if abis != {"abi3"}:
            fail(f"CPython < 3.15 must get the plain cp39-abi3 wheel, got tags {tags}")
    else:
        # GIL-enabled 3.15+: PEP 803 makes the abi3t wheel loadable here too, and
        # installers rank its cp315-abi3 tag above cp39-abi3; either wheel is valid.
        if not abis & {"abi3", "abi3t"}:
            fail(f"expected a stable-ABI wheel, got tags {tags}")

    scripts = sysconfig.get_path("scripts")
    exe = ".exe" if os.name == "nt" else ""
    names = [n for n in re.split(r"[,\s]+", os.environ.get("BIN_NAMES", "")) if n] or ["orchestrator"]
    missing = []
    for name in names:
        path = os.path.join(scripts, name + exe)
        if os.path.isfile(path) and os.access(path, os.X_OK):
            print(f"binary: {path}")
        else:
            missing.append(name + exe)
    if missing:
        listing = ", ".join(sorted(os.listdir(scripts))) if os.path.isdir(scripts) else "(no such dir)"
        fail(f"binaries missing or not executable in {scripts}: {missing}\ncontents: {listing}")
    print("OK")


if __name__ == "__main__":
    main()

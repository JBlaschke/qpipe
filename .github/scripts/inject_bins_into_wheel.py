#!/usr/bin/env python3
"""Inject the workspace's Rust binaries into a built wheel as scripts.

Files under {name}-{version}.data/scripts/ are installed by pip/uv into the
environment's bin/ (Scripts\\ on Windows) and made executable. Repacking with
`python -m wheel pack` recomputes RECORD, so hashes stay valid.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys


def run_cargo_metadata() -> dict:
    return json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"], text=True
        )
    )


def detect_bin_targets(meta: dict) -> list[str]:
    # Root package = the manifest at ./Cargo.toml (same logic as package_release.py).
    root_manifest = pathlib.Path("Cargo.toml").resolve()
    for p in meta.get("packages", []):
        if pathlib.Path(p["manifest_path"]).resolve() == root_manifest:
            return [
                t["name"]
                for t in p.get("targets", [])
                if "bin" in (t.get("kind") or [])
            ]
    return []


def candidate_target_dirs(build_target: str, rust_target: str) -> list[str]:
    # zigbuild glibc-suffixed targets (…-gnu.2.17) sometimes emit under the
    # base triple directory; search both, plus the suffix-stripped form.
    cands: list[str] = []
    for t in (build_target, rust_target):
        t = (t or "").strip()
        if t and t not in cands:
            cands.append(t)
    m = re.match(r"^(.*)\.\d+\.\d+$", (build_target or "").strip())
    if m and m.group(1) not in cands:
        cands.append(m.group(1))
    return cands


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wheel-dir", default=os.getenv("WHEEL_DIR", "dist"))
    ap.add_argument("--build-target", default=os.getenv("BIN_BUILD_TARGET", ""))
    ap.add_argument("--rust-target", default=os.getenv("RUST_TARGET", ""))
    ap.add_argument("--bin-names", default=os.getenv("BIN_NAMES", ""))
    args = ap.parse_args()

    wheels = sorted(pathlib.Path(args.wheel_dir).glob("*.whl"))
    if len(wheels) != 1:
        sys.exit(f"expected exactly one wheel in {args.wheel_dir}, found {len(wheels)}")
    whl = wheels[0]

    detected = detect_bin_targets(run_cargo_metadata())
    names = [x for x in re.split(r"[,\s]+", args.bin_names) if x] or detected
    if not names:
        sys.exit("no bin targets detected and BIN_NAMES not set")

    target_hint = args.rust_target or args.build_target
    is_windows = "windows" in target_hint if target_hint else os.name == "nt"
    exe = ".exe" if is_windows else ""

    cands = candidate_target_dirs(args.build_target, args.rust_target)
    found: dict[str, pathlib.Path] = {}
    missing: list[str] = []
    for name in names:
        for t in cands:
            p = pathlib.Path("target") / t / "release" / f"{name}{exe}"
            if p.exists():
                found[name] = p
                break
        else:
            missing.append(f"{name}{exe}")
    if missing:
        searched = "\n".join(f"  - target/{t}/release" for t in cands)
        sys.exit(
            "binaries not found:\n"
            + "\n".join(f"  - {m}" for m in missing)
            + f"\nsearched:\n{searched}\n"
            "Tip: ensure the bin build step ran with --bins and the right --target."
        )

    work = pathlib.Path("_wheel_inject")
    if work.exists():
        shutil.rmtree(work)
    subprocess.check_call(
        [sys.executable, "-m", "wheel", "unpack", str(whl), "-d", str(work)]
    )
    (unpacked,) = [p for p in work.iterdir() if p.is_dir()]
    scripts = unpacked / f"{unpacked.name}.data" / "scripts"  # e.g. qpipe_rs-1.4.0.data/scripts
    scripts.mkdir(parents=True, exist_ok=True)
    for src in found.values():
        shutil.copy2(src, scripts / src.name)

    out = pathlib.Path("_wheel_out")
    if out.exists():
        shutil.rmtree(out)
    out.mkdir()
    subprocess.check_call(
        [sys.executable, "-m", "wheel", "pack", str(unpacked), "-d", str(out)]
    )
    (new_whl,) = list(out.glob("*.whl"))
    whl.unlink()
    shutil.move(str(new_whl), whl.parent / new_whl.name)
    print(f"injected [{', '.join(found)}] into {new_whl.name}")


if __name__ == "__main__":
    main()

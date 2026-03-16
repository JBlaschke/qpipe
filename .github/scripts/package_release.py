#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shutil
import subprocess
import tarfile
import zipfile
from typing import List, Optional


def run_cargo_metadata() -> dict:
    return json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            text=True,
        )
    )


def choose_root_package(meta: dict) -> dict:
    # Prefer the package whose manifest_path is ./Cargo.toml
    root_manifest = pathlib.Path("Cargo.toml").resolve()
    for p in meta.get("packages", []):
        try:
            if pathlib.Path(p["manifest_path"]).resolve() == root_manifest:
                return p
        except Exception:
            pass

    # Fallback: use first workspace default member
    default_members = meta.get("workspace_default_members") or meta.get("workspace_members") or []
    if default_members:
        root_id = default_members[0]
        for p in meta.get("packages", []):
            if p.get("id") == root_id:
                return p

    # Last resort: first package
    pkgs = meta.get("packages", [])
    if not pkgs:
        raise SystemExit("cargo metadata returned no packages")
    return pkgs[0]


def detect_bin_targets(root_pkg: dict) -> List[str]:
    bins: List[str] = []
    for t in root_pkg.get("targets", []):
        if "bin" in (t.get("kind") or []):
            name = t.get("name")
            if name:
                bins.append(name)
    return bins


def parse_bin_names(override: str, detected: List[str]) -> List[str]:
    override = (override or "").strip()
    if override:
        return [x for x in re.split(r"[,\s]+", override) if x]
    return detected


def candidate_target_dirs(build_target: str, rust_target: str) -> List[str]:
    """
    cargo-zigbuild glibc-suffixed targets (e.g. x86_64-unknown-linux-gnu.2.32)
    sometimes emit artifacts under the *base* triple directory.
    So we try:
      - BUILD_TARGET (as given)
      - RUST_TARGET (base triple)
      - BUILD_TARGET with trailing ".<major>.<minor>" stripped
    """
    cands: List[str] = []
    for t in (build_target, rust_target):
        t = (t or "").strip()
        if t and t not in cands:
            cands.append(t)

    m = re.match(r"^(.*)\.\d+\.\d+$", (build_target or "").strip())
    if m:
        base = m.group(1)
        if base and base not in cands:
            cands.append(base)

    return cands


def copy_if_exists(src: pathlib.Path, dst_dir: pathlib.Path) -> None:
    if src.exists():
        shutil.copy2(src, dst_dir / src.name)


def main() -> None:
    ap = argparse.ArgumentParser(description="Package Rust release binaries into an archive.")
    ap.add_argument("--build-target", default=os.getenv("BUILD_TARGET", ""), help="Target dir to read binaries from (Cargo target triple).")
    ap.add_argument("--rust-target", default=os.getenv("RUST_TARGET", ""), help="Base Rust triple (used as fallback for zigbuild outputs).")
    ap.add_argument("--asset-target", default=os.getenv("ASSET_TARGET", ""), help="Target string used for naming the release asset.")
    ap.add_argument("--tag", default=os.getenv("TAG", ""), help="Tag name (e.g. v1.2.3).")
    ap.add_argument("--archive-ext", default=os.getenv("ARCHIVE_EXT", "tar.gz"), choices=["tar.gz", "zip"])
    ap.add_argument("--bin-names", default=os.getenv("BIN_NAMES", ""), help="Optional override: comma/space separated binary names.")
    args = ap.parse_args()

    if not args.tag:
        raise SystemExit("TAG is required (e.g. TAG=v1.2.3)")
    if not args.asset_target:
        raise SystemExit("ASSET_TARGET is required")
    if not args.build_target:
        raise SystemExit("BUILD_TARGET is required")

    meta = run_cargo_metadata()
    root_pkg = choose_root_package(meta)
    pkg_name = root_pkg.get("name") or "package"

    detected_bins = detect_bin_targets(root_pkg)
    bin_names = parse_bin_names(args.bin_names, detected_bins)

    if not bin_names:
        raise SystemExit(
            "No binary targets found to package.\n"
            "If this is intentional (library-only), remove packaging from the workflow.\n"
            "Otherwise, check your Cargo.toml targets."
        )

    exe = ".exe" if args.asset_target.endswith("windows-msvc") else ""

    out_name = f"{pkg_name}-{args.tag}-{args.asset_target}"
    dist = pathlib.Path("dist") / out_name
    if dist.exists():
        shutil.rmtree(dist)
    dist.mkdir(parents=True)

    cands = candidate_target_dirs(args.build_target, args.rust_target)

    missing: List[str] = []
    for name in bin_names:
        found: Optional[pathlib.Path] = None
        for t in cands:
            p = pathlib.Path("target") / t / "release" / f"{name}{exe}"
            if p.exists():
                found = p
                break

        if found is None:
            missing.append(f"{name}{exe}")
        else:
            shutil.copy2(found, dist / f"{name}{exe}")

    if missing:
        searched = "\n".join(f"  - target/{t}/release" for t in cands)
        raise SystemExit(
            "Some expected binaries were not found after build:\n"
            + "\n".join(f"  - {m}" for m in missing)
            + "\n\nDetected bin targets from cargo metadata:\n"
            + ("\n".join(f"  - {b}" for b in detected_bins) if detected_bins else "  (none)")
            + "\n\nSearched in:\n"
            + searched
            + "\n\nTip: ensure the build step uses `--bins` and that BUILD_TARGET/RUST_TARGET are set correctly."
        )

    # Optional docs
    for fn in ("README.md", "README.txt", "LICENSE", "LICENSE.md", "COPYING", "CLA.md", "COMMERCIAL.md"):
        copy_if_exists(pathlib.Path(fn), dist)

    archive = pathlib.Path(f"{out_name}.{args.archive_ext}")
    if archive.exists():
        archive.unlink()

    if args.archive_ext == "zip":
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for file in dist.rglob("*"):
                if file.is_file():
                    z.write(file, arcname=f"{out_name}/{file.relative_to(dist).as_posix()}")
    else:  # tar.gz
        with tarfile.open(archive, "w:gz") as t:
            t.add(dist, arcname=out_name)

    print(f"Packaged bins: {', '.join(bin_names)}")
    print(f"Created: {archive}")


if __name__ == "__main__":
    main()

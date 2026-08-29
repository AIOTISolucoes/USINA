#!/usr/bin/env python3
"""Build Leaflet manifests from Google Earth KMZ super-overlays.

The KMZ files used by the tracker map already contain a georeferenced image
pyramid.  This tool keeps that pyramid intact: it writes one small JSON
manifest per plant and, when requested, streams the image members directly to
S3 without expanding hundreds of megabytes on disk.

Examples:
    python tools/build_orthomosaics.py --source-dir "$HOME/Downloads"
    python tools/build_orthomosaics.py --source-dir "$HOME/Downloads" --upload
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import posixpath
import sys
import zipfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable
from xml.etree import ElementTree as ET


BUCKET = "scada-dashboard-web"
S3_PREFIX = "orthomosaics/v1"
PUBLIC_BASE_URL = f"https://{BUCKET}.s3.amazonaws.com/{S3_PREFIX}"


@dataclass(frozen=True)
class Source:
    filename: str
    slug: str
    name: str
    captured_at: str | None
    plant_ids: tuple[int, ...]
    aliases: tuple[str, ...]


SOURCES = (
    Source("GP SOLAR 1 A 4.kmz", "gps1", "GPS1", None, (32,), ("gps1", "gps 1")),
    Source("GP SOLAR 5 A 8.kmz", "gps2", "GPS2", None, (33,), ("gps2", "gps 2")),
    Source("GP SOLAR 9 A 14.kmz", "gps3", "GPS3", None, (34,), ("gps3", "gps 3")),
    Source("ITAPIPOCA234.kmz", "itapipoca", "Itapipoca", None, (21,), ("itapipoca",)),
    Source(
        "KMZ - ACOPIARA - 10.03.2026.kmz",
        "acopiara",
        "Acopiara",
        "2026-03-10",
        (13,),
        ("acopiara",),
    ),
    Source(
        "Ortho_Aquiraz_29_12_2025.kmz",
        "aquiraz",
        "Aquiraz",
        "2025-12-29",
        (),
        ("aquiraz",),
    ),
    Source(
        "SANTA QUITÉRIA.kmz",
        "santa-quiteria",
        "Santa Quitéria",
        None,
        (16,),
        ("santa quiteria", "santaquiteria"),
    ),
)


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def resolve_member(kml_name: str, href: str) -> str:
    folder = str(PurePosixPath(kml_name).parent)
    if folder == ".":
        folder = ""
    return posixpath.normpath(posixpath.join(folder, href)).lstrip("/")


def parse_kmz(path: Path, source: Source) -> tuple[dict, list[str]]:
    levels: dict[int, list[list[object]]] = {}
    image_members: set[str] = set()
    south = west = float("inf")
    north = east = float("-inf")

    with zipfile.ZipFile(path) as archive:
        archive_names = set(archive.namelist())
        for kml_name in sorted(n for n in archive_names if n.lower().endswith(".kml")):
            root = ET.fromstring(archive.read(kml_name))
            for overlay in root.iter():
                if local_name(overlay.tag) != "GroundOverlay":
                    continue

                values: dict[str, str] = {}
                for child in overlay.iter():
                    key = local_name(child.tag)
                    if key in {"north", "south", "east", "west", "drawOrder", "href"} and child.text:
                        values[key] = child.text.strip()

                required = {"north", "south", "east", "west", "href"}
                if not required.issubset(values):
                    continue

                member = resolve_member(kml_name, values["href"])
                if member not in archive_names:
                    raise RuntimeError(f"{path.name}: image referenced by KML was not found: {member}")

                level = int(values.get("drawOrder", "1"))
                tile_south = round(float(values["south"]), 12)
                tile_west = round(float(values["west"]), 12)
                tile_north = round(float(values["north"]), 12)
                tile_east = round(float(values["east"]), 12)
                levels.setdefault(level, []).append(
                    [tile_south, tile_west, tile_north, tile_east, member]
                )
                image_members.add(member)
                south = min(south, tile_south)
                west = min(west, tile_west)
                north = max(north, tile_north)
                east = max(east, tile_east)

    if not image_members:
        raise RuntimeError(f"{path.name}: no georeferenced GroundOverlay was found")

    ordered_levels = {str(level): levels[level] for level in sorted(levels)}
    manifest = {
        "version": 1,
        "slug": source.slug,
        "name": source.name,
        "captured_at": source.captured_at,
        "plant_ids": list(source.plant_ids),
        "aliases": list(source.aliases),
        "bounds": [[south, west], [north, east]],
        # drawOrder 1 is adequate at Leaflet zoom 16; each following level
        # doubles the native resolution until drawOrder 8 / zoom 23.
        "zoom_level_offset": 15,
        "min_level": min(levels),
        "max_level": max(levels),
        "tile_base_url": f"{PUBLIC_BASE_URL}/{source.slug}/",
        "levels": ordered_levels,
    }
    return manifest, sorted(image_members)


def content_type(member: str) -> str:
    guessed, _ = mimetypes.guess_type(member)
    return guessed or "application/octet-stream"


def upload_members(path: Path, source: Source, members: Iterable[str], workers: int) -> None:
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise SystemExit("boto3 is required only for --upload; install it with: python -m pip install boto3") from exc

    client = boto3.client(
        "s3",
        region_name="us-east-1",
        config=Config(max_pool_connections=max(10, workers)),
    )
    member_list = list(members)
    total = len(member_list)

    def upload_one(member: str, body: bytes) -> None:
        client.put_object(
            Bucket=BUCKET,
            Key=f"{S3_PREFIX}/{source.slug}/{member}",
            Body=body,
            ContentType=content_type(member),
            CacheControl="public, max-age=31536000, immutable",
            ServerSideEncryption="AES256",
        )

    completed = 0
    # Read the KMZ sequentially and keep only a bounded number of upload
    # bodies in memory. This avoids opening the same OneDrive-backed archive
    # once per worker, which can stall Windows' cloud-file provider.
    with zipfile.ZipFile(path) as archive, ThreadPoolExecutor(max_workers=workers) as executor:
        pending = set()
        for member in member_list:
            pending.add(executor.submit(upload_one, member, archive.read(member)))
            if len(pending) < workers * 3:
                continue
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                future.result()
                completed += 1
                if completed % 250 == 0:
                    print(f"  upload {source.slug}: {completed}/{total}", flush=True)

        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                future.result()
                completed += 1
                if completed == total or completed % 250 == 0:
                    print(f"  upload {source.slug}: {completed}/{total}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "assets" / "orthomosaics",
    )
    parser.add_argument("--only", action="append", help="process only this slug; may be repeated")
    parser.add_argument("--upload", action="store_true", help=f"upload image members to s3://{BUCKET}/{S3_PREFIX}")
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()

    selected = [s for s in SOURCES if not args.only or s.slug in set(args.only)]
    if not selected:
        parser.error("--only did not match any configured slug")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    catalog = []
    for source in selected:
        path = args.source_dir / source.filename
        if not path.exists():
            raise SystemExit(f"KMZ not found: {path}")

        print(f"building {source.slug} from {path.name} ...", flush=True)
        manifest, members = parse_kmz(path, source)
        manifest_path = args.output_dir / f"{source.slug}.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        print(
            f"  manifest: {manifest_path} ({len(members)} tiles, levels {manifest['min_level']}..{manifest['max_level']})",
            flush=True,
        )
        catalog.append(
            {
                "slug": source.slug,
                "name": source.name,
                "plant_ids": list(source.plant_ids),
                "aliases": list(source.aliases),
                "manifest_url": f"assets/orthomosaics/{source.slug}.json",
                "bounds": manifest["bounds"],
            }
        )
        if args.upload:
            upload_members(path, source, members, max(1, args.workers))

    catalog_path = args.output_dir / "catalog.json"
    catalog_path.write_text(
        json.dumps({"version": 1, "items": catalog}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"catalog: {catalog_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Filename rules for 1:1 matching between script/audio/report/meta."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

STEM_RE = re.compile(r"^(?P<date>\d{8})_(?P<time>\d{4})_(?P<tag>[a-z0-9_-]+)$")


@dataclass(frozen=True)
class BuildFiles:
    stem: str
    script_txt: Path
    audio_mp3: Path
    report_html: Path
    meta_json: Path


def build_stem(date_yyyymmdd: str, time_hhmm: str, suffix: str = "arirang") -> str:
    return f"{date_yyyymmdd}_{time_hhmm}_{suffix}"


def build_file_bundle(base_dir: str | Path, date_yyyymmdd: str, time_hhmm: str, suffix: str = "arirang") -> BuildFiles:
    stem = build_stem(date_yyyymmdd=date_yyyymmdd, time_hhmm=time_hhmm, suffix=suffix)
    base = Path(base_dir)
    return BuildFiles(
        stem=stem,
        script_txt=base / f"{stem}.txt",
        audio_mp3=base / f"{stem}.mp3",
        report_html=base / f"{stem}.html",
        meta_json=base / f"{stem}_meta.json",
    )


def validate_stem(stem: str) -> bool:
    return STEM_RE.match(stem) is not None


def validate_bundle(paths: BuildFiles) -> bool:
    stem = paths.stem
    if not validate_stem(stem):
        return False
    return (
        paths.script_txt.name == f"{stem}.txt"
        and paths.audio_mp3.name == f"{stem}.mp3"
        and paths.report_html.name == f"{stem}.html"
        and paths.meta_json.name == f"{stem}_meta.json"
    )

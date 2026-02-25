from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import re
import sys
from typing import Any

from modules.analyzer import analyze_vocabulary, save_vocabulary
from modules.crawler import download_episode, fetch_episode_detail, fetch_episode_list
from modules.reporter import generate_report


LOGGER = logging.getLogger("pipeline")

DEFAULT_CONFIG: dict[str, Any] = {
    "schedule": {
        "time": "07:30",
        "timezone": "Asia/Seoul",
    },
    "crawl": {
        "target_url": "https://www.arirang.com/radio/132",
        "download_path": "./downloads",
        "retry_count": 3,
        "retry_delay": 1.5,
        "timeout_sec": 20,
    },
    "vocabulary": {
        "min_word_length": 4,
        "top_n_words": 30,
        "translation_language": "ko",
    },
    "paths": {
        "logs_dir": "logs",
        "reports_dir": "reports",
    },
}


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_update(out[k], v)
        else:
            out[k] = v
    return out


def _target_date_default() -> str:
    # Pipeline default target is D-1 (KST) => YYYYMMDD
    now = datetime.utcnow() + timedelta(hours=9)
    d1 = now.date() - timedelta(days=1)
    return d1.strftime("%Y%m%d")


def _date_display(date_yyyymmdd: str) -> str:
    return f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"


def _bundle_paths(cfg: dict[str, Any], target_date: str) -> dict[str, Path]:
    download_dir = Path(cfg.get("crawl", {}).get("download_path", "./downloads"))
    logs_dir = Path(cfg.get("paths", {}).get("logs_dir", "logs"))
    reports_dir = Path(cfg.get("paths", {}).get("reports_dir", "reports"))
    stem = f"{target_date}_2155_arirang"
    return {
        "download_dir": download_dir,
        "logs_dir": logs_dir,
        "reports_dir": reports_dir,
        "txt": download_dir / f"{stem}.txt",
        "mp3": download_dir / f"{stem}.mp3",
        "meta": download_dir / f"{stem}_meta.json",
        "vocab_json": logs_dir / f"vocabulary_{target_date}.json",
        "vocab_csv": logs_dir / f"vocabulary_{target_date}.csv",
        "report": reports_dir / f"report_{target_date}_2155.html",
    }


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Load YAML config and merge with defaults.

    If config file is missing or unreadable, returns hardcoded defaults.
    """

    cfg_path = Path(config_path)
    if not cfg_path.exists():
        LOGGER.warning("Config not found: %s. Using defaults.", cfg_path)
        return deepcopy(DEFAULT_CONFIG)

    try:
        import yaml  # type: ignore
    except ImportError:
        LOGGER.warning("PyYAML not installed. Using defaults only.")
        return deepcopy(DEFAULT_CONFIG)

    try:
        loaded = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            LOGGER.warning("Config format invalid. Using defaults.")
            return deepcopy(DEFAULT_CONFIG)
        merged = _deep_update(DEFAULT_CONFIG, loaded)
        return merged
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to parse config (%s). Using defaults.", exc)
        return deepcopy(DEFAULT_CONFIG)


def _select_episode_for_date(episodes: list[dict[str, Any]], target_date: str) -> dict[str, Any] | None:
    d1 = [e for e in episodes if str(e.get("date_str", "")) == target_date]
    if not d1:
        LOGGER.warning("No episode candidates for target date=%s", target_date)
        return None

    token_patterns = [r"21[:.]55", r"\b10\s*PM\b", r"\b2155\b"]

    def has_token(ep: dict[str, Any]) -> bool:
        text = f"{ep.get('title', '')} {ep.get('airtime', '')}"
        return any(re.search(pat, text, flags=re.IGNORECASE) for pat in token_patterns)

    matched = [e for e in d1 if has_token(e)]
    if matched:
        chosen = matched[0]
        LOGGER.info(
            "Selection rule: D-1 + target timeslot token matched (21:55/10 PM/2155). url=%s",
            chosen.get("detail_url", ""),
        )
        return chosen

    def hhmm(ep: dict[str, Any]) -> str:
        t = str(ep.get("airtime", ""))
        if re.fullmatch(r"\d{2}:\d{2}", t):
            return t.replace(":", "")
        return "0000"

    chosen = sorted(d1, key=hhmm, reverse=True)[0]
    LOGGER.info(
        "Selection rule: no explicit target token; selected latest episode in date=%s airtime=%s",
        target_date,
        chosen.get("airtime", ""),
    )
    return chosen


def step_crawl(cfg: dict[str, Any], target_date: str) -> dict[str, Any] | None:
    """Run crawl step: list -> detail -> download.

    Returns selected episode dict on success, or None when not found.
    """

    episodes = fetch_episode_list(cfg)
    episode = _select_episode_for_date(episodes, target_date)
    if not episode:
        return None

    detailed = fetch_episode_detail(episode, cfg)
    downloaded = download_episode(detailed, cfg)

    paths = _bundle_paths(cfg, target_date)
    downloaded["txt_filename"] = paths["txt"].name
    downloaded["mp3_filename"] = paths["mp3"].name
    downloaded["date"] = _date_display(target_date)
    downloaded["airtime"] = "21:55"
    return downloaded


def _load_episode_from_meta(cfg: dict[str, Any], target_date: str) -> dict[str, Any]:
    paths = _bundle_paths(cfg, target_date)
    meta_path = paths["meta"]
    if not meta_path.exists():
        raise FileNotFoundError(f"Meta file not found: {meta_path}")

    episode = json.loads(meta_path.read_text(encoding="utf-8"))
    episode["txt_path"] = str(paths["txt"])
    episode["mp3_path"] = str(paths["mp3"])
    episode["meta_path"] = str(paths["meta"])
    episode["mp3_filename"] = episode.get("mp3_filename", paths["mp3"].name)
    return episode


def step_analyze(episode: dict[str, Any], cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], tuple[str, str]]:
    """Run analyze step using script text or fallback txt file."""

    script_text = str(episode.get("script_text", "") or "").strip()

    if not script_text:
        txt_path = Path(str(episode.get("txt_path", "") or ""))
        if not txt_path.exists():
            # fallback from filename rule
            date_digits = re.sub(r"[^0-9]", "", str(episode.get("date", "")))[:8]
            if not date_digits:
                raise FileNotFoundError("No script_text and no txt_path available")
            txt_path = _bundle_paths(cfg, date_digits)["txt"]

        if not txt_path.exists():
            raise FileNotFoundError(f"Script txt not found: {txt_path}")
        script_text = txt_path.read_text(encoding="utf-8")

    vocab_data = analyze_vocabulary(script_text, cfg)

    date_digits = re.sub(r"[^0-9]", "", str(episode.get("date", "")))[:8]
    if not date_digits:
        # final fallback: today KST D-1
        date_digits = _target_date_default()

    save_paths = save_vocabulary(vocab_data, date_digits, cfg)
    return vocab_data, save_paths


def step_report(episode: dict[str, Any], vocab_data: list[dict[str, Any]], cfg: dict[str, Any]) -> str:
    """Run report step and save interactive HTML."""

    script_text = str(episode.get("script_text", "") or "").strip()
    if not script_text:
        txt_path = Path(str(episode.get("txt_path", "") or ""))
        if not txt_path.exists():
            txt_filename = str(episode.get("txt_filename", "") or "")
            if txt_filename:
                txt_path = Path(cfg.get("crawl", {}).get("download_path", "./downloads")) / txt_filename
        if txt_path.exists():
            episode["script_text"] = txt_path.read_text(encoding="utf-8")

    return generate_report(episode, vocab_data, cfg)


def _load_vocab_from_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    return items


def run_pipeline(cfg: dict[str, Any], target_date: str, step: str | None) -> None:
    """Run pipeline stages.

    If `step` is provided, run only that stage.
    On failure exits with status 1.
    """

    try:
        episode: dict[str, Any] | None = None
        vocab_data: list[dict[str, Any]] = []

        if step in (None, "crawl"):
            LOGGER.info("[STEP] crawl start")
            episode = step_crawl(cfg, target_date)
            if episode is None:
                LOGGER.error("[STEP] crawl failed: no matching episode")
                sys.exit(1)
            LOGGER.info("[STEP] crawl success")
            if step == "crawl":
                return

        if step == "analyze":
            LOGGER.info("[STEP] analyze start")
            episode = _load_episode_from_meta(cfg, target_date)
            vocab_data, save_paths = step_analyze(episode, cfg)
            LOGGER.info("[STEP] analyze success json=%s csv=%s count=%d", save_paths[0], save_paths[1], len(vocab_data))
            return

        if step == "report":
            LOGGER.info("[STEP] report start")
            episode = _load_episode_from_meta(cfg, target_date)
            paths = _bundle_paths(cfg, target_date)
            if not paths["vocab_json"].exists():
                raise FileNotFoundError(f"Vocabulary JSON not found: {paths['vocab_json']}")
            vocab_data = _load_vocab_from_json(paths["vocab_json"])
            out = step_report(episode, vocab_data, cfg)
            LOGGER.info("[STEP] report success path=%s", out)
            return

        # Full run (step is None)
        if episode is None:
            raise RuntimeError("episode missing before analyze")

        LOGGER.info("[STEP] analyze start")
        vocab_data, save_paths = step_analyze(episode, cfg)
        LOGGER.info("[STEP] analyze success json=%s csv=%s count=%d", save_paths[0], save_paths[1], len(vocab_data))

        LOGGER.info("[STEP] report start")
        out = step_report(episode, vocab_data, cfg)
        LOGGER.info("[STEP] report success path=%s", out)

    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Pipeline failed: %s", exc)
        sys.exit(1)


def run_demo(cfg: dict[str, Any]) -> None:
    """Run demo mode without crawling.

    Uses a hardcoded sample script (100+ words) and runs analyze -> report.
    """

    sample_script = (
        "Global markets opened with mixed sentiment as investors weighed inflation data, "
        "new fiscal guidance, and geopolitical risks in several regions. Analysts said the "
        "latest parliamentary debate on subsidy reform and trade tariffs could reshape "
        "industrial competitiveness across Asia. Meanwhile, humanitarian agencies warned that "
        "continued disruptions in logistics and energy infrastructure may intensify regional "
        "volatility. In a separate briefing, regulators highlighted compliance failures linked "
        "to cross-border procurement contracts and called for stronger oversight mechanisms. "
        "Diplomatic channels remain active, but negotiators acknowledged that consensus is "
        "unlikely before next month. Economists added that liquidity conditions remain tight, "
        "which may delay investment in decarbonization projects despite public commitments. "
        "At the same time, technology firms accelerated cybersecurity spending after reports of "
        "coordinated disinformation campaigns targeting election systems. Observers noted that "
        "the current environment requires balanced policy, transparent communication, and "
        "credible long-term planning to prevent a prolonged stalemate."
    )

    target_date = _target_date_default()
    episode = {
        "date": _date_display(target_date),
        "airtime": "21:55",
        "title": "Demo Arirang News Episode",
        "mp3_filename": "",
        "script_text": sample_script,
    }

    LOGGER.info("[DEMO] analyze start")
    vocab_data, save_paths = step_analyze(episode, cfg)
    LOGGER.info("[DEMO] analyze success json=%s csv=%s count=%d", save_paths[0], save_paths[1], len(vocab_data))

    LOGGER.info("[DEMO] report start")
    out = step_report(episode, vocab_data, cfg)
    LOGGER.info("[DEMO] report success path=%s", out)


def _setup_logging(target_date: str, cfg: dict[str, Any]) -> None:
    logs_dir = Path(cfg.get("paths", {}).get("logs_dir", "logs"))
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"pipeline_{target_date}.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Arirang Learner pipeline orchestrator")
    p.add_argument("--date", default="", help="Run target date (YYYYMMDD)")
    p.add_argument("--step", choices=["crawl", "analyze", "report"], default=None)
    p.add_argument("--demo", action="store_true", help="Run demo mode without crawling")
    p.add_argument("--config", default="config.yaml", help="Config file path")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    target_date = args.date or _target_date_default()

    # Load config first with lightweight fallback logging.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    cfg = load_config(args.config)

    # Reconfigure with file handler.
    for h in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(h)
    _setup_logging(target_date, cfg)

    LOGGER.info("Pipeline start date=%s step=%s demo=%s", target_date, args.step, args.demo)
    if args.demo:
        try:
            run_demo(cfg)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Demo failed: %s", exc)
            sys.exit(1)
    else:
        run_pipeline(cfg, target_date=target_date, step=args.step)

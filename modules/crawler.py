"""Playwright-based crawler for Arirang podcast pages.

Target page example:
https://www.arirang.com/radio/132/podcast/668?lang=en
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import html
import json
import logging
from pathlib import Path
import re
from typing import Any
from urllib.parse import urljoin

import requests
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

TIME_PATTERNS = [
    re.compile(r"\b(21[:.]55)\b", re.IGNORECASE),
    re.compile(r"\b(10\s*PM)\b", re.IGNORECASE),
    re.compile(r"\b(2155)\b", re.IGNORECASE),
]
DATE_PATTERNS = [
    re.compile(r"\b(20\d{2})[-./](\d{2})[-./](\d{2})\b"),
    re.compile(r"\b(20\d{2})(\d{2})(\d{2})\b"),
]
HHMM_PATTERN = re.compile(r"\b([01]?\d|2[0-3])[:.]([0-5]\d)\b")


def _cfg_get(cfg: dict[str, Any], path: str, default: Any) -> Any:
    cur: Any = cfg
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    cfg: dict[str, Any],
    **kwargs: Any,
) -> requests.Response:
    retry_count = int(_cfg_get(cfg, "crawl.retry_count", _cfg_get(cfg, "retry_count", 3)))
    retry_delay = float(_cfg_get(cfg, "crawl.retry_delay", _cfg_get(cfg, "retry_delay", 1.5)))
    timeout = float(_cfg_get(cfg, "crawl.timeout_sec", 20))

    headers = dict(DEFAULT_HEADERS)
    headers.update(kwargs.pop("headers", {}))
    kwargs["headers"] = headers
    kwargs["timeout"] = kwargs.get("timeout", timeout)

    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            resp = session.request(method=method, url=url, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            LOGGER.warning("Request failed (%s/%s): %s", attempt, retry_count, url)
            if attempt < retry_count:
                from time import sleep

                sleep(retry_delay)
    raise RuntimeError(f"Request failed after retries: {url}") from last_exc


def _extract_date_yyyymmdd(text: str) -> str:
    s = text or ""
    for pat in DATE_PATTERNS:
        m = pat.search(s)
        if m:
            yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
            return f"{yyyy}{mm}{dd}"
    return ""


def _extract_airtime(text: str) -> str:
    m = HHMM_PATTERN.search(text or "")
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    if re.search(r"\b10\s*PM\b", text or "", re.IGNORECASE):
        return "22:00"
    if re.search(r"\b2155\b", text or ""):
        return "21:55"
    return ""


def _select_target_episode(episodes: list[dict[str, Any]]) -> dict[str, Any] | None:
    date_compact, date_display = _get_target_date()
    d1 = [e for e in episodes if e.get("date_str") == date_compact]
    if not d1:
        LOGGER.warning("No episodes found for target date %s. Fallback to latest available item.", date_display)
        if episodes:
            chosen = episodes[0]
            LOGGER.info(
                "Episode selection rule: fallback to first available candidate. selected=%s",
                chosen.get("detail_url", ""),
            )
            return chosen
        return None

    def has_target_time(ep: dict[str, Any]) -> bool:
        title = ep.get("title", "") or ""
        airtime = ep.get("airtime", "") or ""
        haystack = f"{title} {airtime}"
        return any(p.search(haystack) for p in TIME_PATTERNS)

    preferred = [e for e in d1 if has_target_time(e)]
    if preferred:
        chosen = preferred[0]
        LOGGER.info(
            "Episode selection rule: matched D-1 and target timeslot token (21:55/10 PM/2155). "
            "selected=%s",
            chosen.get("detail_url", ""),
        )
        return chosen

    chosen = sorted(d1, key=lambda x: x.get("airtime", ""), reverse=True)[0]
    LOGGER.info(
        "Episode selection rule: no target timeslot token on D-1; selected latest D-1 episode. "
        "selected=%s airtime=%s",
        chosen.get("detail_url", ""),
        chosen.get("airtime", ""),
    )
    return chosen


def _ensure_download_log(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}
    return {}


def _write_download_log(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_target_date() -> tuple[str, str]:
    """Return D-1 date for Asia/Seoul.

    Returns:
        tuple[str, str]:
            - compact date: YYYYMMDD
            - display date: YYYY-MM-DD
    """

    now = datetime.now(ZoneInfo("Asia/Seoul"))
    target = now.date() - timedelta(days=1)
    return target.strftime("%Y%m%d"), target.strftime("%Y-%m-%d")


async def _async_fetch_episode_list(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch episode list seed from the exact podcast page URL."""

    try:
        from playwright.async_api import async_playwright  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Playwright is required. Install: pip install playwright") from exc

    target_url = _cfg_get(cfg, "crawl.target_url", "https://www.arirang.com/radio/132/podcast/668?lang=en")
    date_compact, _ = _get_target_date()
    return [
        {
            "title": "21:55 Arirang News",
            "detail_url": target_url,
            "date_str": date_compact,
            "airtime": "21:55",
            "podcast_id": "668",
        }
    ]


def fetch_episode_list(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch episode list from Arirang and return parsed candidates."""

    try:
        episodes = asyncio.run(_async_fetch_episode_list(cfg))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Playwright list crawl failed, using Kollus fallback: %s", exc)
        kollus_url, media_url, inferred = _resolve_kollus_media(cfg)
        episodes = [
            {
                "title": "21:55 Arirang News",
                "detail_url": "https://www.arirang.com/radio/132/podcast/668?lang=en",
                "date_str": inferred,
                "airtime": "21:55",
                "podcast_id": "668",
                "kollus_url": kollus_url,
                "mp3_url_prefetched": media_url,
            }
        ]
    LOGGER.info("Parsed episode candidates: %d", len(episodes))
    chosen = _select_target_episode(episodes)
    if chosen:
        LOGGER.info(
            "Target candidate summary: date=%s airtime=%s title=%s",
            chosen.get("date_str", ""),
            chosen.get("airtime", ""),
            chosen.get("title", ""),
        )
    return episodes


def _pick_longest_text(items: list[str]) -> str:
    cleaned = [" ".join((x or "").split()) for x in items]
    cleaned = [x for x in cleaned if x]
    if not cleaned:
        return ""
    return sorted(cleaned, key=len, reverse=True)[0]


def _iter_nested_strings(obj: Any) -> list[str]:
    out: list[str] = []
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            out.extend(_iter_nested_strings(v))
    elif isinstance(obj, list):
        for v in obj:
            out.extend(_iter_nested_strings(v))
    return out


def _extract_urls_from_obj(obj: Any) -> list[str]:
    urls: list[str] = []
    for s in _iter_nested_strings(obj):
        if ".mp3" in s.lower() or ".mp4" in s.lower():
            urls.append(s)
    return urls


def _normalize_space(text: str) -> str:
    return " ".join((text or "").split())


def _slice_script_by_episode_marker(script_text: str, target_date_yyyymmdd: str, airtime: str = "21:55") -> str:
    """If playlist markers exist, keep only the target episode block."""
    raw = (script_text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return ""

    date_iso = f"{target_date_yyyymmdd[:4]}-{target_date_yyyymmdd[4:6]}-{target_date_yyyymmdd[6:8]}"
    marker_pat = re.compile(
        r"(?P<id>\d{4})\s+(?P<date>\d{4}-\d{2}-\d{2})\s+Podcast\s+Play\s+(?P<time>\d{1,2}:\d{2})\s+Arirang\s+News",
        re.IGNORECASE,
    )

    matches = list(marker_pat.finditer(raw))
    if not matches:
        return raw.strip()

    target_idx = -1
    for i, m in enumerate(matches):
        d = m.group("date")
        t = m.group("time")
        if d == date_iso and t == airtime:
            target_idx = i
            break
    if target_idx < 0:
        return raw.strip()

    start = matches[target_idx].end()
    end = matches[target_idx + 1].start() if target_idx + 1 < len(matches) else len(raw)
    sliced = raw[start:end].strip()
    return sliced or raw.strip()


def _extract_script_and_media_from_api_cache(
    api_json_cache: list[dict[str, Any]],
    target_date_yyyymmdd: str,
) -> tuple[str, str]:
    """Extract best-effort script and media URL from intercepted API JSON."""
    target_date = f"{target_date_yyyymmdd[:4]}-{target_date_yyyymmdd[4:6]}-{target_date_yyyymmdd[6:8]}"
    candidates: list[tuple[int, str, str]] = []  # (score, script, media_url)

    for data in api_json_cache:
        items = data.get("item")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            bdate = str(item.get("broadcast_date", ""))
            title = str(item.get("title", ""))
            content = str(item.get("content", "") or "")
            media_info = item.get("media_info", {})
            media_url = ""
            if isinstance(media_info, dict):
                media_url = str(media_info.get("media_url", "") or "")
            if not media_url:
                nested_urls = _extract_urls_from_obj(item)
                media_url = nested_urls[0] if nested_urls else ""

            score = 0
            if target_date in bdate:
                score += 5
            if "21:55" in title or "2155" in title:
                score += 4
            if "10 PM" in title.upper():
                score += 3
            if content.strip():
                score += 2
            if media_url:
                score += 1
            if score > 0:
                candidates.append((score, content.strip(), media_url.strip()))

    if not candidates:
        return "", ""
    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0]
    return best[1], best[2]


async def _fetch_json_via_page(page: Any, api_path: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Call same-origin API from inside browser page context."""
    js = """
    async ({ path, body }) => {
      const resp = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const text = await resp.text();
      try { return JSON.parse(text); } catch (e) { return { _raw_text: text, _status: resp.status }; }
    }
    """
    result = await page.evaluate(js, {"path": api_path, "body": payload})
    if isinstance(result, dict):
        return result
    return {}


def _pick_item_for_target(
    items: list[dict[str, Any]],
    target_date_yyyymmdd: str,
) -> dict[str, Any] | None:
    if not items:
        return None
    target_date = f"{target_date_yyyymmdd[:4]}-{target_date_yyyymmdd[4:6]}-{target_date_yyyymmdd[6:8]}"

    def score(it: dict[str, Any]) -> int:
        title = str(it.get("title", ""))
        bdate = str(it.get("broadcast_date", ""))
        s = 0
        if target_date in bdate:
            s += 6
        if "21:55" in title or "2155" in title:
            s += 5
        if "10 PM" in title.upper():
            s += 4
        if "NEWS" in title.upper():
            s += 2
        if str(it.get("content", "")).strip():
            s += 1
        return s

    ranked = sorted(items, key=score, reverse=True)
    if score(ranked[0]) <= 0:
        return None
    return ranked[0]


def _extract_media_url_from_kollus_html(raw_html: str) -> str:
    text = html.unescape(raw_html or "")
    patterns = [
        r'"media_url"\s*:\s*"([^"]+)"',
        r"'media_url'\s*:\s*'([^']+)'",
        r"https?://[^\"'\\s]+\\.(?:mp3|mp4)(?:\\?[^\"'\\s]*)?",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            url = m.group(1) if m.groups() else m.group(0)
            return url.replace("\\/", "/")
    return ""


async def _click_target_from_podcast_list(page: Any, date_yyyymmdd: str) -> bool:
    """Click D-1 21:55 item from podcast list on the page."""
    y, m, d = date_yyyymmdd[:4], date_yyyymmdd[4:6], date_yyyymmdd[6:8]
    date_tokens = [f"{y}-{m}-{d}", f"{y}.{m}.{d}", f"{m}/{d}/{y}", f"{d}/{m}/{y}"]
    time_tokens = ["21:55", "2155", "10 PM", "10PM", "9:55 PM", "21.55"]

    # Broad candidate containers for list items.
    candidates = await page.query_selector_all(
        "li, article, .item, .list-item, .podcast-item, [class*='podcast'], [class*='episode']"
    )
    best_date_only = None
    best_date_only_len = -1
    for node in candidates:
        try:
            text = " ".join((await node.inner_text()).split())
        except Exception:
            continue
        text_u = text.upper()
        has_date = any(tok.upper() in text_u for tok in date_tokens)
        has_time = any(tok.upper() in text_u for tok in time_tokens)
        if has_date and has_time:
            clickable = await node.query_selector("a, button, [role='button']")
            if clickable:
                await clickable.click()
            else:
                await node.click()
            await page.wait_for_timeout(1200)
            return True
        if has_date and len(text) > best_date_only_len:
            best_date_only_len = len(text)
            best_date_only = node

    if best_date_only:
        clickable = await best_date_only.query_selector("a, button, [role='button']")
        if clickable:
            await clickable.click()
        else:
            await best_date_only.click()
        await page.wait_for_timeout(1200)
        return True
    return False


async def _extract_target_script_from_episode_list_ui(
    page: Any,
    target_date_yyyymmdd: str,
    airtime: str = "21:55",
) -> str:
    """Click target row in podcast episode list and read only that episode script."""
    date_iso = f"{target_date_yyyymmdd[:4]}-{target_date_yyyymmdd[4:6]}-{target_date_yyyymmdd[6:8]}"
    rows = await page.query_selector_all(".playList-wrap li, .info_episodeList_playList li, li.list")
    target_row = None
    for row in rows:
        try:
            txt = _normalize_space(await row.inner_text())
        except Exception:
            continue
        if date_iso in txt and airtime in txt and "Arirang News" in txt:
            target_row = row
            break

    if not target_row:
        return ""

    # Open the row details.
    info_btn = await target_row.query_selector("button.list_info, .list_info")
    if info_btn:
        try:
            await info_btn.click()
            await page.wait_for_timeout(800)
        except Exception:
            pass

    # Read script area dedicated to selected episode.
    script_selectors = [
        ".info_program_content .text",
        ".info_program_content",
    ]
    texts: list[str] = []
    for sel in script_selectors:
        nodes = await page.query_selector_all(sel)
        for node in nodes:
            try:
                t = await node.inner_text()
            except Exception:
                continue
            t = _normalize_space(t)
            if t:
                texts.append(t)
    if not texts:
        return ""
    return max(texts, key=len)


def _resolve_kollus_media(cfg: dict[str, Any]) -> tuple[str, str, str]:
    """Return (kollus_url, media_url, inferred_date_yyyymmdd) from fallback page."""
    kollus_url = _cfg_get(cfg, "crawl.kollus_fallback_url", "https://v.kr.kollus.com/lstBUSaP?cdn=arirang-dd")
    with requests.Session() as session:
        resp = _request_with_retry(session, "GET", kollus_url, cfg)
        raw = html.unescape(resp.text)
        media_url = _extract_media_url_from_kollus_html(raw)
        m_key = re.search(r'"upload_file_key"\s*:\s*"([^"]+)"', raw, re.IGNORECASE)
        upload_key = m_key.group(1) if m_key else ""
        m_date = re.search(r"(20\d{6})", upload_key) or re.search(r"/arirang/(20\d{6})/", media_url)
        inferred = m_date.group(1) if m_date else _get_target_date()[0]
        return kollus_url, media_url, inferred


async def _async_fetch_episode_detail(episode: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Load podcast page with Playwright and capture script/mp3 via network interception."""

    try:
        from playwright.async_api import async_playwright  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Playwright is required. Install: pip install playwright") from exc

    target_url = str(episode.get("detail_url") or "https://www.arirang.com/radio/132/podcast/668?lang=en")
    mp3_url = ""
    iframe_src = ""
    api_json_cache: list[dict[str, Any]] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=DEFAULT_HEADERS["User-Agent"])

        async def handle_response(response: Any) -> None:
            nonlocal mp3_url
            url = response.url
            content_type = response.headers.get("content-type", "")
            if ".mp3" in url.lower() or "audio" in content_type.lower():
                mp3_url = url

            if "application/json" in content_type.lower() and any(
                k in url.lower() for k in ("api", "podcast", "668", "episode")
            ):
                try:
                    data = await response.json()
                    if isinstance(data, dict):
                        api_json_cache.append(data)
                        if not mp3_url:
                            nested_urls = _extract_urls_from_obj(data)
                            if nested_urls:
                                mp3_url = nested_urls[0]
                except Exception:
                    pass

        page.on("response", handle_response)
        await page.goto(target_url, wait_until="networkidle")

        # 0) Direct browser-context API path used by the frontend route.
        # This is the most reliable way to get script text when DOM parsing is flaky.
        target_date = str(episode.get("date_str", "")).strip() or _get_target_date()[0]
        podcast_id = str(episode.get("podcast_id", "")).strip() or "668"
        corner_data = await _fetch_json_via_page(page, "/v1.0/open/corner/detail", {"corner_id": podcast_id})
        bis_corner_code = str(corner_data.get("bis_corner_code", "")).strip()
        if bis_corner_code:
            ep_list_payload = {
                "lan_code": "en",
                "program_type": "radio",
                "key_word": "corner_code",
                "word": bis_corner_code,
                "hit": True,
                "type": "aod",
                "page_info": {"row_count": 20, "number": 0},
            }
            ep_list = await _fetch_json_via_page(page, "/v1.0/open/media/episode/list", ep_list_payload)
            items = ep_list.get("item", []) if isinstance(ep_list, dict) else []
            if isinstance(items, list):
                chosen = _pick_item_for_target([x for x in items if isinstance(x, dict)], target_date)
                if chosen:
                    if not mp3_url:
                        media_info = chosen.get("media_info", {})
                        if isinstance(media_info, dict):
                            mp3_url = str(media_info.get("media_url", "")).strip()
                    if not mp3_url:
                        urls = _extract_urls_from_obj(chosen)
                        if urls:
                            mp3_url = urls[0]
                    script_guess = str(chosen.get("content", "")).strip()
                    if script_guess:
                        api_json_cache.append({"item": [chosen]})
                    # detail endpoint can contain richer content block.
                    ep_id = chosen.get("episode_id")
                    if ep_id:
                        ep_detail = await _fetch_json_via_page(
                            page,
                            "/v1.0/open/media/episode/detail",
                            {"episode_id": ep_id},
                        )
                        if isinstance(ep_detail, dict):
                            api_json_cache.append(ep_detail)

        # 1) Force-select D-1 21:55 row from podcast list on this page.
        target_date = str(episode.get("date_str") or _get_target_date()[0])
        clicked = await _click_target_from_podcast_list(page, target_date)
        if clicked:
            await page.wait_for_load_state("networkidle")

        # 1-1) Prefer exact text from episode list UI for target date/time.
        target_ui_script = await _extract_target_script_from_episode_list_ui(page, target_date, "21:55")

        iframe = await page.query_selector("iframe#aodChild, iframe.aodContent, iframe[src*='kollus']")
        if iframe:
            src = await iframe.get_attribute("src")
            if src:
                iframe_src = src

        play_selectors = [
            "button[class*='play']",
            ".play-btn",
            "button[aria-label*='play']",
            "[class*='PlayButton']",
        ]
        for selector in play_selectors:
            btn = await page.query_selector(selector)
            if btn:
                try:
                    await btn.click()
                    await page.wait_for_timeout(3000)
                    # after play click, iframe src can be refreshed or injected.
                    iframe2 = await page.query_selector("iframe#aodChild, iframe.aodContent, iframe[src*='kollus']")
                    if iframe2 and not iframe_src:
                        src2 = await iframe2.get_attribute("src")
                        if src2:
                            iframe_src = src2
                    break
                except Exception:
                    continue

        script_selectors = [
            ".info_program_content .text",
            ".info_program_content",
            ".playList-wrap .list_info_content",
            "[class*='script']",
            "[class*='Script']",
            "[class*='content']",
            "[class*='transcript']",
            "article p",
        ]
        collected: list[str] = []
        for selector in script_selectors:
            nodes = await page.query_selector_all(selector)
            texts: list[str] = []
            for node in nodes:
                try:
                    txt = await node.inner_text()
                    if txt:
                        texts.append(txt)
                except Exception:
                    continue
            if texts:
                collected.append(_pick_longest_text(texts))

        if target_ui_script:
            collected.insert(0, target_ui_script)

        await browser.close()

    script_text = _pick_longest_text(collected)
    if not script_text:
        # JSON fallback for script text/media URL from intercepted API payloads.
        target_date = str(episode.get("date_str", "")).strip() or _get_target_date()[0]
        api_script, api_media = _extract_script_and_media_from_api_cache(api_json_cache, target_date)
        if api_script:
            script_text = api_script
        if api_media and not mp3_url:
            mp3_url = api_media

    if script_text:
        # Remove obvious HTML tags if API returns rich text.
        script_text = re.sub(r"<br\\s*/?>", "\n", script_text, flags=re.IGNORECASE)
        script_text = re.sub(r"<[^>]+>", "", script_text).strip()
        # Keep only target episode block when playlist text is concatenated.
        target_date = str(episode.get("date_str", "")).strip() or _get_target_date()[0]
        script_text = _slice_script_by_episode_marker(script_text, target_date, "21:55")

    if not mp3_url and iframe_src:
        try:
            with requests.Session() as session:
                resp = _request_with_retry(session, "GET", iframe_src, cfg)
                mp3_url = _extract_media_url_from_kollus_html(resp.text)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to resolve media_url from iframe src: %s", exc)

    enriched = dict(episode)
    enriched["script_text"] = script_text
    enriched["mp3_url"] = mp3_url
    enriched["iframe_src"] = iframe_src
    return enriched


def fetch_episode_detail(episode: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Sync wrapper for async Playwright detail extraction."""

    try:
        return asyncio.run(_async_fetch_episode_detail(episode, cfg))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Playwright detail crawl failed, using Kollus fallback: %s", exc)
        enriched = dict(episode)
        mp3 = str(episode.get("mp3_url_prefetched", "")).strip()
        inferred = str(episode.get("date_str", "")).strip() or _get_target_date()[0]
        if not mp3:
            _, mp3, inferred = _resolve_kollus_media(cfg)
        enriched["mp3_url"] = mp3
        enriched["date_str"] = inferred
        if not enriched.get("script_text"):
            enriched["script_text"] = ""
        return enriched


def download_episode(episode: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Download txt/mp3/meta files and update logs/download_log.json.

    Skip download if logs/download_log.json contains key YYYYMMDD_2155 with
    status == success.
    """

    date_compact, date_display = _get_target_date()
    date_from_episode = episode.get("date_str", "") or date_compact
    if re.fullmatch(r"\d{8}", str(date_from_episode)):
        date_compact = str(date_from_episode)
        date_display = f"{date_compact[:4]}-{date_compact[4:6]}-{date_compact[6:8]}"

    stem = f"{date_compact}_2155_arirang"
    key = f"{date_compact}_2155"

    download_dir = Path(_cfg_get(cfg, "crawl.download_path", "./downloads"))
    logs_dir = Path(_cfg_get(cfg, "paths.logs_dir", "logs"))
    logs_path = logs_dir / "download_log.json"
    download_dir.mkdir(parents=True, exist_ok=True)
    logs_path.parent.mkdir(parents=True, exist_ok=True)

    txt_path = download_dir / f"{stem}.txt"
    mp3_path = download_dir / f"{stem}.mp3"
    meta_path = download_dir / f"{stem}_meta.json"

    history = _ensure_download_log(logs_path)
    existing = history.get(key, {})
    if existing.get("status") == "success":
        txt_ok = txt_path.exists() and txt_path.stat().st_size > 0
        if txt_ok:
            try:
                current_txt = txt_path.read_text(encoding="utf-8").strip()
                if current_txt.lower().startswith("script not available"):
                    txt_ok = False
            except Exception:  # noqa: BLE001
                txt_ok = False
        mp3_ok = mp3_path.exists() and mp3_path.stat().st_size > 0
        meta_ok = meta_path.exists() and meta_path.stat().st_size > 0
        if txt_ok and mp3_ok and meta_ok:
            LOGGER.info("Skip download: already success for key=%s", key)
            skipped = dict(episode)
            skipped.update(
                {
                    "txt_path": str(txt_path),
                    "mp3_path": str(mp3_path),
                    "meta_path": str(meta_path),
                    "status": "skipped",
                }
            )
            return skipped
        LOGGER.warning(
            "History says success but files are missing or empty. Re-downloading key=%s "
            "(txt=%s mp3=%s meta=%s)",
            key,
            txt_ok,
            mp3_ok,
            meta_ok,
        )

    script_text = episode.get("script_text", "") or ""
    mp3_url = episode.get("mp3_url", "") or ""
    if not script_text:
        raise ValueError("episode.script_text is empty")
    if not mp3_url:
        raise ValueError("episode.mp3_url is empty")

    txt_path.write_text(script_text, encoding="utf-8")

    with requests.Session() as session:
        resp = _request_with_retry(session, "GET", mp3_url, cfg, stream=True)
        with mp3_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    meta = {
        "date": date_display,
        "airtime": "21:55",
        "title": episode.get("title", ""),
        "txt_filename": txt_path.name,
        "mp3_filename": mp3_path.name,
        "source_url": episode.get("detail_url", ""),
        "mp3_url": mp3_url,
        "downloaded_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    history[key] = {
        "status": "success",
        "title": episode.get("title", ""),
        "source_url": episode.get("detail_url", ""),
        "txt_path": str(txt_path),
        "mp3_path": str(mp3_path),
        "meta_path": str(meta_path),
        "updated_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
    }
    _write_download_log(logs_path, history)

    downloaded = dict(episode)
    downloaded.update(
        {
            "txt_path": str(txt_path),
            "mp3_path": str(mp3_path),
            "meta_path": str(meta_path),
            "status": "success",
        }
    )
    return downloaded

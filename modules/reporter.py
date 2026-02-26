"""Interactive HTML report generator.

This module never calls external APIs. It only renders analyzer output
(`vocab_data`) into a single self-contained HTML file.
"""

from __future__ import annotations

import html
import json
import logging
from pathlib import Path
import re
from typing import Any


LOGGER = logging.getLogger(__name__)


def _cfg_get(cfg: dict[str, Any], path: str, default: Any) -> Any:
    cur: Any = cfg
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _normalize_date(date_value: str) -> tuple[str, str]:
    raw = (date_value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        y, m, d = raw[0:4], raw[4:6], raw[6:8]
        return f"{y}.{m}.{d}", f"{y}{m}{d}"
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        y, m, d = raw[0:4], raw[5:7], raw[8:10]
        return f"{y}.{m}.{d}", f"{y}{m}{d}"
    return raw.replace("-", "."), raw.replace("-", "")


def _prettify_script_text(text: str) -> str:
    """Normalize noisy script text for readability in report viewer."""
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return ""
    raw = html.unescape(raw)

    # Strip leftover inline markup from highlighted/script HTML fragments.
    raw = re.sub(
        r"\bdata-[a-z-]+\s*=\s*(?:'[^']*'|\"[^\"]*\"|’[^’]*’|[^\s>]+)\s*(?:>|&gt;)?",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(r"</?mark[^>]*>", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"<[^>]+>", "", raw)

    # Keep one coherent bulletin block only.
    # If multiple "Podcast Play" chunks are concatenated, keep the text before the first chunk marker.
    marker_pat = re.compile(r"\b\d{4}\s+\d{4}-\d{2}-\d{2}\s+Podcast\s+Play\b", re.IGNORECASE)
    marker_match = marker_pat.search(raw)
    if marker_match:
        raw = raw[: marker_match.start()]

    # Start from first "Welcome to Arirang News" when present.
    welcome_idx = raw.lower().find("welcome to arirang news")
    if welcome_idx >= 0:
        raw = raw[welcome_idx:]

    # Remove recurring UI/table noise captured from page containers.
    raw = re.sub(
        r"Podcast\s+List\s+Table\s+NO\s+Date\(KST\)\s+Title\s+\d+\s+\d{4}-\d{2}-\d{2}\s+Podcast\s+Play\s+21:55\s+Arirang\s+News",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    noise_patterns = [
        r"Podcast\s+List\s+Table",
        r"NO\s+Date\(KST\)\s+Title",
        r"^\s*\d+\s+\d{4}-\d{2}-\d{2}\s+Podcast(?:\s+Play)?\s*$",
    ]
    lines: list[str] = []
    for line in raw.split("\n"):
        s = line.strip()
        if not s:
            continue
        if any(re.search(pat, s, flags=re.IGNORECASE) for pat in noise_patterns):
            continue
        lines.append(s)
    text = "\n".join(lines)

    # Ensure list paragraph markers start new paragraphs only when likely heading bullets.
    # Avoid breaking values like "33.67" or year-like numerics.
    text = re.sub(r"(?m)(^|\n)\s*(\d{1,2}\.)\s*(?=[A-Z])", r"\n\n\2 ", text)
    # Also split inline numbered markers like "...hour. 1. Speaking ..."
    text = re.sub(r"(?<!\d)([.!?])\s+(\d{1,2}\.)\s+(?=[A-Z])", r"\1\n\n\2 ", text)

    # Sentence-level wrapping for readability.
    text = re.sub(r"([.!?])\s+(?=[A-Z\"'])", r"\1\n", text)

    # Keep numbered sections separated by a blank line, while preserving
    # sentence-level line breaks inside each section.
    segments = re.split(r"(?m)(?=^\s*\d{1,2}\.\s+)", text)
    if len(segments) > 1:
        head = segments[0].strip()
        numbered = []
        for seg in segments[1:]:
            s = seg.strip()
            if not s:
                continue
            s = re.sub(r"[ \t]*\n[ \t]*", "\n", s).strip()
            s = re.sub(r"\n{3,}", "\n\n", s)
            numbered.append(s)
        text = (head + "\n\n" if head else "") + "\n\n".join(numbered)

    # Final cleanup.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def generate_report(episode: dict[str, Any], vocab_data: list[dict[str, Any]], cfg: dict[str, Any]) -> str:
    """Generate a single-file interactive HTML learning report.

    Args:
        episode: Episode metadata dictionary.
        vocab_data: Analyzer output list containing all dictionary fields.
        cfg: Runtime config dictionary.

    Returns:
        str: Saved report file path.
    """

    reports_dir = Path(_cfg_get(cfg, "paths.reports_dir", "reports"))
    reports_dir.mkdir(parents=True, exist_ok=True)

    date_display, date_compact = _normalize_date(str(episode.get("date", "")))
    airtime = str(episode.get("airtime", "21:55")) or "21:55"
    title = str(episode.get("title", "Arirang News"))
    mp3_filename = str(episode.get("mp3_filename", "") or "")
    mp3_url = str(episode.get("mp3_url", "") or "")
    script_text = _prettify_script_text(str(episode.get("script_text", "") or ""))

    report_filename = f"report_{date_compact}_2155.html"
    report_path = reports_dir / report_filename

    vocab_json = json.dumps(vocab_data, ensure_ascii=False)
    episode_json = json.dumps(
        {
            "date_display": date_display,
            "airtime": airtime,
            "title": title,
            "mp3_filename": mp3_filename,
            "mp3_url": mp3_url,
            "script_text": script_text,
        },
        ensure_ascii=False,
    )

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Arirang Learner Report</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --card: #ffffff;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #dbe1ea;
      --accent: #1e40af;
      --good: #15803d;
      --warn: #b45309;
      --bad: #b91c1c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: "Segoe UI", "Noto Sans KR", sans-serif;
    }}
    .container {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 20px;
      display: grid;
      gap: 16px;
    }}
    .panel {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
    }}
    .hero {{
      color: #fff;
      border: 0;
      background:
        radial-gradient(circle at 80% 20%, rgba(255,255,255,0.22), transparent 35%),
        linear-gradient(135deg, #1d4ed8 0%, #0f766e 55%, #0b132b 100%);
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 24px; }}
    .hero .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px 12px;
      font-size: 14px;
      opacity: 0.95;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(255,255,255,0.2);
      font-weight: 600;
    }}
    .header-row {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 10px;
    }}
    .cefr-chart {{
      display: flex;
      align-items: end;
      gap: 8px;
      min-height: 74px;
    }}
    .cefr-col {{
      width: 34px;
      text-align: center;
      font-size: 11px;
      color: #e2e8f0;
    }}
    .cefr-bar {{
      width: 100%;
      border-radius: 8px 8px 2px 2px;
      background: rgba(255,255,255,0.86);
      margin-bottom: 4px;
      min-height: 6px;
    }}
    .audio-wrap {{ display: block; }}
    audio {{ width: 100%; }}
    .script-box {{
      max-height: 400px;
      overflow: auto;
      white-space: pre-wrap;
      line-height: 1.7;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fbfdff;
    }}
    .script-box.expanded {{
      max-height: none;
    }}
    .script-tools {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
      gap: 8px;
    }}
    .script-hint {{
      font-size: 12px;
      color: var(--muted);
    }}
    .script-toggle {{
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      border-radius: 8px;
      padding: 6px 10px;
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
    }}
    mark {{
      background: transparent;
      cursor: pointer;
      padding: 0 1px;
      border-radius: 2px;
      transition: background 0.15s ease;
    }}
    mark:hover {{ background: rgba(59,130,246,0.12); }}
    .mark-b2 {{ border-bottom: 3px solid #facc15; }}
    .mark-c1 {{ border-bottom: 3px solid #fb923c; }}
    .mark-c2 {{ border-bottom: 3px solid #ef4444; }}
    .cards-head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 10px;
    }}
    .cards-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .v-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: #fff;
      position: relative;
    }}
    .v-card.bookmarked {{
      border: 2px solid #f59e0b;
      box-shadow: 0 0 0 2px rgba(245,158,11,0.15) inset;
    }}
    .card-top {{
      display: flex;
      align-items: baseline;
      gap: 8px;
      margin-bottom: 8px;
      padding-right: 28px;
    }}
    .word {{ font-size: 20px; font-weight: 700; }}
    .cefr {{
      font-size: 11px;
      font-weight: 700;
      border-radius: 999px;
      border: 1px solid var(--line);
      padding: 2px 8px;
    }}
    .phonetic {{ color: var(--muted); font-size: 13px; }}
    .sub {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .bookmark-btn {{
      position: absolute;
      right: 8px;
      top: 8px;
      border: 0;
      background: transparent;
      cursor: pointer;
      font-size: 18px;
      line-height: 1;
    }}
    .bookmark-btn[aria-pressed="true"] {{ filter: saturate(160%); }}
    .card-body {{ display: none; margin-top: 8px; }}
    .v-card.open .card-body {{ display: block; }}
    .def-box {{
      background: #f3f4f6;
      padding: 8px;
      border-radius: 8px;
      margin-bottom: 8px;
      font-size: 14px;
    }}
    .ko {{
      color: var(--good);
      font-weight: 600;
      margin-bottom: 8px;
      font-size: 14px;
    }}
    .example {{
      font-style: italic;
      color: #374151;
      margin-bottom: 8px;
      font-size: 14px;
    }}
    .context {{
      border-left: 4px solid #3b82f6;
      padding: 8px;
      background: #eff6ff;
      border-radius: 6px;
      margin-bottom: 8px;
      font-size: 14px;
    }}
    .tags {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }}
    .tag {{
      border: 1px solid var(--line);
      border-radius: 999px;
      font-size: 12px;
      padding: 2px 8px;
      color: #374151;
      background: #fff;
    }}
    .quiz-list {{ display: grid; gap: 12px; }}
    .q {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      background: #fff;
    }}
    .q .stem {{ margin-bottom: 8px; }}
    .choices {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
    }}
    .choice {{
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 8px;
      padding: 8px;
      text-align: left;
      cursor: pointer;
    }}
    .choice.correct {{ background: #dcfce7; border-color: #16a34a; }}
    .choice.wrong {{ background: #fee2e2; border-color: #ef4444; }}
    .q-result {{
      margin-top: 8px;
      font-size: 13px;
      color: #334155;
    }}
    .progress {{
      height: 10px;
      background: #e5e7eb;
      border-radius: 999px;
      overflow: hidden;
      margin-top: 8px;
    }}
    .progress > i {{
      display: block;
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, #16a34a, #22c55e);
      transition: width 0.2s ease;
    }}
    @media (max-width: 600px) {{
      .cards-grid {{ grid-template-columns: 1fr; }}
      .choices {{ grid-template-columns: 1fr; }}
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #0b1220;
        --card: #111827;
        --ink: #e5e7eb;
        --muted: #9ca3af;
        --line: #253041;
      }}
      .script-box {{ background: #0f172a; }}
      .def-box {{ background: #1f2937; }}
      .context {{ background: #0b2947; }}
      .choice {{ background: #0f172a; }}
      .q {{ background: #111827; }}
    }}
    @media print {{
      .audio-wrap, .quiz-section {{ display: none !important; }}
      .v-card .card-body {{ display: block !important; }}
      .container {{ max-width: 100%; padding: 0; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="panel hero">
      <h1 id="news-title"></h1>
      <div class="meta" id="news-meta"></div>
      <div class="header-row">
        <div style="display:flex; gap:8px; flex-wrap:wrap;">
          <span class="badge" id="vocab-count-badge"></span>
          <span class="badge" id="bookmark-count-badge">북마크 0개</span>
        </div>
        <div class="cefr-chart" id="cefr-chart"></div>
      </div>
    </section>

    <section class="panel audio-wrap" id="audio-section">
      <h2>Audio Player</h2>
      <audio controls id="audio-player"></audio>
    </section>

    <section class="panel">
      <h2>Script Viewer</h2>
      <div class="script-tools">
        <div class="script-hint">스크롤로 전체 확인 가능 · 필요 시 전체보기</div>
        <button id="script-toggle" class="script-toggle" type="button">전체보기</button>
      </div>
      <div class="script-box" id="script-box"></div>
    </section>

    <section class="panel">
      <div class="cards-head">
        <h2 style="margin:0;">Vocabulary Cards</h2>
      </div>
      <div class="cards-grid" id="cards-grid"></div>
    </section>

    <section class="panel quiz-section" id="quiz-section">
      <h2>빈칸 퀴즈</h2>
      <div id="quiz-score">0 / 0</div>
      <div class="progress"><i id="quiz-progress"></i></div>
      <div class="quiz-list" id="quiz-list" style="margin-top:12px;"></div>
    </section>
  </div>

  <script>
    const episode = {episode_json};
    const rawVocabData = {vocab_json};
    const vocabData = JSON.parse(JSON.stringify(rawVocabData));
    const bookmarksKey = "arirang_bookmarks";

    function escHtml(s) {{
      return String(s ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }}

    function cleanInlineArtifacts(s) {{
      let t = String(s ?? "");
      t = t.replace(/\\bdata-[a-z-]+\\s*=\\s*(?:'[^']*'|\"[^\"]*\"|’[^’]*’|[^\\s>]+)\\s*(?:>|&gt;)?/gi, "");
      t = t.replace(/data-[a-z-]+\\s*=\\s*(?:&#39;[^&#]*&#39;|&quot;[^&]*&quot;|[^\\s&]+)\\s*(?:&gt;)?/gi, "");
      t = t.replace(/<[^>]+>/g, "");
      return t;
    }}

    function loadBookmarks() {{
      try {{
        return JSON.parse(localStorage.getItem(bookmarksKey) || "{{}}");
      }} catch (_) {{
        return {{}};
      }}
    }}

    function saveBookmarks(bm) {{
      localStorage.setItem(bookmarksKey, JSON.stringify(bm));
    }}

    function cefrClass(level) {{
      if (level === "C2") return "mark-c2";
      if (level === "C1") return "mark-c1";
      if (level === "B2") return "mark-b2";
      return "";
    }}

    function openCard(lemma) {{
      const el = document.getElementById("card-" + lemma);
      if (!el) return;
      el.classList.add("open");
      el.scrollIntoView({{ behavior: "smooth", block: "center" }});
    }}

    function renderHeader() {{
      document.getElementById("news-title").textContent = episode.title || "Arirang News";
      document.getElementById("news-meta").innerHTML =
        "<span>" + escHtml(episode.date_display || "") + "</span>" +
        "<span>•</span>" +
        "<span>" + escHtml(episode.airtime || "21:55") + "</span>";

      document.getElementById("vocab-count-badge").textContent = "어휘 " + vocabData.length + "개";

      const dist = {{ B2:0, C1:0, C2:0 }};
      for (const v of vocabData) {{
        if (v.cefr_level === "B2") dist.B2++;
        if (v.cefr_level === "C1") dist.C1++;
        if (v.cefr_level === "C2") dist.C2++;
      }}
      const max = Math.max(1, dist.B2, dist.C1, dist.C2);
      const chart = document.getElementById("cefr-chart");
      chart.innerHTML = ["B2","C1","C2"].map(k => {{
        const h = Math.max(6, Math.round((dist[k] / max) * 56));
        return "<div class='cefr-col'>" +
               "<div class='cefr-bar' style='height:" + h + "px'></div>" +
               "<div>" + k + "</div>" +
               "</div>";
      }}).join("");
    }}

    function renderAudio() {{
      const section = document.getElementById("audio-section");
      const player = document.getElementById("audio-player");
      const bust = "?v=" + Date.now();
      if (!episode.mp3_filename && !episode.mp3_url) {{
        section.style.display = "none";
        return;
      }}
      if (episode.mp3_filename) {{
        // report file is in reports/, media files are in downloads/
        player.src = "../downloads/" + episode.mp3_filename + bust;
      }} else {{
        player.src = episode.mp3_url + (String(episode.mp3_url).includes("?") ? "&v=" : "?v=") + Date.now();
      }}
    }}

    function renderScript() {{
      const box = document.getElementById("script-box");
      const toggle = document.getElementById("script-toggle");
      let src = String(episode.script_text || "");
      // Defensive cleanup for raw/encoded inline attribute residue.
      src = src.replace(/\\bdata-[a-z-]+\\s*=\\s*(?:'[^']*'|\"[^\"]*\"|’[^’]*’|[^\\s>]+)\\s*(?:>|&gt;)?/gi, "");
      let text = escHtml(src);
      text = text.replace(/data-[a-z-]+\\s*=\\s*(?:&#39;[^&#]*&#39;|&quot;[^&]*&quot;|[^\\s&]+)\\s*(?:&gt;)?/gi, "");
      if (!text) {{
        box.textContent = "스크립트가 없습니다.";
        toggle.style.display = "none";
        return;
      }}

      const sorted = [...vocabData]
        .filter(v => v.word)
        .sort((a,b) => String(b.word).length - String(a.word).length);

      for (const item of sorted) {{
        const word = String(item.word || "").trim();
        if (!word) continue;
        const lemma = String(item.lemma || "").trim();
        const cls = cefrClass(item.cefr_level);
        const pattern = new RegExp("\\\\b(" + word.replace(/[.*+?^${{}}()|[\\\\]\\\\]/g, "\\\\$&") + ")\\\\b", "gi");
        text = text.replace(pattern, (m) => {{
          return "<mark class='" + cls + "' data-lemma='" + escHtml(lemma) + "'>" + escHtml(m) + "</mark>";
        }});
      }}

      box.innerHTML = text;
      box.querySelectorAll("mark[data-lemma]").forEach((el) => {{
        el.addEventListener("click", () => openCard(el.getAttribute("data-lemma")));
      }});

      toggle.addEventListener("click", () => {{
        box.classList.toggle("expanded");
        toggle.textContent = box.classList.contains("expanded") ? "접기" : "전체보기";
      }});
    }}

    function renderCards() {{
      const bm = loadBookmarks();
      const grid = document.getElementById("cards-grid");
      grid.innerHTML = vocabData.map((v) => {{
        const lemma = escHtml(v.lemma || "");
        const ko = String(v.translation_ko || "").trim() || "사전 미등록";
        const deriv = Array.isArray(v.derivatives) ? v.derivatives : [];
        const tags = deriv.map(x => "<span class='tag'>" + escHtml(x) + "</span>").join("");
        const bookmarked = bm[v.word] ? "bookmarked" : "";
        const pressed = bm[v.word] ? "true" : "false";
        const definition = cleanInlineArtifacts(v.definition_en || "");
        const example = cleanInlineArtifacts(v.example_en || "");
        const context = cleanInlineArtifacts(v.context_sentence || "");
        return "<article class='v-card " + bookmarked + "' id='card-" + lemma + "'>" +
               "<button class='bookmark-btn' data-word='" + escHtml(v.word) + "' aria-pressed='" + pressed + "'>🔖</button>" +
               "<div class='card-top'>" +
               "<span class='word'>" + escHtml(v.word || "") + "</span>" +
               "<span class='cefr'>" + escHtml(v.cefr_level || "") + "</span>" +
               "<span class='phonetic'>" + escHtml(v.phonetic || "") + "</span>" +
               "</div>" +
               "<div class='sub'>" + escHtml(v.pos_ko || "") + " · 빈도점수 " + escHtml(v.frequency_score) + "</div>" +
               "<div class='card-body'>" +
               "<div class='def-box'>" + escHtml(definition) + "</div>" +
               "<div class='ko'>" + escHtml(ko) + "</div>" +
               "<div class='example'>" + escHtml(example) + "</div>" +
               "<div class='context'>" + escHtml(context) + "</div>" +
               "<div class='tags'>" + tags + "</div>" +
               "</div>" +
               "</article>";
      }}).join("");

      grid.querySelectorAll(".v-card").forEach((card) => {{
        card.addEventListener("click", (ev) => {{
          if (ev.target && ev.target.classList.contains("bookmark-btn")) return;
          card.classList.toggle("open");
        }});
      }});

      grid.querySelectorAll(".bookmark-btn").forEach((btn) => {{
        btn.addEventListener("click", (ev) => {{
          ev.stopPropagation();
          const word = btn.getAttribute("data-word");
          const store = loadBookmarks();
          store[word] = !store[word];
          if (!store[word]) delete store[word];
          saveBookmarks(store);
          renderCards();
          updateBookmarkCount();
        }});
      }});
    }}

    function updateBookmarkCount() {{
      const bm = loadBookmarks();
      const n = Object.keys(bm).length;
      document.getElementById("bookmark-count-badge").textContent = "북마크 " + n + "개";
    }}

    function shuffle(arr) {{
      const a = [...arr];
      for (let i = a.length - 1; i > 0; i--) {{
        const j = Math.floor(Math.random() * (i + 1));
        [a[i], a[j]] = [a[j], a[i]];
      }}
      return a;
    }}

    function buildQuizItems() {{
      const withCtx = vocabData.filter(v => cleanInlineArtifacts(v.context_sentence || "").trim() && v.word);
      const picked = shuffle(withCtx).slice(0, 5);
      const words = vocabData.map(v => v.word).filter(Boolean);
      return picked.map((q, idx) => {{
        const answer = q.word;
        const wrongPool = shuffle(words.filter(w => w !== answer)).slice(0, 3);
        const choices = shuffle([answer, ...wrongPool]);
        const cleanCtx = cleanInlineArtifacts(q.context_sentence || "");
        const stem = cleanCtx.replace(new RegExp("\\\\b" + answer.replace(/[.*+?^${{}}()|[\\\\]\\\\]/g, "\\\\$&") + "\\\\b", "i"), "______");
        return {{
          id: idx + 1,
          answer,
          choices,
          stem,
          definition: q.definition_en || ""
        }};
      }});
    }}

    function renderQuiz() {{
      const list = document.getElementById("quiz-list");
      const scoreEl = document.getElementById("quiz-score");
      const progress = document.getElementById("quiz-progress");
      const items = buildQuizItems();
      if (!items.length) {{
        document.getElementById("quiz-section").style.display = "none";
        return;
      }}

      let answered = 0;
      let correct = 0;

      function refresh() {{
        scoreEl.textContent = correct + " / " + items.length;
        progress.style.width = Math.round((answered / items.length) * 100) + "%";
      }}

      list.innerHTML = items.map((q) => {{
        return "<div class='q' data-id='" + q.id + "'>" +
               "<div class='stem'><strong>Q" + q.id + ".</strong> " + escHtml(q.stem) + "</div>" +
               "<div class='choices'>" +
               q.choices.map(c => "<button class='choice' data-choice='" + escHtml(c) + "'>" + escHtml(c) + "</button>").join("") +
               "</div>" +
               "<div class='q-result'></div>" +
               "</div>";
      }}).join("");

      list.querySelectorAll(".q").forEach((qEl) => {{
        const id = Number(qEl.getAttribute("data-id"));
        const q = items.find(x => x.id === id);
        if (!q) return;
        let done = false;
        qEl.querySelectorAll(".choice").forEach((btn) => {{
          btn.addEventListener("click", () => {{
            if (done) return;
            done = true;
            answered++;
            const picked = btn.getAttribute("data-choice");
            const result = qEl.querySelector(".q-result");
            qEl.querySelectorAll(".choice").forEach((b) => {{
              const val = b.getAttribute("data-choice");
              if (val === q.answer) b.classList.add("correct");
            }});
            if (picked === q.answer) {{
              correct++;
              btn.classList.add("correct");
              result.textContent = "정답! " + (q.definition ? q.definition : "");
            }} else {{
              btn.classList.add("wrong");
              result.textContent = "오답. 정답: " + q.answer + (q.definition ? " | " + q.definition : "");
            }}
            refresh();
          }});
        }});
      }});

      refresh();
    }}

    renderHeader();
    renderAudio();
    renderScript();
    renderCards();
    updateBookmarkCount();
    renderQuiz();
  </script>
</body>
</html>
"""

    report_path.write_text(html_doc, encoding="utf-8")
    LOGGER.info("Saved report: %s", report_path)
    return str(report_path)

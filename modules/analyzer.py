"""Vocabulary analyzer for Arirang Learner.

Priority strategy:
1) NLTK WordNet (offline)
2) PyDictionary
3) Free Dictionary API (no-auth REST, best-effort)
4) Korean meaning from OMW (or empty)
"""

from __future__ import annotations

import csv
from datetime import datetime
import html
import json
import logging
import os
from pathlib import Path
import re
import time
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)

POS_KO_MAP = {
    "NOUN": "\uba85\uc0ac",
    "VERB": "\ub3d9\uc0ac",
    "ADJ": "\ud615\uc6a9\uc0ac",
    "ADV": "\ubd80\uc0ac",
}

# Hardcoded B2+ current-affairs vocabulary (100 words).
B2_PLUS_NEWS_WORDS = {
    "acquisition",
    "allegation",
    "amendment",
    "antitrust",
    "arbitration",
    "asylum",
    "austerity",
    "autonomous",
    "ballistic",
    "benchmark",
    "bilateral",
    "boycott",
    "bureaucracy",
    "censorship",
    "ceasefire",
    "coalition",
    "compliance",
    "concession",
    "consensus",
    "constitutional",
    "contingency",
    "controversy",
    "convene",
    "corruption",
    "credential",
    "cybersecurity",
    "declaration",
    "default",
    "delegation",
    "demographic",
    "deportation",
    "derivative",
    "diplomatic",
    "disinformation",
    "disruptive",
    "diversification",
    "embargo",
    "emission",
    "escalation",
    "evacuation",
    "exemption",
    "expansionary",
    "expenditure",
    "extradition",
    "faction",
    "federal",
    "fiscal",
    "fluctuation",
    "formulation",
    "friction",
    "geopolitical",
    "governance",
    "humanitarian",
    "immunity",
    "implementation",
    "incentive",
    "incumbent",
    "indictment",
    "inflation",
    "infrastructure",
    "injunction",
    "integration",
    "intervention",
    "jurisdiction",
    "legislation",
    "legitimacy",
    "liquidity",
    "litigation",
    "macroeconomic",
    "mandate",
    "mediation",
    "merger",
    "militant",
    "mobilization",
    "monetary",
    "moratorium",
    "multilateral",
    "negotiation",
    "oversight",
    "pandemic",
    "parliamentary",
    "peninsula",
    "plaintiff",
    "polarization",
    "procurement",
    "prosecution",
    "ratification",
    "recession",
    "referendum",
    "regulatory",
    "retaliatory",
    "sanction",
    "sovereignty",
    "stalemate",
    "subsidy",
    "surveillance",
    "tariff",
    "transparency",
    "unilateral",
    "volatile",
    "withdrawal",
}


def _cfg_get(cfg: dict[str, Any], path: str, default: Any) -> Any:
    cur: Any = cfg
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _sanitize_script_text(script_text: str) -> str:
    """Remove crawler/page markup residue before NLP analysis."""
    text = html.unescape((script_text or "").replace("\r\n", "\n").replace("\r", "\n"))
    if not text.strip():
        return ""
    # Remove leaked inline attrs such as data-lemma='word'> that appear as plain text.
    # Remove leaked inline attrs (quoted/unquoted, including smart quotes).
    text = re.sub(
        r"\bdata-[a-z-]+\s*=\s*(?:'[^']*'|\"[^\"]*\"|’[^’]*’|[^\s>]+)\s*(?:>|&gt;)?",
        "",
        text,
        flags=re.IGNORECASE,
    )
    # Remove any remaining HTML tags.
    text = re.sub(r"</?mark[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    # Normalize whitespace.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _load_dependencies() -> dict[str, Any]:
    deps: dict[str, Any] = {}

    try:
        import nltk  # type: ignore
        from nltk.corpus import wordnet as wn  # type: ignore
        from nltk.corpus import stopwords  # type: ignore

        deps["nltk"] = nltk
        deps["wn"] = wn
        deps["stopwords"] = stopwords
    except ImportError:
        LOGGER.warning("NLTK not installed. WordNet/stopwords features will be reduced.")
        deps["nltk"] = None
        deps["wn"] = None
        deps["stopwords"] = None

    try:
        import spacy  # type: ignore

        deps["spacy"] = spacy
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("spaCy unavailable (%s). Falling back to regex tokenization.", exc)
        deps["spacy"] = None

    try:
        from wordfreq import word_frequency  # type: ignore

        deps["word_frequency"] = word_frequency
    except ImportError:
        LOGGER.warning("wordfreq not installed. Using heuristic frequency fallback.")
        deps["word_frequency"] = None

    # Keep dictionary source policy deterministic:
    # WordNet (offline) -> Free Dictionary API -> blank.
    deps["PyDictionary"] = None

    return deps


def _ensure_nltk_data(nltk_mod: Any) -> None:
    if nltk_mod is None:
        return
    # Use project-local NLTK data dir to avoid permission issues on user profile paths.
    local_nltk_dir = Path(__file__).resolve().parents[1] / ".nltk_data"
    local_nltk_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("NLTK_DATA", str(local_nltk_dir))
    if str(local_nltk_dir) not in nltk_mod.data.path:
        nltk_mod.data.path.insert(0, str(local_nltk_dir))
    for pkg in ("wordnet", "omw-1.4", "stopwords"):
        try:
            nltk_mod.data.find(f"corpora/{pkg}")
        except Exception:
            try:
                nltk_mod.data.find(f"corpora/{pkg}.zip")
                continue
            except Exception:
                pass
            try:
                nltk_mod.download(pkg, quiet=True, download_dir=str(local_nltk_dir))
            except Exception:  # noqa: BLE001
                LOGGER.warning("Failed to download NLTK corpus: %s", pkg)


def _frequency_score(lemma: str, word_frequency_fn: Any) -> float:
    if word_frequency_fn is None:
        # fallback: longer words are treated as rarer.
        return max(1.0, min(8.0, len(lemma) / 1.2))

    freq = float(word_frequency_fn(lemma, "en"))
    if freq <= 0:
        return 8.0
    # Rare-word score = -log10(freq)
    import math

    return round(-math.log10(freq), 4)


def _estimate_cefr(score: float) -> str:
    if score >= 7.0:
        return "C2"
    if score >= 6.0:
        return "C1"
    if score >= 5.0:
        return "B2"
    if score >= 4.0:
        return "B1"
    if score >= 3.0:
        return "A2"
    return "A1"


def _get_wordnet_info(lemma: str, wn: Any) -> dict[str, Any]:
    out = {
        "definition_en": "",
        "example_en": "",
        "translation_ko": "",
        "derivatives": [],
    }
    if wn is None:
        return out

    try:
        synsets = wn.synsets(lemma)
    except Exception:  # noqa: BLE001
        synsets = []
    if synsets:
        first = synsets[0]
        out["definition_en"] = first.definition() or ""
        examples = first.examples() or []
        out["example_en"] = examples[0] if examples else ""

        derivs: set[str] = set()
        for syn in synsets[:5]:
            for l in syn.lemmas():
                derivs.add(l.name().replace("_", " "))
        derivs.discard(lemma)
        out["derivatives"] = sorted(derivs)[:15]

    # OMW korean lookup
    try:
        ko_synsets = wn.synsets(lemma, lang="kor")
    except Exception:  # noqa: BLE001
        ko_synsets = []
    if ko_synsets:
        try:
            ko_names = ko_synsets[0].lemma_names("kor")
        except Exception:  # noqa: BLE001
            ko_names = []
        if ko_names:
            out["translation_ko"] = ko_names[0].replace("_", " ")

    return out


def _lemma_candidates(word: str, lemma: str, wn: Any) -> list[str]:
    """Build retry candidates to reduce dictionary-miss cases."""
    seeds = [str(lemma or "").lower().strip(), str(word or "").lower().strip()]
    out: list[str] = []
    seen: set[str] = set()

    def push(x: str) -> None:
        t = x.strip().lower()
        if not t or t in seen:
            return
        seen.add(t)
        out.append(t)

    for s in seeds:
        push(s)

    # WordNet normalization candidates.
    if wn is not None:
        for s in list(out):
            for pos in ("n", "v", "a", "r"):
                try:
                    m = wn.morphy(s, pos=pos)
                except Exception:  # noqa: BLE001
                    m = None
                if m:
                    push(str(m))

    # Simple fallback morphology.
    for s in list(out):
        if s.endswith("ies") and len(s) > 4:
            push(s[:-3] + "y")
        if s.endswith("es") and len(s) > 3:
            push(s[:-2])
        if s.endswith("s") and len(s) > 3:
            push(s[:-1])
        if s.endswith("ing") and len(s) > 5:
            push(s[:-3])
            push(s[:-3] + "e")
        if s.endswith("ed") and len(s) > 4:
            push(s[:-2])
            push(s[:-1])

    return out


def _get_pydictionary_info(lemma: str, pydict_cls: Any) -> dict[str, str]:
    if pydict_cls is None:
        return {"definition_en": ""}
    try:
        pyd = pydict_cls()
        meaning = pyd.meaning(lemma)
        if not meaning:
            return {"definition_en": ""}
        for pos_key in ("Noun", "Verb", "Adjective", "Adverb"):
            defs = meaning.get(pos_key)
            if defs:
                return {"definition_en": str(defs[0])}
    except Exception:  # noqa: BLE001
        return {"definition_en": ""}
    return {"definition_en": ""}


def _get_free_dict_info(
    lemma: str,
    api_cache: dict[str, dict[str, str]],
    timeout_sec: int = 3,
) -> dict[str, str]:
    if lemma in api_cache:
        return api_cache[lemma]

    data = {"phonetic": "", "definition_en": "", "example_en": ""}
    url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{lemma}"
    try:
        with requests.Session() as session:
            session.trust_env = False
            resp = session.get(url, timeout=timeout_sec)
        if resp.ok:
            payload = resp.json()
            first = payload[0] if payload else {}
            phonetics = first.get("phonetics", [])
            if phonetics:
                data["phonetic"] = phonetics[0].get("text", "") or ""
            meanings = first.get("meanings", [])
            if meanings:
                defs = meanings[0].get("definitions", [])
                if defs:
                    data["definition_en"] = defs[0].get("definition", "") or ""
                    data["example_en"] = defs[0].get("example", "") or ""
    except Exception:  # noqa: BLE001
        # Silently ignore failures by design.
        pass

    api_cache[lemma] = data
    time.sleep(0.2)
    return data


def _extract_candidates_spacy(
    script_text: str,
    min_word_length: int,
    spacy_mod: Any,
    stopwords_mod: Any,
) -> list[dict[str, Any]]:
    try:
        nlp = spacy_mod.load("en_core_web_sm")
    except Exception:  # noqa: BLE001
        LOGGER.warning("spaCy model en_core_web_sm unavailable. Falling back to regex tokenization.")
        return []

    try:
        stop_words = set(stopwords_mod.words("english")) if stopwords_mod else set()
    except Exception:  # noqa: BLE001
        stop_words = set()

    doc = nlp(script_text)
    rows: list[dict[str, Any]] = []
    for sent in doc.sents:
        sent_text = sent.text.strip()
        for tok in sent:
            lemma = tok.lemma_.lower().strip()
            if not tok.is_alpha:
                continue
            if tok.pos_ == "PROPN":
                continue
            if tok.is_stop or lemma in stop_words:
                continue
            if len(lemma) < min_word_length:
                continue
            rows.append(
                {
                    "word": tok.text,
                    "lemma": lemma,
                    "pos": tok.pos_,
                    "context_sentence": sent_text,
                }
            )
    return rows


def _extract_candidates_regex(script_text: str, min_word_length: int) -> list[dict[str, Any]]:
    sent_split = [s.strip() for s in re.split(r"(?<=[.!?])\s+", script_text) if s.strip()]
    rows: list[dict[str, Any]] = []
    token_re = re.compile(r"\b[a-zA-Z][a-zA-Z'-]+\b")
    for sent in sent_split:
        for m in token_re.finditer(sent):
            word = m.group(0)
            lemma = word.lower()
            if len(lemma) < min_word_length:
                continue
            rows.append(
                {
                    "word": word,
                    "lemma": lemma,
                    "pos": "",
                    "context_sentence": sent,
                }
            )
    return rows


def analyze_vocabulary(script_text: str, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Analyze difficult vocabulary from script text.

    Rules:
    - Exclude stopwords, short tokens(<4 by default), non-alpha, PROPN.
    - Select candidates where frequency_score(-log10) >= 5.0.
    - Dictionary resolution: WordNet -> PyDictionary -> Free Dictionary API.
    - Korean translation: OMW only, else empty string.
    """

    script_text = _sanitize_script_text(script_text)
    deps = _load_dependencies()
    _ensure_nltk_data(deps["nltk"])

    min_word_length = int(_cfg_get(cfg, "vocabulary.min_word_length", 4))
    top_n_words = int(_cfg_get(cfg, "vocabulary.top_n_words", 30))

    candidates = _extract_candidates_spacy(
        script_text=script_text,
        min_word_length=min_word_length,
        spacy_mod=deps["spacy"],
        stopwords_mod=deps["stopwords"],
    )
    if not candidates:
        candidates = _extract_candidates_regex(script_text=script_text, min_word_length=min_word_length)

    by_lemma: dict[str, dict[str, Any]] = {}
    for c in candidates:
        lemma = c["lemma"]
        if lemma not in by_lemma:
            by_lemma[lemma] = c

    api_cache: dict[str, dict[str, str]] = {}
    results: list[dict[str, Any]] = []

    for lemma, base in by_lemma.items():
        score = _frequency_score(lemma, deps["word_frequency"])
        if score < 5.0:
            continue

        definition_en = ""
        example_en = ""
        translation_ko = ""
        derivatives: list[str] = []
        phonetic = ""

        candidates = _lemma_candidates(base.get("word", ""), lemma, deps["wn"])
        for cand in candidates:
            # 1) WordNet
            wn_info = _get_wordnet_info(cand, deps["wn"])
            if not definition_en:
                definition_en = wn_info["definition_en"]
            if not example_en:
                example_en = wn_info["example_en"]
            if not translation_ko:
                translation_ko = wn_info["translation_ko"]
            if not derivatives and wn_info["derivatives"]:
                derivatives = wn_info["derivatives"]

            # 2) PyDictionary fallback for definition
            if not definition_en:
                py_info = _get_pydictionary_info(cand, deps["PyDictionary"])
                definition_en = py_info.get("definition_en", "") or definition_en

            # 3) Free Dictionary API for phonetic/example/definition
            api_info = _get_free_dict_info(cand, api_cache=api_cache, timeout_sec=3)
            if not phonetic:
                phonetic = api_info.get("phonetic", "") or ""
            if not definition_en:
                definition_en = api_info.get("definition_en", "") or ""
            if not example_en:
                example_en = api_info.get("example_en", "") or ""

            # Stop early when all major fields are resolved.
            if definition_en and (example_en or phonetic or translation_ko):
                break

        pos = base.get("pos", "") or ""
        pos_ko = POS_KO_MAP.get(pos, "")
        row = {
            "word": base.get("word", lemma),
            "lemma": lemma,
            "pos": pos,
            "pos_ko": pos_ko,
            "phonetic": phonetic,
            "definition_en": definition_en,
            "translation_ko": translation_ko,
            "example_en": example_en,
            "context_sentence": base.get("context_sentence", ""),
            "cefr_level": _estimate_cefr(score),
            "frequency_score": round(score, 4),
            "is_b2_plus": lemma in B2_PLUS_NEWS_WORDS,
            "derivatives": derivatives,
        }
        results.append(row)

    results.sort(key=lambda x: (-x["frequency_score"], x["lemma"]))
    return results[:top_n_words]


def save_vocabulary(vocab_data: list[dict[str, Any]], date_str: str, cfg: dict[str, Any]) -> tuple[str, str]:
    """Save vocabulary data to JSON/CSV under logs directory.

    Files:
    - logs/vocabulary_YYYYMMDD.json
    - logs/vocabulary_YYYYMMDD.csv (utf-8-sig)
    """

    logs_dir = Path(_cfg_get(cfg, "paths.logs_dir", "logs"))
    logs_dir.mkdir(parents=True, exist_ok=True)

    json_path = logs_dir / f"vocabulary_{date_str}.json"
    csv_path = logs_dir / f"vocabulary_{date_str}.csv"

    payload = {
        "created_at": datetime.now().isoformat(),
        "count": len(vocab_data),
        "items": vocab_data,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "word",
        "lemma",
        "pos",
        "pos_ko",
        "phonetic",
        "definition_en",
        "translation_ko",
        "example_en",
        "context_sentence",
        "cefr_level",
        "frequency_score",
        "is_b2_plus",
        "derivatives",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in vocab_data:
            row = dict(item)
            row["derivatives"] = "|".join(item.get("derivatives", []))
            writer.writerow(row)

    LOGGER.info("Saved vocabulary json=%s csv=%s count=%d", json_path, csv_path, len(vocab_data))
    return str(json_path), str(csv_path)

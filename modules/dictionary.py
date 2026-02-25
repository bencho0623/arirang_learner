"""Dictionary utilities with strict source priority.

Priority:
1) NLTK WordNet/omw-1.4 (offline)
2) Free Dictionary API (no auth)
3) Empty string

Translation APIs (Google/DeepL/Papago) are intentionally not used.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

import requests
from nltk.corpus import wordnet as wn

DefinitionSource = Literal["wordnet", "free_dictionary_api", "none"]


@lru_cache(maxsize=4096)
def get_korean_meaning(word: str) -> str:
    """Return Korean meaning from WordNet OMW only; else empty string."""
    w = word.lower().strip()
    if not w:
        return ""

    try:
        synsets = wn.synsets(w)
    except LookupError:
        return ""

    for syn in synsets:
        try:
            kor_lemmas = syn.lemma_names("kor")
        except Exception:
            kor_lemmas = []
        if kor_lemmas:
            return kor_lemmas[0].replace("_", " ")
    return ""


@lru_cache(maxsize=4096)
def get_english_definition(word: str, timeout_sec: int = 4) -> tuple[str, DefinitionSource]:
    """Return English definition based on configured priority."""
    w = word.lower().strip()
    if not w:
        return "", "none"

    try:
        synsets = wn.synsets(w)
    except LookupError:
        synsets = []

    if synsets:
        return synsets[0].definition(), "wordnet"

    try:
        url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{w}"
        resp = requests.get(url, timeout=timeout_sec)
        if resp.ok:
            data = resp.json()
            for entry in data:
                for meaning in entry.get("meanings", []):
                    defs = meaning.get("definitions", [])
                    if defs and defs[0].get("definition"):
                        return defs[0]["definition"], "free_dictionary_api"
    except Exception:
        pass

    return "", "none"

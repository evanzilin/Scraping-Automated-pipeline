"""
Map raw ``industry`` / ``sub_industry`` to canonical taxonomy pairs.

Uses ``INDUSTRY_TAXONOMY`` from the repo-root ``industry_taxonomy`` module (single taxonomy source).
Logic aligns with ``validator_models.checks_icp`` pairing rules without importing that module
(to avoid optional Leadpoet / aiohttp side effects at import time).

Scoped classification (ICP / allowed taxonomies):

- :func:`resolve_taxonomy_sub_key_scope` — sub keys allowed under optional parent and/or sub lists.
- :func:`pick_sub_industry_from_definition_similarity` — best sub key by ``definition`` vs text (optional key subset).
- :func:`classify_industry_sub_from_description_in_scope` — full ``(industry, sub_industry)`` + error reason.
"""

from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher, get_close_matches
from typing import Collection, Dict, FrozenSet, List, Optional, Tuple

from industry_taxonomy import INDUSTRY_TAXONOMY

logger = logging.getLogger(__name__)

# Minimum confidence for fuzzy sub-industry match (same threshold as legacy checks_icp)
_SUB_FUZZY_MIN_CONFIDENCE = 0.5
_SUB_RESTRICTED_CUTOFF = 0.45
# Looser gates for last-chance sub mapping after ``normalize_industry_pair`` finds no pair.
_SUB_FALLBACK_GLOBAL_CONFIDENCE = 0.45
_SUB_FALLBACK_RESTRICTED_CUTOFF = 0.35
_SUB_FALLBACK_OVERLAP_MIN = 0.35

# Definition-based classification (same defaults as ``company_b2b_scrapingdog``).
_DEFAULT_DEFINITION_SIMILARITY_MIN = 0.6
_DEFAULT_DEFINITION_COMPARE_MAX_CHARS = 1200
_DEFAULT_DEFINITION_BRIEF_MIN_LEN = 16
_INDUSTRY_HINT_DEFINITION_BOOST = 0.05

# Free-text (e.g. LinkedIn) parent labels → closest ``industries`` string in ``INDUSTRY_TAXONOMY``.
_INDUSTRY_FUZZY_MIN_SCORE = 0.45
_INDUSTRY_STOPWORDS = frozenset(
    {
        "and",
        "or",
        "the",
        "of",
        "for",
        "to",
        "in",
        "a",
        "an",
        "services",
        "service",
        "other",
        "miscellaneous",
        "misc",
    }
)


def _normalize_industry_alias_key(s: str) -> str:
    """Lowercase, drop punctuation to a single space, collapse whitespace (for alias lookup)."""
    t = (s or "").strip().lower()
    t = re.sub(r"[^\w\s]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


# Known LinkedIn / vendor labels → exact ``industries`` string from ``INDUSTRY_TAXONOMY``.
_INDUSTRY_PARENT_ALIASES: Dict[str, str] = {
    _normalize_industry_alias_key("Technology, Information and Internet"): "Information Technology",
}


def _strip(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    t = str(s).strip()
    return t or None


def get_all_valid_industries() -> frozenset[str]:
    """All parent industry labels appearing under any sub-industry key."""
    industries: set[str] = set()
    for meta in INDUSTRY_TAXONOMY.values():
        for g in meta.get("industries") or []:
            if str(g).strip():
                industries.add(str(g).strip())
    return frozenset(industries)


def _exact_sub_key(raw: str) -> Optional[str]:
    rl = raw.strip().lower()
    for key in INDUSTRY_TAXONOMY:
        if key.lower() == rl:
            return key
    return None


def fuzzy_match_sub_industry(claimed_sub_industry: str) -> Tuple[Optional[str], Optional[dict], float]:
    """
    Fuzzy-match free text to a taxonomy sub-industry key.

    Returns ``(matched_key, taxonomy_entry, confidence)``.
    """
    if not claimed_sub_industry:
        return None, None, 0.0

    claimed_lower = claimed_sub_industry.strip().lower()

    for key in INDUSTRY_TAXONOMY:
        if key.lower() == claimed_lower:
            return key, INDUSTRY_TAXONOMY[key], 1.0

    best_match: Optional[str] = None
    best_confidence = 0.0

    for key in INDUSTRY_TAXONOMY:
        key_lower = key.lower()

        if claimed_lower in key_lower or key_lower in claimed_lower:
            longer = max(len(claimed_lower), len(key_lower)) or 1
            shorter = min(len(claimed_lower), len(key_lower))
            confidence = shorter / longer
            if confidence > best_confidence:
                best_match = key
                best_confidence = confidence

        claimed_words = set(claimed_lower.replace("-", " ").replace("/", " ").split())
        key_words = set(key_lower.replace("-", " ").replace("/", " ").split())
        if claimed_words and key_words:
            overlap = len(claimed_words & key_words)
            total = len(claimed_words | key_words)
            word_confidence = overlap / total if total > 0 else 0.0
            if word_confidence > best_confidence:
                best_match = key
                best_confidence = word_confidence

    if best_match and best_confidence >= _SUB_FUZZY_MIN_CONFIDENCE:
        return best_match, INDUSTRY_TAXONOMY[best_match], best_confidence

    return None, None, 0.0


def validate_industry_sub_industry_pairing(claimed_industry: str, matched_sub_industry: str) -> Tuple[bool, str]:
    """Return whether ``claimed_industry`` is a valid parent for ``matched_sub_industry``."""
    if not matched_sub_industry or matched_sub_industry not in INDUSTRY_TAXONOMY:
        return False, f"Sub-industry '{matched_sub_industry}' not in taxonomy"

    valid_groups: List[str] = list(INDUSTRY_TAXONOMY[matched_sub_industry].get("industries") or [])
    if not valid_groups:
        return True, "no specific industry group requirements"

    claimed_lower = (claimed_industry or "").strip().lower()
    if not claimed_lower:
        return False, "empty industry"

    for group in valid_groups:
        gl = group.lower()
        if gl == claimed_lower:
            return True, "exact industry match"
        if claimed_lower in gl or gl in claimed_lower:
            return True, "loose industry match"

    return False, f"industry not in valid groups: {valid_groups}"


def _canonical_group_for_sub(sub_key: str, claimed_industry: Optional[str]) -> Optional[str]:
    """Pick the taxonomy industry string for a fixed sub key."""
    groups = list(INDUSTRY_TAXONOMY[sub_key].get("industries") or [])
    if not groups:
        return claimed_industry
    if not claimed_industry:
        return groups[0]
    ok, _ = validate_industry_sub_industry_pairing(claimed_industry, sub_key)
    if ok:
        cl = claimed_industry.strip().lower()
        for g in groups:
            gl = g.lower()
            if gl == cl or cl in gl or gl in cl:
                return g
        return groups[0]
    close = get_close_matches(claimed_industry, groups, n=1, cutoff=0.35)
    if close:
        return close[0]
    return groups[0]


def _subs_for_industry_group(canonical_industry: str) -> List[str]:
    """Sub-industry keys whose parent list includes this industry (case-insensitive)."""
    il = canonical_industry.strip().lower()
    out: List[str] = []
    for key, meta in INDUSTRY_TAXONOMY.items():
        for g in meta.get("industries") or []:
            if g.lower() == il or il in g.lower() or g.lower() in il:
                out.append(key)
                break
    return out


def _industry_word_tokens(s: str) -> set[str]:
    t = re.sub(r"[^\w\s]+", " ", (s or "").lower())
    return {w for w in t.split() if w and w not in _INDUSTRY_STOPWORDS}


def _fuzzy_match_industry_parent(raw: str) -> Optional[str]:
    """
    Map a noisy parent label (LinkedIn-style) to the closest taxonomy ``industries`` string.
    Uses substring overlap and token Jaccard similarity against all distinct parents.
    """
    if not raw or not str(raw).strip():
        return None
    raw_s = str(raw).strip()
    rl = raw_s.lower()
    all_i = sorted(get_all_valid_industries())
    best_score = 0.0
    best: List[str] = []
    for g in all_i:
        gl = g.lower()
        if gl == rl:
            return g
        substr_score = 0.0
        if gl in rl or rl in gl:
            longer = max(len(rl), len(gl)) or 1
            shorter = min(len(rl), len(gl))
            substr_score = shorter / longer
        rw = _industry_word_tokens(raw_s)
        gw = _industry_word_tokens(g)
        jaccard = 0.0
        if rw and gw:
            jaccard = len(rw & gw) / len(rw | gw)
        score = max(substr_score, jaccard)
        if score > best_score:
            best_score = score
            best = [g]
        elif score == best_score and score > 0:
            best.append(g)
    if best_score >= _INDUSTRY_FUZZY_MIN_SCORE and best:
        return sorted(set(best))[0]
    return None


def _linkedin_style_it_parent(stripped: str) -> Optional[str]:
    """Map common LinkedIn IT umbrella strings to the taxonomy parent ``Information Technology``."""
    rl = stripped.lower()
    if not re.search(r"\bit\b", rl) and "information technology" not in rl:
        return None
    if not any(
        x in rl
        for x in (
            "service",
            "consult",
            "software",
            "hardware",
            "solution",
            "tech",
            "saas",
            "cloud",
            "data",
            "cyber",
            "network",
        )
    ):
        return None
    valid = get_all_valid_industries()
    if "Information Technology" in valid:
        return "Information Technology"
    return None


def _resolve_industry_to_canonical(raw: Optional[str]) -> Optional[str]:
    """Map free-text industry to exact taxonomy parent label if possible."""
    if not raw:
        return None
    all_i = sorted(get_all_valid_industries())
    stripped = raw.strip()
    # exact case-insensitive
    rl = stripped.lower()
    for g in all_i:
        if g.lower() == rl:
            return g
    alias_target = _INDUSTRY_PARENT_ALIASES.get(_normalize_industry_alias_key(stripped))
    if alias_target:
        valid = get_all_valid_industries()
        if alias_target in valid:
            return alias_target
    # Token / substring fit against taxonomy parents (before difflib: difflib can spuriously
    # match unrelated long strings at moderate cutoffs, e.g. LinkedIn labels vs "Travel and Tourism").
    fuzzy = _fuzzy_match_industry_parent(stripped)
    if fuzzy:
        return fuzzy
    close = get_close_matches(stripped, all_i, n=1, cutoff=0.6)
    if close:
        return close[0]
    it_parent = _linkedin_style_it_parent(stripped)
    if it_parent:
        return it_parent
    return None


def normalize_industry_pair(
    industry: Optional[str],
    sub_industry: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Resolve ``(industry, sub_industry)`` to canonical taxonomy strings.

    Returns ``(industry_out, sub_out, reject_reason)``.
    If ``reject_reason`` is not None, the record should be skipped.
    """
    ind = _strip(industry)
    sub = _strip(sub_industry)

    # 1) Exact or fuzzy sub-industry first
    sub_key: Optional[str] = None
    if sub:
        sub_key = _exact_sub_key(sub)
        if not sub_key:
            sk, _meta, conf = fuzzy_match_sub_industry(sub)
            if sk and conf >= _SUB_FUZZY_MIN_CONFIDENCE:
                sub_key = sk

    if sub_key:
        canon_ind = _canonical_group_for_sub(sub_key, ind)
        if not canon_ind:
            return None, None, "could not resolve industry for matched sub_industry"
        logger.debug("Industry normalize: via sub_industry -> %s / %s", canon_ind, sub_key)
        return canon_ind, sub_key, None

    # 2) Valid industry, map sub within that industry
    if ind:
        canon_ind = _resolve_industry_to_canonical(ind)
        if canon_ind and sub:
            candidates = _subs_for_industry_group(canon_ind)
            if candidates:
                close = get_close_matches(sub.strip(), candidates, n=1, cutoff=_SUB_RESTRICTED_CUTOFF)
                if close:
                    sub_key = close[0]
                    ok, _ = validate_industry_sub_industry_pairing(canon_ind, sub_key)
                    if ok:
                        logger.debug("Industry normalize: restricted fuzzy sub -> %s / %s", canon_ind, sub_key)
                        return canon_ind, sub_key, None

        # 3) Swapped fields: treat industry string as sub-industry candidate
        sk2, _m, conf2 = fuzzy_match_sub_industry(ind)
        if sk2 and conf2 >= _SUB_FUZZY_MIN_CONFIDENCE:
            canon2 = _canonical_group_for_sub(sk2, sub or ind)
            if canon2:
                logger.debug("Industry normalize: swapped field heuristic -> %s / %s", canon2, sk2)
                return canon2, sk2, None

    reason = f"unresolved industry/sub_industry pair raw=({ind!r}, {sub!r})"
    logger.info("Industry normalize reject: %s", reason)
    return None, None, reason


def refine_raw_industry_sub_taxonomy_fallback(
    raw_industry: Optional[str],
    raw_sub_industry: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Map raw ``(industry, sub_industry)`` toward ``INDUSTRY_TAXONOMY`` when
    ``normalize_industry_pair`` / candidate pairing did not yield a canonical pair.

    Uses the same taxonomy as ``normalize_industry_pair``: resolve a canonical parent from
    ``raw_industry``, then match ``raw_sub_industry`` to allowed sub keys under that parent
    (``get_close_matches`` with a lower cutoff, then token/substring overlap). If that fails,
    applies ``fuzzy_match_sub_industry`` globally with a slightly lower confidence floor.
    Returns ``(industry_out, sub_out)`` using taxonomy keys when possible; otherwise the
    trimmed raw strings.
    """
    ind = _strip(raw_industry)
    sub = _strip(raw_sub_industry)
    if not ind and not sub:
        return None, None
    if not sub:
        ci = _resolve_industry_to_canonical(ind) if ind else None
        return ci or ind, None

    canon_i = _resolve_industry_to_canonical(ind) if ind else None

    if canon_i:
        candidates = _subs_for_industry_group(canon_i)
        if candidates:
            close = get_close_matches(
                sub.strip(), candidates, n=1, cutoff=_SUB_FALLBACK_RESTRICTED_CUTOFF
            )
            if close:
                sk = close[0]
                if validate_industry_sub_industry_pairing(canon_i, sk)[0]:
                    logger.debug(
                        "Industry sub fallback: restricted close -> %s / %s", canon_i, sk
                    )
                    return canon_i, sk
            sl = sub.strip().lower()
            best_sk: Optional[str] = None
            best_score = 0.0
            for cand in candidates:
                cl = cand.lower()
                if sl == cl:
                    return canon_i, cand
                sw = set(sl.replace("-", " ").replace("/", " ").split())
                cw = set(cl.replace("-", " ").replace("/", " ").split())
                if sw and cw:
                    overlap = len(sw & cw) / len(sw | cw)
                    if overlap > best_score:
                        best_score = overlap
                        best_sk = cand
                elif sl in cl or cl in sl:
                    longer = max(len(sl), len(cl)) or 1
                    shorter = min(len(sl), len(cl))
                    score = shorter / longer
                    if score > best_score:
                        best_score = score
                        best_sk = cand
            if best_sk and best_score >= _SUB_FALLBACK_OVERLAP_MIN:
                if validate_industry_sub_industry_pairing(canon_i, best_sk)[0]:
                    logger.debug(
                        "Industry sub fallback: overlap -> %s / %s", canon_i, best_sk
                    )
                    return canon_i, best_sk

    sk_g, _meta, conf = fuzzy_match_sub_industry(sub)
    if sk_g and conf >= _SUB_FALLBACK_GLOBAL_CONFIDENCE:
        ci2 = _canonical_group_for_sub(sk_g, canon_i or ind)
        if ci2:
            logger.debug("Industry sub fallback: global fuzzy -> %s / %s", ci2, sk_g)
            return ci2, sk_g

    return ind, sub


def _sub_keys_under_parent_labels(canonical_parents: FrozenSet[str]) -> FrozenSet[str]:
    """Sub-industry keys whose ``industries`` list includes any of ``canonical_parents`` (exact)."""
    if not canonical_parents:
        return frozenset()
    keys: set[str] = set()
    for sk, meta in INDUSTRY_TAXONOMY.items():
        for g in meta.get("industries") or []:
            gs = str(g).strip()
            if gs in canonical_parents:
                keys.add(sk)
                break
    return frozenset(keys)


def _resolve_allowed_sub_keys(allowed_sub: Collection[str]) -> FrozenSet[str]:
    """Map free-text sub labels to exact ``INDUSTRY_TAXONOMY`` keys."""
    keys: set[str] = set()
    for raw in allowed_sub:
        s = (raw or "").strip()
        if not s:
            continue
        ek = _exact_sub_key(s)
        if ek:
            keys.add(ek)
            continue
        sk, _m, conf = fuzzy_match_sub_industry(s)
        if sk and conf >= _SUB_FUZZY_MIN_CONFIDENCE:
            keys.add(sk)
    return frozenset(keys)


def _canonical_parent_labels_from_scope(raw_parents: Collection[str]) -> FrozenSet[str]:
    """Normalize scope parent strings to exact taxonomy ``industries`` labels."""
    out: set[str] = set()
    valid = get_all_valid_industries()
    for p in raw_parents:
        s = (p or "").strip()
        if not s:
            continue
        c = _resolve_industry_to_canonical(s)
        if c and c in valid:
            out.add(c)
            continue
        sl = s.lower()
        for g in valid:
            if g.lower() == sl:
                out.add(g)
                break
    return frozenset(out)


def resolve_taxonomy_sub_key_scope(
    *,
    allowed_sub_industries: Optional[Collection[str]] = None,
    allowed_parent_industries: Optional[Collection[str]] = None,
) -> FrozenSet[str]:
    """
    Sub-industry keys to consider when classifying inside a restricted taxonomy scope.

    - If both arguments are ``None``, returns all keys in ``INDUSTRY_TAXONOMY``.
    - If only ``allowed_sub_industries`` is set, returns those entries resolved to exact keys
      (unknown strings are skipped).
    - If only ``allowed_parent_industries`` is set, returns every sub key that lists at least
      one of the resolved canonical parents in ``industries``.
    - If both are set, returns the **intersection** (sub must match both constraints).
    """
    all_keys = frozenset(INDUSTRY_TAXONOMY.keys())
    if not allowed_sub_industries and not allowed_parent_industries:
        return all_keys

    subs: Optional[FrozenSet[str]] = None
    if allowed_sub_industries:
        subs = _resolve_allowed_sub_keys(list(allowed_sub_industries))
        subs = frozenset(k for k in subs if k in INDUSTRY_TAXONOMY)

    parents_keys: Optional[FrozenSet[str]] = None
    if allowed_parent_industries:
        pl = _canonical_parent_labels_from_scope(list(allowed_parent_industries))
        parents_keys = _sub_keys_under_parent_labels(pl) if pl else frozenset()

    if subs is not None and parents_keys is not None:
        return frozenset(subs & parents_keys)
    if subs is not None:
        return subs
    if parents_keys is not None:
        return parents_keys
    return all_keys


def pick_sub_industry_from_definition_similarity(
    text: Optional[str],
    industry_hint: Optional[str] = None,
    *,
    scope_sub_keys: Optional[Collection[str]] = None,
    min_ratio: float = _DEFAULT_DEFINITION_SIMILARITY_MIN,
    min_brief_len: int = _DEFAULT_DEFINITION_BRIEF_MIN_LEN,
    max_compare_chars: int = _DEFAULT_DEFINITION_COMPARE_MAX_CHARS,
) -> Optional[str]:
    """
    Pick the taxonomy sub-industry key whose ``definition`` best matches ``text``
    (``SequenceMatcher`` ratio ``>= min_ratio``).

    If ``scope_sub_keys`` is set, only those keys (must exist in ``INDUSTRY_TAXONOMY``) are
    scored. When ``industry_hint`` loosely matches a sub's parent ``industries`` entry, the
    score is nudged upward slightly (same behavior as B2B enrichment).
    """
    if not (text or "").strip():
        return None
    brief_n = re.sub(r"\s+", " ", (text or "").strip().lower())[:max_compare_chars]
    if len(brief_n) < min_brief_len:
        return None

    if scope_sub_keys is not None:
        scope = frozenset(k for k in scope_sub_keys if k in INDUSTRY_TAXONOMY)
        if not scope:
            return None
    else:
        scope = frozenset(INDUSTRY_TAXONOMY.keys())

    hint_l = (industry_hint or "").strip().lower()
    best_key: Optional[str] = None
    best_score = 0.0

    for sub_key in scope:
        meta = INDUSTRY_TAXONOMY.get(sub_key) or {}
        defn = (meta.get("definition") or "").strip()
        if not defn:
            continue
        defn_n = re.sub(r"\s+", " ", defn.lower())[:max_compare_chars]
        r = SequenceMatcher(None, brief_n, defn_n).ratio()
        if hint_l:
            for p in meta.get("industries") or []:
                pl = str(p).lower()
                if not pl:
                    continue
                if hint_l == pl or hint_l in pl or pl in hint_l:
                    r = min(1.0, r + _INDUSTRY_HINT_DEFINITION_BOOST)
                    break
        if r > best_score:
            best_score = r
            best_key = sub_key

    if best_key is not None and best_score >= min_ratio:
        return best_key
    return None


def classify_industry_sub_from_description_in_scope(
    description: Optional[str],
    *,
    allowed_sub_industries: Optional[Collection[str]] = None,
    allowed_parent_industries: Optional[Collection[str]] = None,
    industry_hint: Optional[str] = None,
    min_definition_similarity: float = _DEFAULT_DEFINITION_SIMILARITY_MIN,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Classify to a canonical ``(industry, sub_industry)`` from free text, restricted to a taxonomy scope.

    Scope is the intersection of optional ``allowed_sub_industries`` and ``allowed_parent_industries``
    (see :func:`resolve_taxonomy_sub_key_scope`). The best sub key is chosen by comparing
    ``description`` to each candidate's ``definition`` in ``INDUSTRY_TAXONOMY``.

    Returns
    -------
    (industry, sub_industry, error_reason)
        On success ``error_reason`` is ``None``. Otherwise strings are ``None`` and ``error_reason``
        explains the failure (e.g. empty scope, text too short, no candidate above threshold).
    """
    scope = resolve_taxonomy_sub_key_scope(
        allowed_sub_industries=allowed_sub_industries,
        allowed_parent_industries=allowed_parent_industries,
    )
    if not scope:
        return None, None, "empty_taxonomy_scope"

    if not (description or "").strip():
        return None, None, "missing_description"

    brief_n = re.sub(r"\s+", " ", (description or "").strip().lower())[
        :_DEFAULT_DEFINITION_COMPARE_MAX_CHARS
    ]
    if len(brief_n) < _DEFAULT_DEFINITION_BRIEF_MIN_LEN:
        return None, None, "description_too_short"

    sub_key = pick_sub_industry_from_definition_similarity(
        description,
        industry_hint,
        scope_sub_keys=scope,
        min_ratio=min_definition_similarity,
    )
    if not sub_key:
        return None, None, "no_sub_industry_above_similarity_threshold"

    canon = _canonical_group_for_sub(sub_key, industry_hint)
    if not canon:
        return None, None, "could_not_resolve_canonical_industry"
    return canon, sub_key, None


def coerce_industry_sub_to_taxonomy_scope(
    industry: Optional[str],
    sub_industry: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Align ``(industry, sub_industry)`` with ``INDUSTRY_TAXONOMY`` spelling and parent lists.

    When ``sub_industry`` resolves to a taxonomy sub key (exact or fuzzy at the usual sub
    threshold), returns the canonical parent from ``_canonical_group_for_sub`` and the
    exact sub key spelling from the taxonomy. Otherwise refines ``industry`` via
    ``_resolve_industry_to_canonical`` when possible and returns the trimmed raw sub.
    """
    ind = _strip(industry)
    sub = _strip(sub_industry)
    if not ind and not sub:
        return None, None

    sub_key: Optional[str] = None
    if sub:
        sub_key = _exact_sub_key(sub)
        if not sub_key:
            sk, _meta, conf = fuzzy_match_sub_industry(sub)
            if sk and conf >= _SUB_FUZZY_MIN_CONFIDENCE:
                sub_key = sk

    if sub_key:
        canon = _canonical_group_for_sub(sub_key, ind)
        if canon:
            return canon, sub_key
        return ind, sub_key

    ci = _resolve_industry_to_canonical(ind) if ind else None
    return ci or ind, sub

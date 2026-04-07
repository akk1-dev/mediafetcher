#!/usr/bin/env python3
"""
fetcher.py — Scan media folders and fetch TMDB/IMDb metadata.

For every entry in one or more media folders the script:
  1. Applies any prefix override from json/overrides.json
  2. Cleans the folder/file name (strips codec/lang/release tags per
     json/cleaning.json, peels release-group suffixes, detects the year)
  3. Searches TMDB (with year filter when available) and resolves an IMDb ID
  4. Fetches the IMDb record from OMDb by that ID (with title-search fallback)
  5. Optionally scans the disk for episodes and diffs against TMDB's season
     list to build a missing-episodes report (--check-episodes)
  6. Writes records to json/serien.json or json/filme.json for the HTML viewer
     in html/, plus scan/error/missing/complete logs to logs/

Workers run in a ThreadPoolExecutor with a per-host token-bucket rate limiter
and retry-on-5xx. A warm cache (existing output JSON) makes re-runs instant;
use --force to invalidate. Quota-exhausted OMDb (HTTP 401) is handled
fail-soft: the host is disabled for the rest of the run and records fall back
to TMDB-only without losing progress. On the next run (no --force needed),
the cache fast-path retries OMDb only for previously-unrated entries via a
single cheap call each — so libraries that hit the OMDb daily limit
self-heal across runs without re-doing the expensive TMDB search.

Configuration (all next to this script):
  .env              — TMDB_API_KEY and OMDB_API_KEY (unquoted KEY=value pairs)
  json/cleaning.json — codec/release/language/TLD tags to strip from names
  json/overrides.json — folder-prefix → (title, year[, imdb_id]) overrides

API keys (free tiers):
  - TMDB: https://www.themoviedb.org/settings/api
  - OMDb: https://www.omdbapi.com/apikey.aspx  (1000 req/day)

Usage:
    # Single folder, series mode (default)
    python3 fetcher.py "/media/veracrypt5/Serien"

    # Multiple folders merged into one JSON, movie mode
    python3 fetcher.py "/media/veracrypt3/Movies 1080p" \\
                       "/media/veracrypt1/Movies 4K" --type movie

    # Force cache rebuild + verify episodes against TMDB
    python3 fetcher.py "/media/veracrypt5/Serien" \\
                       --type series --check-episodes --force

    # Mixed folder (movies + series), TMDB multi-search with year post-filter
    python3 fetcher.py /media/mixed --type auto

Flags:
    --type {series,movie,auto}   media type (default: series)
    --check-episodes             diff on-disk episodes against TMDB seasons
    --force                      ignore cache and re-scan every entry
    --output <path>              override default output JSON path
    --tmdb-key / --omdb-key      override env vars / .env values

"""

import os
import sys
import json
import time
import re
import argparse
import threading
import urllib.request
import urllib.parse
import urllib.error
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "YOUR_TMDB_API_KEY")
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "YOUR_OMDB_API_KEY")
OUTPUT_FILE   = "json/serien.json"  # overridden at runtime based on --type

# Parallelism / rate limiting
MAX_WORKERS   = 6       # concurrent per-entry workers
HTTP_TIMEOUT  = 15      # seconds per HTTP call
HTTP_RETRIES  = 3       # retry count for transient failures
HTTP_BACKOFF  = 0.5     # initial backoff seconds (doubled per retry)
# TMDB allows ~40 req/sec; OMDb ~10/sec in practice. Keep a safe ceiling.
_RATE_LIMITS = {
    "api.themoviedb.org": 20.0,   # requests per second
    "www.omdbapi.com":     8.0,
}

SEASON_DIR_PATTERN = re.compile(r'[Ss](?:eason\s*)?(\d{1,2})', re.IGNORECASE)


# ---------------------------------------------------------------------------
# HTTP: rate-limited, retrying, error-aware
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Simple per-host token bucket. Thread-safe.

    Each host has a max RPS — callers block in acquire() until enough time
    has passed since the previous call on that host.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._last = {}   # host -> last-call monotonic timestamp

    def acquire(self, host: str) -> None:
        rps = _RATE_LIMITS.get(host, 5.0)
        min_interval = 1.0 / rps
        with self._lock:
            now  = time.monotonic()
            prev = self._last.get(host, 0.0)
            wait = (prev + min_interval) - now
            # Reserve the slot before releasing the lock so that concurrent
            # threads for the same host queue up rather than racing.
            self._last[host] = max(now, prev + min_interval)
        if wait > 0:
            time.sleep(wait)

_RATE = _RateLimiter()


# Hosts that returned 401 — we stop calling them for the rest of the run.
# Quota-exhaustion (the common 401 case) should not kill the whole scan;
# the script degrades to using whatever other APIs still work.
_DISABLED_HOSTS: set = set()
_DISABLED_HOSTS_LOCK = threading.Lock()


def _disable_host(host: str, reason: str) -> None:
    with _DISABLED_HOSTS_LOCK:
        if host not in _DISABLED_HOSTS:
            _DISABLED_HOSTS.add(host)
            print(
                f"\n  [FATAL for {host}] {reason}\n"
                f"  → Disabling {host} for the rest of this run. "
                f"Other lookups will continue.\n",
                file=sys.stderr,
            )


def http_get_json(url: str, context: str = "") -> dict | None:
    """Fetch JSON from `url` with rate limiting and retry.

    Returns the parsed dict on success, None on any 4xx / persistent failure.
    Retries on timeout / 5xx / URLError with exponential backoff.

    On 401, the entire host is disabled for the rest of the process so that
    quota-exhausted OMDb doesn't kill an otherwise-working TMDB scan.
    """
    host = urllib.parse.urlparse(url).netloc
    with _DISABLED_HOSTS_LOCK:
        if host in _DISABLED_HOSTS:
            return None

    last_err: Exception | None = None
    for attempt in range(HTTP_RETRIES):
        _RATE.acquire(host)
        try:
            with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 401:
                _disable_host(host, f"HTTP 401 Unauthorized — bad API key or daily quota exhausted")
                return None
            if 400 <= e.code < 500:
                return None   # legitimate "not found" / bad request
            last_err = e       # 5xx: retry
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
        except Exception as e:
            last_err = e

        if attempt < HTTP_RETRIES - 1:
            time.sleep(HTTP_BACKOFF * (2 ** attempt))

    if last_err and context:
        print(f"  [WARN] HTTP fail ({context}): {last_err}", file=sys.stderr)
    elif last_err:
        print(f"  [WARN] HTTP fail for {url}: {last_err}", file=sys.stderr)
    return None

# ---------------------------------------------------------------------------
# Title overrides — loaded from overrides.json (next to this script).
# Format in JSON: "Folder-Prefix": ["Search Title", "Year"]
# ---------------------------------------------------------------------------
def _load_overrides() -> dict:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json/overrides.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    merged = {}
    for section, entries in data.items():
        if section.startswith("_"):
            continue
        for key, val in entries.items():
            merged[key] = tuple(val)
    return merged

_TITLE_OVERRIDES: dict[str, tuple] = _load_overrides()
# Pre-sorted by key length descending so the LONGEST matching prefix wins
# (e.g. "Star Trek Discovery" before "Star Trek").
_OVERRIDES_BY_LENGTH: list = sorted(
    _TITLE_OVERRIDES.items(), key=lambda kv: len(kv[0]), reverse=True
)

# ---------------------------------------------------------------------------
# Name cleaning — config loaded from json/cleaning.json
# ---------------------------------------------------------------------------

def _load_cleaning_config() -> dict:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json", "cleaning.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)

_CLEANING = _load_cleaning_config()

VIDEO_EXTS = set(_CLEANING.get("video_extensions", [
    '.mkv', '.mp4', '.avi', '.m4v', '.ts', '.mov', '.wmv', '.flv', '.mpg', '.mpeg'
]))

# Sentinel that never matches anything — used when a config list is empty,
# so an accidentally-empty list doesn't turn into a match-everything regex.
_NEVER_MATCH = re.compile(r'(?!x)x')

def _build_tag_pattern() -> re.Pattern:
    tags = (
        _CLEANING.get("codec_tags", []) +
        _CLEANING.get("release_tags", [])
    )
    if not tags:
        return _NEVER_MATCH
    escaped = [re.escape(t) for t in tags]
    return re.compile(r'\b(' + '|'.join(escaped) + r')\b', re.IGNORECASE)

def _build_lang_pattern() -> re.Pattern:
    langs = _CLEANING.get("language_tags", [])
    if not langs:
        return _NEVER_MATCH
    escaped = [re.escape(lang) for lang in langs]
    return re.compile(r'\b(' + '|'.join(escaped) + r')\b', re.IGNORECASE)

def _build_tld_pattern() -> re.Pattern:
    tlds = _CLEANING.get("website_tlds", [])
    if not tlds:
        return _NEVER_MATCH
    return re.compile(r'\b\w+\.(' + '|'.join(tlds) + r')\b', re.IGNORECASE)

_TAG_RE  = _build_tag_pattern()
_LANG_RE = _build_lang_pattern()
_TLD_RE  = _build_tld_pattern()


_AUDIO_PREFIX      = r'(?:DTS-?MA-?D?|DTSMA-?D?|DTSD|DTS-?HD|DTS|TrueHD|DDP|DD|EAC3D?|AC3D?)'
_AUDIO_SURROUND_RE = re.compile(rf'\b{_AUDIO_PREFIX}\.?[57]\.?1D?\b', re.IGNORECASE)
_AUDIO_STEREO_RE   = re.compile(rf'\b{_AUDIO_PREFIX}\.?2\.?0\b',      re.IGNORECASE)
_MAX_PLAUSIBLE_YEAR = datetime.now().year + 2


def _rreplace(s: str, old: str, new: str, count: int) -> str:
    """Replace from the right — used so we only strip the LAST year occurrence."""
    return new.join(s.rsplit(old, count))


def clean_name(name: str) -> tuple:
    # Returns (clean_title, year). Strips codec/language/release tags.
    root, ext = os.path.splitext(name)
    if ext.lower() in VIDEO_EXTS:
        name = root
    if '[' in name:
        name = name[:name.index('[')].strip()
    name = name.strip()
    # Remove audio channel/format combinations before dot→space so we don't
    # end up with orphan "5 1" / "7 1" tokens. Covers .5.1, .7.1, .2.0 variants
    # with an optional trailing D (e.g. DD5.1D = Deutsch 5.1).
    name = _AUDIO_SURROUND_RE.sub('', name)  # 5.1 / 7.1 surround
    name = _AUDIO_STEREO_RE.sub('', name)    # 2.0 stereo
    # Standalone ".5.1" / ".7.1" / ".2.0" with no audio prefix
    # (e.g. "TrueHD.5.1" after we stripped "TrueHD" via tag regex)
    name = re.sub(r'(?<=[.\s])[57]\.1\b', '', name)
    name = re.sub(r'(?<=[.\s])2\.0\b',    '', name)
    # Website junk in the original dotted form (filecrypt.cc, xxxxx.to)
    name = _TLD_RE.sub('', name)
    # Region markers embedded in dotted form (e.g. ".US." between UHD and BluRay).
    # Only matches when NOT at the start of the name, so "Us.2019..." survives.
    name = re.sub(r'(?<=\.)\.?US\.', '.', name)
    name = re.sub(r'(?<=\.)\.?UK\.', '.', name)
    name = name.replace('.', ' ').replace('_', ' ')
    # Strip tags/langs/leftover TLDs FIRST so the trailing-dash release-group
    # stripper doesn't misidentify legitimate hyphenated titles (e.g. "Wu-Tang
    # Clan ... h264-WEBLE" — if we stripped trailing junk before removing
    # "h264", the regex could greedily eat "-Tang Clan ... WEBLE").
    name = _TLD_RE.sub('', name)
    name = _TAG_RE.sub('', name)
    name = _LANG_RE.sub('', name)
    # Year detection: prefer the LAST plausible year in the string (release
    # year comes after the title, e.g. "1917 2019" — 1917 is the title, 2019
    # is the year). Only accept years between 1900 and (current year + 2);
    # this prevents titles like "Blade Runner 2049" or "2001: A Space Odyssey"
    # from having a phantom future year stripped as if it were the release.
    name_stripped = re.sub(r'\s+', ' ', name).strip()
    year_matches = re.findall(r'\b(19\d{2}|20\d{2})\b', name_stripped)
    plausible = [y for y in year_matches if 1900 <= int(y) <= _MAX_PLAUSIBLE_YEAR]
    year = ''
    if plausible and name_stripped not in plausible:
        year = plausible[-1]
        name = _rreplace(name, year, '', 1)
    name = re.sub(r'\bS\d{2}\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\bSeason\s*\d+\b', '', name, flags=re.IGNORECASE)

    # NOW peel off trailing "-GROUPNAME" + bare junk. By this point all known
    # codec/lang/year tokens are gone, so the only thing that can be at the
    # end of the string is either the actual title or release-group residue.
    for _ in range(4):
        prev = name
        name = re.sub(r'[\s\-~]+$', '', name)
        name = re.sub(r'\s+(?:int|iNT|INT|GER|ger|MULTi|RG|rg|RM|rm|WiKi|wiki)$', '', name, flags=re.IGNORECASE)
        # Strip a trailing "~ filesize" annotation (e.g. "~ 6.6 GB", "~6GB").
        name = re.sub(r'\s*~[^~]*$', '', name)
        # Strip a trailing "-WORD" when the char before the dash is whitespace
        # or another dash (handles `--DARM` from double release groups).
        # Does NOT match when the char before is a letter (leaves "Wu-Tang" intact).
        name = re.sub(r'(?<=[\s\-])-\s*\w+\s*$', '', name)
        if name == prev:
            break

    name = re.sub(r'\s+', ' ', name).strip()
    return name, year


def resolve_title(raw: str) -> tuple:
    """Check TITLE_OVERRIDES by longest-prefix match, then fall back to clean_name.

    Always returns (title, year, imdb_id, is_override).
    Iterates _OVERRIDES_BY_LENGTH so "Star Trek Discovery" wins over "Star Trek".

    The match requires a WORD BOUNDARY after the override key — so an override
    keyed `Alles.steht.Kopf.2` (the 2024 sequel) does NOT half-match the folder
    `Alles.steht.Kopf.2015` (where the year 2015 happens to start with `2`).
    """
    # Strip extension only if it's a known video extension. Naive splitext on
    # folders like `Steve-O - Vol.1 Don't Try This` would treat `.1 Don't Try
    # This` as the "extension" and catastrophically truncate the stem.
    root, ext = os.path.splitext(raw)
    stem = root if ext.lower() in VIDEO_EXTS else raw
    stem_clean = re.sub(r'\s*\[.*', '', stem).strip()
    stem_norm = stem_clean.replace('.', ' ').lower()
    for key, val in _OVERRIDES_BY_LENGTH:
        key_norm = key.replace('.', ' ').lower()
        if not stem_norm.startswith(key_norm):
            continue
        # Word-boundary guard: the char immediately after the matched prefix
        # must NOT be alphanumeric, otherwise the prefix is biting into the
        # next token (e.g. "...kopf.2" matching "...kopf.2015").
        next_char_idx = len(key_norm)
        if next_char_idx < len(stem_norm) and stem_norm[next_char_idx].isalnum():
            continue
        title   = val[0]
        year_ov = val[1]
        imdb_ov = val[2] if len(val) > 2 else ""
        return title, year_ov, imdb_ov, True
    title, year = clean_name(raw)
    return title, year, "", False

# ---------------------------------------------------------------------------
# TMDB search
# ---------------------------------------------------------------------------

def _result_year(r: dict) -> str:
    """Extract the 4-digit year from a TMDB result, handling both tv/movie."""
    return (r.get("first_air_date") or r.get("release_date") or "")[:4]


def _year_match(pool: list, target: str, tol: int) -> dict | None:
    """Return the first result in pool whose year is within tol of target."""
    for r in pool:
        ry = _result_year(r)
        if ry and abs(int(ry) - int(target)) <= tol:
            return r
    return None


def tmdb_search(query: str, api_key: str, media_type: str = "tv", year: str = "") -> dict | None:
    endpoint = "multi" if media_type == "multi" else media_type
    params = {"api_key": api_key, "query": query, "page": 1}
    # TMDB's /search/multi does NOT honor year params; only /search/movie
    # (primary_release_year) and /search/tv (first_air_date_year) do. For multi
    # we pass no year to TMDB and post-filter the results by year ourselves.
    if year and media_type != "multi":
        params["primary_release_year" if media_type == "movie" else "first_air_date_year"] = year
    url = f"https://api.themoviedb.org/3/search/{endpoint}?{urllib.parse.urlencode(params)}"
    data = http_get_json(url, context=f"TMDB search {query!r}")
    if not data:
        return None
    results = data.get("results", [])
    if not results:
        return None

    if media_type == "multi":
        tv_results    = [r for r in results if r.get("media_type") == "tv"]
        movie_results = [r for r in results if r.get("media_type") == "movie"]
        if year:
            # Search across BOTH pools for a year match (exact, then ±1).
            # Prefer TV ties over movie ties. Only if no year-matching result
            # exists do we fall back to the default "first TV then first movie".
            for tol in (0, 1):
                hit = _year_match(tv_results, year, tol) or _year_match(movie_results, year, tol)
                if hit:
                    return hit
        if tv_results:    return tv_results[0]
        if movie_results: return movie_results[0]
    return results[0]


def tmdb_get_imdb_id(tmdb_id: int, api_key: str, media_type: str = "tv") -> str | None:
    """Fetch the IMDb ID for a TMDB entry via the external_ids endpoint."""
    mtype = "tv" if media_type in ("tv", "multi") else "movie"
    url = f"https://api.themoviedb.org/3/{mtype}/{tmdb_id}/external_ids?api_key={api_key}"
    data = http_get_json(url, context=f"TMDB external_ids id={tmdb_id}")
    return (data or {}).get("imdb_id") or None


def tmdb_get_available_seasons(tmdb_id: int, api_key: str) -> list:
    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={api_key}"
    data = http_get_json(url, context=f"TMDB series details id={tmdb_id}")
    if not data:
        return []
    return [s["season_number"] for s in data.get("seasons", []) if s.get("season_number", 0) > 0]


# Per-process cache: (tmdb_id, season_num) -> list of episode dicts.
# Avoids re-fetching the same season when the same series is scanned twice.
_SEASON_CACHE: dict = {}
_SEASON_CACHE_LOCK = threading.Lock()

def tmdb_get_season(tmdb_id: int, season_num: int, api_key: str) -> list | None:
    """Fetch episode list for a specific season from TMDB (with in-memory cache)."""
    cache_key = (tmdb_id, season_num)
    with _SEASON_CACHE_LOCK:
        if cache_key in _SEASON_CACHE:
            return _SEASON_CACHE[cache_key]
    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season_num}?api_key={api_key}"
    data = http_get_json(url, context=f"TMDB season {season_num} id={tmdb_id}")
    if data is None:
        return None
    episodes = data.get("episodes", [])
    with _SEASON_CACHE_LOCK:
        _SEASON_CACHE[cache_key] = episodes
    return episodes


def extract_all_episodes(name: str) -> list[tuple[int, int]]:
    """Extract all (season, episode) pairs from a name.

    Handles multi-episode files like S01E23E24, S06E01E02, S03E24-E25.
    """
    results = []
    # Match S01E01 or S01.E01 followed by optional additional episode numbers (E02, -E03, etc.)
    for m in re.finditer(r'[Ss](\d{1,2})\.?[Ee](\d{1,3})(?:[-.]?[Ee](\d{1,3}))*', name):
        season = int(m.group(1))
        # Get all episode numbers including dash-separated ones like S03E24-E25
        ep_matches = re.findall(r'[Ee](\d{1,3})', m.group(0))
        for ep in ep_matches:
            results.append((season, int(ep)))
    if results:
        return results
    # Fallback: 1x03 format
    for m in re.finditer(r'(\d{1,2})[Xx](\d{1,3})', name):
        results.append((int(m.group(1)), int(m.group(2))))
    if results:
        return results
    # Fallback: bare E01 without season prefix (e.g. "BBC.Earth.E01.Title" or "BBC Earth E01 Title")
    for m in re.finditer(r'(?:^|[\s.])E(\d{2,3})(?:[\s.]|$)', name):
        results.append((1, int(m.group(1))))
    if results:
        return results
    # Fallback: 4-digit SSEE format (e.g. "nitro.circus.live.0101-yestv.mp4")
    # Skip year-like numbers (1900-2099)
    for m in re.finditer(r'\.(\d{2})(\d{2})[-.]', name):
        full = int(m.group(0).strip('-.'))
        if 1900 <= full <= 2099:
            continue
        s, e = int(m.group(1)), int(m.group(2))
        if 1 <= s <= 30 and 1 <= e <= 99:
            results.append((s, e))
    return results


def scan_episodes_on_disk(series_path: str) -> dict[int, set[int]]:
    """Scan a series folder recursively for episode markers. Returns {season: {ep_numbers}}.

    Handles multiple layouts:
      - Flat:   Series/S01E01.mkv
      - 2-deep: Series/Season 01/S01E01.mkv
      - 3-deep: Series/Release.S01.Pack/Release.S01E01.Dir/file.mkv
    Also handles multi-episode files like S01E23E24 or S03E24-E25.
    """
    found = {}

    def walk(path: str, depth: int = 0):
        if depth > 3:
            return
        try:
            for entry in os.scandir(path):
                for s, e in extract_all_episodes(entry.name):
                    found.setdefault(s, set()).add(e)
                if entry.is_dir():
                    walk(entry.path, depth + 1)
        except PermissionError:
            pass

    walk(series_path)
    return found


def check_missing_episodes(series_path: str, tmdb_id: int, total_seasons: int,
                           api_key: str) -> dict:
    """Compare disk episodes against TMDB and return missing info."""
    disk_eps = scan_episodes_on_disk(series_path)
    has_missing    = False
    season_summary = {}

    available_seasons = tmdb_get_available_seasons(tmdb_id, api_key)
    if not available_seasons:
        available_seasons = list(range(1, total_seasons + 1))
    # Fetch all seasons in parallel via threadpool — each call is rate-limited
    # against TMDB by the global _RATE limiter.
    with ThreadPoolExecutor(max_workers=min(4, len(available_seasons) or 1)) as pool:
        futures = {pool.submit(tmdb_get_season, tmdb_id, s, api_key): s for s in available_seasons}
        season_eps = {futures[f]: f.result() for f in as_completed(futures)}
    for s in available_seasons:
        tmdb_eps = season_eps.get(s)
        if tmdb_eps is None:
            continue

        expected = {ep["episode_number"] for ep in tmdb_eps if ep.get("episode_number")}
        on_disk = disk_eps.get(s, set())
        missing_eps = sorted(expected - on_disk)
        season_summary[str(s)] = {
            "expected": len(expected),
            "found": len(on_disk & expected),
            "missing": missing_eps,
        }
        if missing_eps:
            has_missing = True

    return {
        "seasons_on_disk": sorted(disk_eps.keys()),
        "season_details": season_summary,
        "has_missing": has_missing,
    }


def tmdb_find(query: str, api_key: str, media_type: str, year: str = "") -> tuple:
    search_type = "multi" if media_type == "auto" else ("tv" if media_type == "series" else "movie")
    result = None
    if year:
        result = tmdb_search(query, api_key, search_type, year=year)
    if not result:
        result = tmdb_search(query, api_key, search_type)
    if not result:
        return None, None, None
    tmdb_id = result.get("id")
    rtype   = result.get("media_type", search_type)
    if rtype not in ("tv", "movie"):
        rtype = search_type if search_type != "multi" else "tv"
    imdb_id = tmdb_get_imdb_id(tmdb_id, api_key, rtype)
    return result, imdb_id, rtype

# ---------------------------------------------------------------------------
# OMDb fetch by IMDb ID
# ---------------------------------------------------------------------------

def omdb_by_id(imdb_id: str, api_key: str) -> dict | None:
    """Fetch full OMDb record using a known IMDb ID."""
    params = urllib.parse.urlencode({"i": imdb_id, "apikey": api_key})
    url = f"https://www.omdbapi.com/?{params}"
    data = http_get_json(url, context=f"OMDb id={imdb_id}")
    if data and data.get("Response") == "True":
        return data
    return None

# ---------------------------------------------------------------------------
# Fallback: direct OMDb title search (no TMDB involved)
# ---------------------------------------------------------------------------

def omdb_by_title(title: str, api_key: str, media_type: str) -> dict | None:
    mtype = "" if media_type == "auto" else ("series" if media_type in ("series", "tv") else "movie")
    params = {"t": title, "apikey": api_key}
    if mtype:
        params["type"] = mtype
    url = f"https://www.omdbapi.com/?{urllib.parse.urlencode(params)}"
    data = http_get_json(url, context=f"OMDb title={title!r}")
    if data and data.get("Response") == "True":
        return data
    return None

# ---------------------------------------------------------------------------
# Record building
# ---------------------------------------------------------------------------

def retry_unrated_cached(record: dict, omdb_key: str, media_type: str) -> bool:
    """Try to fill in OMDb fields for a cached record that has tmdb_id but no
    IMDb rating. Returns True if the record was updated with a real rating.

    Mutates `record` in place. Used by the cache fast-path so previously
    quota-exhausted entries self-heal on subsequent runs without needing
    --force (which would re-do the expensive TMDB search).

    No-op (returns False) if:
      - the record already has an IMDb rating (caller should pre-filter)
      - the record has no tmdb_id (TMDB never matched, nothing to retry)
      - OMDb returns no record OR the record has imdbRating = "N/A"
        (e.g. show is too new / has too few votes)
      - OMDb is currently disabled (host returned 401 earlier this run)
    """
    if not record.get("tmdb_id"):
        return False

    if record.get("imdb_id"):
        refreshed = omdb_by_id(record["imdb_id"], omdb_key)
    else:
        refreshed = omdb_by_title(record.get("title", record.get("folder", "")),
                                  omdb_key, media_type)

    if not refreshed:
        return False
    new_rating = refreshed.get("imdbRating", "N/A")
    if not new_rating or new_rating == "N/A":
        return False

    # Got a real rating — merge OMDb fields back into the record.
    record["imdb"]    = new_rating
    record["votes"]   = refreshed.get("imdbVotes",    record.get("votes", "N/A"))
    record["genre"]   = refreshed.get("Genre",        record.get("genre", "N/A"))
    record["plot"]    = refreshed.get("Plot",         record.get("plot", ""))
    record["poster"]  = refreshed.get("Poster",       record.get("poster", ""))
    record["imdb_id"] = refreshed.get("imdbID",       record.get("imdb_id", ""))
    record["rated"]   = refreshed.get("Rated",        record.get("rated", "N/A"))
    record["seasons"] = refreshed.get("totalSeasons", record.get("seasons", "N/A"))
    return True


def build_record(raw_name: str, tmdb: dict | None, omdb: dict | None) -> dict:
    """Build a unified record from optional TMDB + OMDb results.

    OMDb fields (from IMDb) take precedence when present; TMDB is the fallback
    and always supplies tmdb_id/tmdb_type so the viewer's TMDB link works.
    """
    record = {
        "folder":    raw_name,
        "title":     raw_name,
        "year":      "N/A",
        "genre":     "N/A",
        "imdb":      "N/A",
        "votes":     "N/A",
        "plot":      "",
        "poster":    "",
        "imdb_id":   "",
        "tmdb_id":   "",
        "tmdb_type": "",
        "seasons":   "N/A",
        "rated":     "N/A",
        "type":      "N/A",
    }

    if tmdb:
        record["tmdb_id"]   = str(tmdb.get("id", ""))
        record["tmdb_type"] = tmdb.get("media_type") or ("tv" if tmdb.get("name") else "movie")
        record["title"]     = tmdb.get("name") or tmdb.get("title") or raw_name
        record["year"]      = (tmdb.get("first_air_date") or tmdb.get("release_date") or "")[:4] or "N/A"
        record["plot"]      = tmdb.get("overview", "")
        record["type"]      = tmdb.get("media_type", "N/A")
        if tmdb.get("poster_path"):
            record["poster"] = f"https://image.tmdb.org/t/p/w185{tmdb['poster_path']}"

    if omdb:
        record["title"]   = omdb.get("Title", record["title"])
        record["year"]    = omdb.get("Year", record["year"])
        record["genre"]   = omdb.get("Genre", "N/A")
        record["imdb"]    = omdb.get("imdbRating", "N/A")
        record["votes"]   = omdb.get("imdbVotes", "N/A")
        record["plot"]    = omdb.get("Plot", record["plot"])
        record["poster"]  = omdb.get("Poster", record["poster"])
        record["imdb_id"] = omdb.get("imdbID", "")
        record["seasons"] = omdb.get("totalSeasons", "N/A")
        record["rated"]   = omdb.get("Rated", "N/A")
        record["type"]    = omdb.get("Type", record["type"])

    return record

# ---------------------------------------------------------------------------
# Folder scan
# ---------------------------------------------------------------------------


# Subfolder names that contain supplemental material, not the main feature.
# These are skipped at scan time so they don't show up as phantom entries.
_JUNK_DIR_NAMES = {
    "subs", "subtitles", "subtitle",
    "extras", "extra", "featurettes", "featurette",
    "artwork", "covers", "fanart", "posters",
    "proof", "proofs", "sample", "samples",
    "behind the scenes", "behindthescenes",
    "trailers", "trailer",
}

def _is_sample_file(name: str) -> bool:
    """Return True for typical sample/junk filenames that shouldn't be looked up."""
    lowered = name.lower()
    if "sample" in lowered:
        return True
    # Scene-style lowercase-dashed dumps like `watchable-thenegotiator-1080-de.mkv`
    # get mangled by the release-group stripper into nothing useful.
    if re.match(r'^[a-z0-9]+-[a-z0-9]+-\d{3,4}(p|-)', lowered):
        return True
    return False


def scan_folder(folder: str) -> list:
    try:
        entries = list(os.scandir(folder))
    except PermissionError:
        raise PermissionError(f"[ERROR] Cannot access: {folder}")
    dirs  = sorted(
        e.name for e in entries
        if e.is_dir()
        and not e.name.startswith('.')
        and e.name.lower() not in _JUNK_DIR_NAMES
    )
    files = sorted(
        e.name for e in entries
        if e.is_file()
        and not e.name.startswith('.')
        and os.path.splitext(e.name)[1].lower() in VIDEO_EXTS
        and not _is_sample_file(e.name)
    )
    return dirs + files


def is_category_folder(path: str) -> bool:
    """True if path is a grouping/collection folder, not a media item itself.

    Rules:
     - multiple REAL video files directly inside → collection
     - subdirs that don't look like season/junk folders → collection
     - single video file or season dirs → media item

    Samples and junk folders are excluded from the count, so a movie folder
    containing "movie.mkv" + "movie-sample.mkv" is correctly identified as
    a single media item, not a collection.
    """
    try:
        entries = list(os.scandir(path))
    except PermissionError:
        return False
    video_files = [
        e for e in entries
        if e.is_file()
        and not e.name.startswith('.')
        and os.path.splitext(e.name)[1].lower() in VIDEO_EXTS
        and not _is_sample_file(e.name)
    ]
    if len(video_files) > 1:
        return True
    if video_files:
        return False
    subdirs = [
        e.name for e in entries
        if e.is_dir()
        and not e.name.startswith('.')
        and e.name.lower() not in _JUNK_DIR_NAMES
    ]
    if not subdirs:
        return False
    if any(SEASON_DIR_PATTERN.match(d) for d in subdirs):
        return False
    return True


# ---------------------------------------------------------------------------
# Per-entry processing
# ---------------------------------------------------------------------------

_EntryResult = namedtuple(
    '_EntryResult',
    ['index', 'record', 'log_entry', 'missing_entry', 'complete_entry'],
)


@dataclass
class _ScanResult:
    scan_log:     list
    errors:       list
    missing_log:  list
    complete_log: list
    scan_end:     datetime
    duration:     timedelta
    matched:      int
    rated:        int
    unmatched:    int


@dataclass
class _ScanConfig:
    cache:          dict
    omdb_key:       str
    tmdb_key:       str
    media_type:     str
    check_episodes: bool
    total:          int
    print_lock:     threading.Lock


def _process_one(index_folder_raw: tuple, cfg: _ScanConfig) -> _EntryResult:
    """Resolve, fetch, and log a single media entry."""
    i, folder, raw = index_folder_raw
    # Per-entry output buffer so each entry's lines print atomically,
    # even when other worker threads run in parallel.
    lines: list[str] = []

    # Cache hit — fast path
    if raw in cfg.cache:
        record = cfg.cache[raw]
        has_imdb_cached = record.get('imdb', 'N/A') != 'N/A'

        # Self-healing: previously-unrated cache entries get one cheap
        # OMDb retry without re-doing the expensive TMDB search.
        retried_label = ""
        if not has_imdb_cached and retry_unrated_cached(record, cfg.omdb_key, cfg.media_type):
            has_imdb_cached = True
            retried_label = f" + RETRY → IMDb {record['imdb']}"

        lines.append(f"[{i:03}/{cfg.total:03}] {raw!r:50s} → CACHED ({record['title']}){retried_label}")
        log_entry = {
            "index":         i,
            "folder":        raw,
            "searched_as":   record.get('title', raw),
            "year_used":     record.get('year', ''),
            "override":      False,
            "matched_title": record.get('title', ''),
            "matched_year":  record.get('year', ''),
            "imdb":          record.get('imdb', 'N/A'),
            "matched":       has_imdb_cached or bool(record.get('tmdb_id')),
            "rated":         has_imdb_cached,
            "cached":        True,
        }

        # Even on cache hit, honour --check-episodes: the cached record
        # has no episode info, and gaps change over time as new episodes
        # air. We use the cached tmdb_id directly (no re-search needed).
        cached_missing = None
        cached_complete = None
        if cfg.check_episodes and record.get("tmdb_id") and record.get("tmdb_type") == "tv":
            try:
                tmdb_id_val = int(record["tmdb_id"])
            except (TypeError, ValueError):
                tmdb_id_val = None
            if tmdb_id_val:
                series_path = os.path.join(folder, raw)
                if os.path.isdir(series_path):
                    ep_info = check_missing_episodes(series_path, tmdb_id_val, 0, cfg.tmdb_key)
                    record["episode_check"] = ep_info
                    if ep_info["has_missing"]:
                        gaps = [f"S{int(sn):02d}: missing {det['missing']}"
                                for sn, det in ep_info["season_details"].items() if det["missing"]]
                        lines.append(f"           ⚠ Missing: {'; '.join(gaps)}")
                        cached_missing = {
                            "folder":  raw,
                            "title":   record["title"],
                            "year":    record["year"],
                            "seasons": ep_info["season_details"],
                        }
                    elif ep_info["seasons_on_disk"]:
                        lines.append(f"           ✓ All episodes present (seasons {ep_info['seasons_on_disk']})")
                        total_eps = sum(det["expected"] for det in ep_info["season_details"].values())
                        cached_complete = {
                            "title":       record["title"],
                            "year":        record["year"],
                            "imdb":        record["imdb"],
                            "num_seasons": len(ep_info["season_details"]),
                            "total_eps":   total_eps,
                        }

        with cfg.print_lock:
            for ln in lines:
                print(ln)
        return _EntryResult(i, record, log_entry, cached_missing, cached_complete)

    cleaned, year, direct_id, override_used = resolve_title(raw)
    tag = " [OVERRIDE]" if override_used else ""

    omdb_result  = None
    tmdb_result  = None
    imdb_id      = ""
    rtype        = "movie" if cfg.media_type == "movie" else "tv"

    if direct_id:
        imdb_id     = direct_id
        omdb_result = omdb_by_id(imdb_id, cfg.omdb_key)
        tmdb_result, tmdb_imdb_id, rtype = tmdb_find(cleaned, cfg.tmdb_key, cfg.media_type, year=year)
        if not omdb_result and tmdb_imdb_id:
            imdb_id     = tmdb_imdb_id
            omdb_result = omdb_by_id(imdb_id, cfg.omdb_key)
    else:
        tmdb_result, imdb_id, rtype = tmdb_find(cleaned, cfg.tmdb_key, cfg.media_type, year=year)
        if imdb_id:
            omdb_result = omdb_by_id(imdb_id, cfg.omdb_key)
        if not omdb_result and not tmdb_result:
            omdb_result = omdb_by_title(cleaned, cfg.omdb_key, cfg.media_type)

    record = build_record(raw, tmdb_result, omdb_result)
    # Persist any non-empty IMDb ID we know about, even if OMDb didn't
    # return one. Order of preference: explicit override > TMDB external_ids.
    # This guarantees the viewer's IMDb link works for direct-ID overrides
    # like ["Knox Goes Away", "2023", "tt20115766"] even when OMDb is down.
    if direct_id and not record.get("imdb_id"):
        record["imdb_id"] = direct_id
    elif imdb_id and not record.get("imdb_id"):
        record["imdb_id"] = imdb_id
    record['display_name'] = clean_name(raw)[0]
    is_rated   = record['imdb'] != 'N/A'
    is_matched = is_rated or bool(record.get('tmdb_id')) or bool(omdb_result)

    if is_rated:
        status = f"IMDb: {record['imdb']}"
    elif is_matched:
        status = "TMDB only (no IMDb rating)"
    else:
        status = "NOT FOUND"

    lines.append(f"[{i:03}/{cfg.total:03}] {raw!r:50s} → {cleaned!r} ({year}){tag}")
    lines.append(f"           → {record['title']} ({record['year']}) | {status}")

    log_entry = {
        "index":         i,
        "folder":        raw,
        "searched_as":   cleaned,
        "year_used":     year,
        "override":      override_used,
        "matched_title": record['title'],
        "matched_year":  record['year'],
        "imdb":          record['imdb'],
        "matched":       is_matched,
        "rated":         is_rated,
        "cached":        False,
    }

    missing_entry  = None
    complete_entry = None

    if cfg.check_episodes and tmdb_result and rtype == "tv":
        tmdb_id_val = tmdb_result.get("id")
        try:
            total_seasons = int(record.get("seasons") or 0)
        except (TypeError, ValueError):
            total_seasons = 0
        if tmdb_id_val:
            series_path = os.path.join(folder, raw)
            if os.path.isdir(series_path):
                ep_info = check_missing_episodes(series_path, tmdb_id_val, total_seasons, cfg.tmdb_key)
                record["episode_check"] = ep_info
                if ep_info["has_missing"]:
                    gaps = [f"S{int(sn):02d}: missing {det['missing']}"
                            for sn, det in ep_info["season_details"].items() if det["missing"]]
                    lines.append(f"           ⚠ Missing: {'; '.join(gaps)}")
                    missing_entry = {
                        "folder":  raw,
                        "title":   record["title"],
                        "year":    record["year"],
                        "seasons": ep_info["season_details"],
                    }
                else:
                    disk_s = ep_info["seasons_on_disk"]
                    if disk_s:
                        lines.append(f"           ✓ All episodes present (seasons {disk_s})")
                        total_eps = sum(det["expected"] for det in ep_info["season_details"].values())
                        complete_entry = {
                            "title":       record["title"],
                            "year":        record["year"],
                            "imdb":        record["imdb"],
                            "num_seasons": len(ep_info["season_details"]),
                            "total_eps":   total_eps,
                        }
                    else:
                        lines.append(f"           ? No episode files detected on disk")

    # Flush all of this entry's lines as one atomic block.
    with cfg.print_lock:
        for ln in lines:
            print(ln)

    return _EntryResult(i, record, log_entry, missing_entry, complete_entry)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scan folder and fetch IMDb ratings via TMDB + OMDb.")
    parser.add_argument("folders", nargs="+", help="One or more paths to media folders")
    parser.add_argument("--output", default=OUTPUT_FILE, help=f"Output JSON (default: {OUTPUT_FILE})")
    parser.add_argument("--tmdb-key", default=TMDB_API_KEY, help="TMDB API key")
    parser.add_argument("--omdb-key", default=OMDB_API_KEY, help="OMDb API key")
    parser.add_argument(
        "--type", dest="media_type", default="series",
        choices=["series", "movie", "auto"],
        help="'series' (default), 'movie', or 'auto'",
    )
    parser.add_argument(
        "--check-episodes", action="store_true",
        help="Scan disk for episodes and compare against TMDB to find missing ones",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Ignore cache and re-scan all entries",
    )
    args = parser.parse_args()

    if args.output == OUTPUT_FILE:
        if args.media_type == "movie":
            args.output = "json/filme.json"
        else:
            args.output = "json/serien.json"

    tmdb_key = os.environ.get("TMDB_API_KEY") or args.tmdb_key
    omdb_key = os.environ.get("OMDB_API_KEY") or args.omdb_key

    if tmdb_key == "YOUR_TMDB_API_KEY":
        sys.exit(
            "[ERROR] No TMDB API key.\n"
            "  Get one free at: https://www.themoviedb.org/settings/api\n"
            "  Then: export TMDB_API_KEY=yourkey"
        )
    if omdb_key == "YOUR_OMDB_API_KEY":
        sys.exit(
            "[ERROR] No OMDb API key.\n"
            "  Get one free at: https://www.omdbapi.com/apikey.aspx\n"
            "  Then: export OMDB_API_KEY=yourkey"
        )

    # Load existing JSON as cache (skip already-rated entries on next run)
    cache: dict[str, dict] = {}
    output_path = os.path.abspath(args.output)
    if not args.force and os.path.exists(output_path):
        try:
            with open(output_path, encoding="utf-8") as f:
                cached_records = json.load(f)
            cache = {r["folder"]: r for r in cached_records if (r.get("imdb", "N/A") != "N/A" or r.get("tmdb_id")) and r.get("folder")}
            print(f"Cache:    {len(cache)} already-rated entries loaded (use --force to rescan all)\n")
        except Exception as e:
            print(f"[WARN] Could not load cache from {output_path}: {e}", file=sys.stderr)

    folders = []
    for raw_path in args.folders:
        p = os.path.expanduser(raw_path)
        if not os.path.isdir(p):
            print(f"[WARN] Not a directory, skipping: {p}", file=sys.stderr)
        else:
            folders.append(p)
    if not folders:
        sys.exit("[ERROR] No valid directories given.")

    print(f"Mode:     --type {args.media_type}")
    print(f"Folders:  {len(folders)}")
    for p in folders:
        print(f"  • {p}")
    print()

    all_entries = []
    for folder in folders:
        try:
            d = scan_folder(folder)
        except PermissionError as e:
            sys.exit(str(e))
        direct = []
        for name in d:
            sub_path = os.path.join(folder, name)
            if os.path.isdir(sub_path) and args.media_type == "movie" and is_category_folder(sub_path):
                try:
                    sub = scan_folder(sub_path)
                except PermissionError as e:
                    print(str(e), file=sys.stderr)
                    continue
                print(f"  {len(sub):3d} entries in {sub_path} (subfolder)")
                all_entries.extend((sub_path, x) for x in sub)
            else:
                direct.append(name)
        if direct:
            print(f"  {len(direct):3d} entries in {folder}")
            all_entries.extend((folder, x) for x in direct)

    # Dedup on (folder, raw), not raw alone — two folders may legitimately
    # contain the same title (e.g. "The Office" in /uk and /us).
    seen_pairs = set()
    unique_entries = []
    for folder, raw in all_entries:
        key = (folder, raw)
        if key not in seen_pairs:
            seen_pairs.add(key)
            unique_entries.append((folder, raw))
        else:
            print(f"[SKIP] Duplicate: {raw!r} in {folder}")
    print(f"\nTotal: {len(unique_entries)} entries to process.\n")

    scan_start    = datetime.now()
    records       = []
    errors        = []
    scan_log      = []
    missing_log   = []
    complete_log  = []
    total         = len(unique_entries)
    cfg = _ScanConfig(
        cache=cache,
        omdb_key=omdb_key,
        tmdb_key=tmdb_key,
        media_type=args.media_type,
        check_episodes=args.check_episodes,
        total=total,
        print_lock=threading.Lock(),
    )

    # Dispatch work to the thread pool; collect results in submission order
    # so the output and logs stay in folder-listing order.
    tasks = [(i, f, r) for i, (f, r) in enumerate(unique_entries, 1)]
    results_by_index: dict = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process_one, t, cfg): t[0] for t in tasks}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results_by_index[idx] = fut.result()
            except Exception as e:
                print(f"  [ERROR] Worker failed for entry {idx}: {e}", file=sys.stderr)

    for i in sorted(results_by_index):
        entry = results_by_index[i]
        record, log_entry, missing_entry, complete_entry = (
            entry.record, entry.log_entry, entry.missing_entry, entry.complete_entry
        )
        records.append(record)
        scan_log.append(log_entry)
        if not log_entry["matched"]:
            errors.append(log_entry)
        if missing_entry:
            missing_log.append(missing_entry)
        if complete_entry:
            complete_log.append(complete_entry)

    # ── Write JSON output ───────────────────────────────────────────────────
    out_parent = os.path.dirname(output_path)
    if out_parent:
        os.makedirs(out_parent, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    rated     = sum(1 for r in records if r["imdb"] != "N/A")
    matched   = sum(1 for r in records if r["imdb"] != "N/A" or r.get("tmdb_id"))
    unmatched = len(records) - matched
    scan_end  = datetime.now()
    duration  = scan_end - scan_start

    print(f"\nDone! {matched}/{len(records)} matched ({rated} rated, {matched - rated} TMDB-only, {unmatched} not found)")
    print(f"Output → {output_path}")

    write_logs(
        _ScanResult(
            scan_log=scan_log, errors=errors,
            missing_log=missing_log, complete_log=complete_log,
            scan_end=scan_end, duration=duration,
            matched=matched, rated=rated, unmatched=unmatched,
        ),
        output_path=output_path,
        folders=folders,
        media_type=args.media_type,
        check_episodes=args.check_episodes,
    )

    viewer = "html/filme.html" if args.media_type == "movie" else "html/serien.html"
    print(f"\nOpen {viewer} to explore the results.")


def write_logs(result: _ScanResult, output_path: str, folders: list,
               media_type: str, check_episodes: bool):
    """Write scan_log, error log, and (if check_episodes) missing/complete logs.

    Logs directory is always next to the fetcher.py script, not derived from
    the --output path, so `--output /tmp/foo.json` doesn't try to create /logs.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(script_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    date_str  = result.scan_end.strftime("%Y-%m-%d_%H-%M-%S")
    total     = len(result.scan_log)
    summary   = (f"{total} scanned | {result.matched} matched | "
                 f"{result.rated} rated | {result.unmatched} not found")

    # ── Scan log (every entry) ──────────────────────────────────────────────
    scan_path = os.path.join(logs_dir, f"{date_str}_scanlog.txt")
    with open(scan_path, "w", encoding="utf-8") as sf:
        sf.write("scan log\n")
        sf.write(f"Date       : {result.scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
        sf.write(f"Duration   : {str(result.duration).split('.')[0]}\n")
        sf.write(f"Mode       : --type {media_type}\n")
        sf.write(f"Output     : {output_path}\n")
        sf.write(f"Folders    :\n\n")
        for p in folders:
            sf.write(f"  • {p}\n")
        sf.write(f"\nTotal: {summary}\n")
        sf.write("\n" + "=" * 70 + "\n\n")
        for e in result.scan_log:
            ov     = " [OVERRIDE]" if e["override"] else ""
            cached = " [CACHED]"   if e.get("cached") else ""
            if e.get("rated"):
                ok = "✓"
            elif e.get("matched"):
                ok = "~"  # matched but no IMDb rating
            else:
                ok = "✗"
            sf.write(f"[{e['index']:03}] {ok} {e['folder']}{cached}\n")
            sf.write(f"       searched : {e['searched_as']!r}")
            if e["year_used"]:
                sf.write(f"  year={e['year_used']}")
            sf.write(f"{ov}\n")
            if e.get("rated"):
                sf.write(f"       result  : {e['matched_title']} ({e['matched_year']}) | IMDb {e['imdb']}\n")
            elif e.get("matched"):
                sf.write(f"       result  : {e['matched_title']} ({e['matched_year']}) | no IMDb rating\n")
            else:
                sf.write(f"       result  : NOT FOUND\n")
            sf.write("\n")
        tmdb_only = result.matched - result.rated
        sf.write("=" * 70 + "\n")
        sf.write(f"Done! {result.matched}/{total} matched "
                 f"({result.rated} rated, {tmdb_only} TMDB-only, {result.unmatched} not found)\n")
    print(f"📋 Scan log  → {scan_path}")

    # ── Error log (only NOT FOUND, i.e. nothing matched at all) ─────────────
    error_path = os.path.join(logs_dir, f"{date_str}_error.txt")
    with open(error_path, "w", encoding="utf-8") as ef:
        ef.write("fetch_ratings — error log\n")
        ef.write(f"Date    : {result.scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
        ef.write(f"Output  : {output_path}\n")
        ef.write(f"Total   : {summary}\n")
        ef.write("=" * 70 + "\n\n")
        if result.errors:
            for e in result.errors:
                ef.write(f"NOT FOUND: {e['folder']}\n")
                ef.write(f"  Searched as : {e['searched_as']!r}")
                if e["year_used"]:
                    ef.write(f"  (year: {e['year_used']})")
                ef.write("\n")
                ef.write(f"  → Add to TITLE_OVERRIDES to fix\n\n")
        else:
            ef.write("No errors — everything matched!\n")
    if result.errors:
        print(f"⚠  Error log → {error_path}  ({result.unmatched} not found)")
    else:
        print(f"✓  No errors — {error_path}")

    # ── Missing episodes / complete-series logs ─────────────────────────────
    if check_episodes:
        missing_path = os.path.join(logs_dir, f"{date_str}_missing_episodes.txt")
        with open(missing_path, "w", encoding="utf-8") as mf:
            mf.write("fetch_ratings — missing episodes log\n")
            mf.write(f"Date    : {result.scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
            mf.write(f"Total   : {len(result.missing_log)} series with missing episodes\n")
            mf.write("=" * 70 + "\n\n")
            if result.missing_log:
                for entry in result.missing_log:
                    mf.write(f"{entry['title']} ({entry['year']})  [{entry['folder']}]\n")
                    for sn, det in entry["seasons"].items():
                        if det["missing"]:
                            mf.write(f"  S{int(sn):02d}: {det['found']}/{det['expected']} present — missing: {det['missing']}\n")
                    mf.write("\n")
            else:
                mf.write("No missing episodes found!\n")
        if result.missing_log:
            print(f"⚠  Missing log → {missing_path}  ({len(result.missing_log)} series affected)")
        else:
            print(f"✓  No missing episodes — {missing_path}")

        complete_path = os.path.join(logs_dir, f"{date_str}_complete.txt")
        with open(complete_path, "w", encoding="utf-8") as cf:
            cf.write("fetch_ratings — complete series log\n")
            cf.write(f"Date    : {result.scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
            cf.write(f"Total   : {len(result.complete_log)} complete series\n")
            cf.write("=" * 70 + "\n\n")
            if result.complete_log:
                for entry in sorted(result.complete_log, key=lambda x: x["title"].lower()):
                    cf.write(f"{entry['title']} ({entry['year']})\n")
                    cf.write(f"  IMDb rating : {entry['imdb']}\n")
                    cf.write(f"  Seasons     : {entry['num_seasons']}\n")
                    cf.write(f"  Episodes    : {entry['total_eps']}\n")
                    cf.write("\n")
            else:
                cf.write("No complete series found.\n")
        print(f"✓  Complete log → {complete_path}  ({len(result.complete_log)} series)")


if __name__ == "__main__":
    main()

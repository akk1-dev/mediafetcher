#!/usr/bin/env python3
"""
fetch_ratings.py - Scan a folder and fetch IMDb ratings.

Flow:
  1. Clean folder name
  2. Search TMDB (handles German titles, fuzzy matching)
  3. Get IMDb ID from TMDB result
  4. Fetch IMDb rating from OMDb using that ID
  5. Optionally scan disk for episodes and compare against TMDB (--check-episodes)
  6. Write ratings.json for the HTML viewer

API keys (set via env vars or edit the script):
  - TMDB (free): https://www.themoviedb.org/settings/api
  - OMDb (free): https://www.omdbapi.com/apikey.aspx

Usage:
    python3 fetch_ratings.py "/media/veracrypt4/4K Serien"
    python3 fetch_ratings.py "/media/veracrypt4/4K Serien" --check-episodes
    python3 fetch_ratings.py "/media/veracrypt4/4K Filme" --type movie
    python3 fetch_ratings.py "/media/mixed"               --type auto
"""

import os
import sys
import json
import time
import re
import argparse
import urllib.request
import urllib.parse
from datetime import datetime

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
REQUEST_DELAY = 0.25   # seconds between API calls

# Episode filename patterns: S01E03, s1e3, 1x03, E03
EP_PATTERNS = [
    re.compile(r'[Ss](\d{1,2})[Ee](\d{1,3})'),   # S01E03
    re.compile(r'(\d{1,2})[Xx](\d{1,3})'),         # 1x03
]
SEASON_DIR_PATTERN = re.compile(r'[Ss](?:eason\s*)?(\d{1,2})', re.IGNORECASE)

# ---------------------------------------------------------------------------
# Title overrides — loaded from overrides.json (next to this script).
# Format in JSON: "Ordner-Präfix": ["Suchtitel", "Jahr"]
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

TITLE_OVERRIDES: dict[str, tuple] = _load_overrides()

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

def _build_tag_pattern() -> re.Pattern:
    tags = (
        _CLEANING.get("codec_tags", []) +
        _CLEANING.get("release_tags", [])
    )
    escaped = [re.escape(t) for t in tags]
    return re.compile(r'\b(' + '|'.join(escaped) + r')\b', re.IGNORECASE)

def _build_lang_pattern() -> re.Pattern:
    langs = _CLEANING.get("language_tags", [])
    escaped = [re.escape(l) for l in langs]
    return re.compile(r'\b(' + '|'.join(escaped) + r')\b', re.IGNORECASE)

def _build_tld_pattern() -> re.Pattern:
    tlds = _CLEANING.get("website_tlds", [])
    return re.compile(r'\b\w+\.(' + '|'.join(tlds) + r')\b', re.IGNORECASE)

_TAG_RE  = _build_tag_pattern()
_LANG_RE = _build_lang_pattern()
_TLD_RE  = _build_tld_pattern()


def clean_name(name: str) -> tuple:
    # Returns (clean_title, year). Strips codec/language/release tags.
    root, ext = os.path.splitext(name)
    if ext.lower() in VIDEO_EXTS:
        name = root
    if '[' in name:
        name = name[:name.index('[')].strip()
    name = name.strip()
    # Remove audio decimal formats before dot→space (e.g. DTSD.5.1, DD5.1)
    name = re.sub(r'\b(?:DTSD|DTS-?HD|DD|DTS)\.5\.1\b', '', name, flags=re.IGNORECASE)
    name = name.replace('.', ' ').replace('_', ' ')
    # Strip trailing -GroupName patterns (handle trailing spaces + multiple -word)
    name = re.sub(r'(\s*-\s*\w+)+\s*$', '', name).strip()
    # Remove website-like garbage (word.tld)
    name = _TLD_RE.sub('', name)
    name = _TAG_RE.sub('', name)
    name = _LANG_RE.sub('', name)
    name_stripped = re.sub(r'\s+', ' ', name).strip()
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', name_stripped)
    if year_match and name_stripped != year_match.group(1):
        year = year_match.group(1)
        name = re.sub(r'\b(19\d{2}|20\d{2})\b', '', name)
    else:
        year = ''
    name = re.sub(r'\bS\d{2}\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\bSeason\s*\d+\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[\s\-]+$', '', name)
    name = re.sub(r'\b[A-Z]{5,}\b', '', name)
    # Remove mixed-case release group names (e.g. iNTERNEL, SUBFiX, CONTRiBUTiON)
    name = re.sub(r'\b\w*[A-Z]{4,}\w*\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name, year


def resolve_title(raw: str) -> tuple:
    """Check TITLE_OVERRIDES by prefix match, then fall back to clean_name.

    Returns (title, year) or (title, year, imdb_id) if override has a direct IMDb ID.
    """
    stem = os.path.splitext(raw)[0]
    stem_clean = re.sub(r'\s*\[.*', '', stem).strip()
    stem_norm = stem_clean.replace('.', ' ')  # normalize dots so file-based names match
    for key, val in TITLE_OVERRIDES.items():
        key_norm = key.replace('.', ' ')
        if stem_norm.lower().startswith(key_norm.lower()):
            return val  # may be (title, year) or (title, year, imdb_id)
    return clean_name(raw)

# ---------------------------------------------------------------------------
# TMDB search
# ---------------------------------------------------------------------------

def tmdb_search(query: str, api_key: str, media_type: str = "tv", year: str = "") -> dict | None:
    endpoint = "multi" if media_type == "multi" else media_type
    params = {"api_key": api_key, "query": query, "page": 1}
    if year:
        params["primary_release_year" if media_type == "movie" else "first_air_date_year"] = year
    url = f"https://api.themoviedb.org/3/search/{endpoint}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        results = data.get("results", [])
        if not results:
            return None
        if media_type == "multi":
            tv_results    = [r for r in results if r.get("media_type") == "tv"]
            movie_results = [r for r in results if r.get("media_type") == "movie"]
            if tv_results:    return tv_results[0]
            if movie_results: return movie_results[0]
        return results[0]
    except Exception as e:
        print(f"  [WARN] TMDB search failed for '{query}': {e}", file=sys.stderr)
    return None


def tmdb_get_imdb_id(tmdb_id: int, api_key: str, media_type: str = "tv") -> str | None:
    """Fetch the IMDb ID for a TMDB entry via the external_ids endpoint."""
    mtype = "tv" if media_type in ("tv", "multi") else "movie"
    url = f"https://api.themoviedb.org/3/{mtype}/{tmdb_id}/external_ids?api_key={api_key}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return data.get("imdb_id") or None
    except Exception as e:
        print(f"  [WARN] TMDB external_ids failed for id {tmdb_id}: {e}", file=sys.stderr)
    return None


def tmdb_get_available_seasons(tmdb_id: int, api_key: str) -> list:
    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={api_key}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return [s["season_number"] for s in data.get("seasons", []) if s.get("season_number", 0) > 0]
    except Exception as e:
        print(f"  [WARN] TMDB series details failed for id {tmdb_id}: {e}", file=sys.stderr)
    return []


def tmdb_get_season(tmdb_id: int, season_num: int, api_key: str) -> list[dict] | None:
    """Fetch episode list for a specific season from TMDB."""
    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season_num}?api_key={api_key}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return data.get("episodes", [])
    except Exception as e:
        print(f"  [WARN] TMDB season {season_num} fetch failed for id {tmdb_id}: {e}", file=sys.stderr)
    return None


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
    # Fallback: bare E01 without season prefix (e.g. "BBC.Earth.Afrika.E01.Title")
    for m in re.finditer(r'\.E(\d{2,3})\.', name):
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
    missing = {}
    season_summary = {}

    available_seasons = tmdb_get_available_seasons(tmdb_id, api_key)
    time.sleep(REQUEST_DELAY)
    if not available_seasons:
        available_seasons = list(range(1, total_seasons + 1))
    for s in available_seasons:
        tmdb_eps = tmdb_get_season(tmdb_id, s, api_key)
        time.sleep(REQUEST_DELAY)
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
            missing[str(s)] = missing_eps

    return {
        "seasons_on_disk": sorted(disk_eps.keys()),
        "season_details": season_summary,
        "has_missing": bool(missing),
    }


def tmdb_find(query: str, api_key: str, media_type: str, year: str = "") -> tuple:
    search_type = "multi" if media_type == "auto" else ("tv" if media_type == "series" else "movie")
    result = None
    if year:
        result = tmdb_search(query, api_key, search_type, year=year)
        time.sleep(REQUEST_DELAY)
    if not result:
        result = tmdb_search(query, api_key, search_type)
        time.sleep(REQUEST_DELAY)
    if not result:
        return None, None, None
    tmdb_id = result.get("id")
    rtype   = result.get("media_type", search_type)
    if rtype not in ("tv", "movie"):
        rtype = search_type if search_type != "multi" else "tv"
    imdb_id = tmdb_get_imdb_id(tmdb_id, api_key, rtype)
    time.sleep(REQUEST_DELAY)
    return result, imdb_id, rtype

# ---------------------------------------------------------------------------
# OMDb fetch by IMDb ID
# ---------------------------------------------------------------------------

def omdb_by_id(imdb_id: str, api_key: str) -> dict | None:
    """Fetch full OMDb record using a known IMDb ID."""
    params = urllib.parse.urlencode({"i": imdb_id, "apikey": api_key})
    url = f"https://www.omdbapi.com/?{params}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if data.get("Response") == "True":
            return data
    except Exception as e:
        print(f"  [WARN] OMDb fetch failed for {imdb_id}: {e}", file=sys.stderr)
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
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if data.get("Response") == "True":
            return data
    except Exception as e:
        print(f"  [WARN] OMDb title search failed for '{title}': {e}", file=sys.stderr)
    return None

# ---------------------------------------------------------------------------
# Record building
# ---------------------------------------------------------------------------

def build_record(raw_name: str, tmdb: dict | None, omdb: dict | None) -> dict:
    # Prefer OMDb for title/metadata (it's the IMDb data), fall back to TMDB
    tmdb_id   = str(tmdb.get("id", "")) if tmdb else ""
    tmdb_type = tmdb.get("media_type") or ("tv" if tmdb and tmdb.get("name") else "movie") if tmdb else ""
    if omdb:
        return {
            "folder":    raw_name,
            "title":     omdb.get("Title", raw_name),
            "year":      omdb.get("Year", "N/A"),
            "genre":     omdb.get("Genre", "N/A"),
            "imdb":      omdb.get("imdbRating", "N/A"),
            "votes":     omdb.get("imdbVotes", "N/A"),
            "plot":      omdb.get("Plot", ""),
            "poster":    omdb.get("Poster", "N/A"),
            "imdb_id":   omdb.get("imdbID", ""),
            "tmdb_id":   tmdb_id,
            "tmdb_type": tmdb_type,
            "seasons":   omdb.get("totalSeasons", "N/A"),
            "rated":     omdb.get("Rated", "N/A"),
            "type":      omdb.get("Type", "N/A"),
        }
    if tmdb:
        title  = tmdb.get("name") or tmdb.get("title") or raw_name
        year   = (tmdb.get("first_air_date") or tmdb.get("release_date") or "")[:4] or "N/A"
        poster = f"https://image.tmdb.org/t/p/w185{tmdb['poster_path']}" if tmdb.get("poster_path") else ""
        return {
            "folder":    raw_name,
            "title":     title,
            "year":      year,
            "genre":     "N/A",
            "imdb":      "N/A",
            "votes":     "N/A",
            "plot":      tmdb.get("overview", ""),
            "poster":    poster,
            "imdb_id":   "",
            "tmdb_id":   tmdb_id,
            "tmdb_type": tmdb_type,
            "seasons":   "N/A",
            "rated":     "N/A",
            "type":      tmdb.get("media_type", "N/A"),
        }
    return {
        "folder": raw_name, "title": raw_name,
        "year": "N/A", "genre": "N/A", "imdb": "N/A",
        "votes": "N/A", "plot": "", "poster": "",
        "imdb_id": "", "tmdb_id": "", "tmdb_type": "",
        "seasons": "N/A", "rated": "N/A", "type": "N/A",
    }

# ---------------------------------------------------------------------------
# Folder scan
# ---------------------------------------------------------------------------


def scan_folder(folder: str) -> list:
    try:
        entries = list(os.scandir(folder))
    except PermissionError:
        sys.exit(f"[ERROR] Cannot access: {folder}")
    dirs  = sorted(e.name for e in entries if e.is_dir()  and not e.name.startswith('.'))
    files = sorted(e.name for e in entries if e.is_file() and not e.name.startswith('.')
                   and os.path.splitext(e.name)[1].lower() in VIDEO_EXTS)
    return dirs + files


def is_category_folder(path: str) -> bool:
    """True if path is a grouping/collection folder, not a media item itself.

    Rules:
     - multiple video files directly inside → collection
     - subdirs that don't look like season folders → collection
     - single video file or season dirs → media item
    """
    try:
        entries = list(os.scandir(path))
    except PermissionError:
        return False
    video_files = [e for e in entries if e.is_file() and not e.name.startswith('.')
                   and os.path.splitext(e.name)[1].lower() in VIDEO_EXTS]
    if len(video_files) > 1:
        return True
    if video_files:
        return False
    subdirs = [e.name for e in entries if e.is_dir() and not e.name.startswith('.')]
    if not subdirs:
        return False
    if any(SEASON_DIR_PATTERN.match(d) for d in subdirs):
        return False
    return True

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
    output_path_early = os.path.abspath(args.output)
    if not args.force and os.path.exists(output_path_early):
        try:
            with open(output_path_early, encoding="utf-8") as f:
                cached_records = json.load(f)
            cache = {r["folder"]: r for r in cached_records if (r.get("imdb", "N/A") != "N/A" or r.get("tmdb_id")) and r.get("folder")}
            print(f"Cache:    {len(cache)} already-rated entries loaded (use --force to rescan all)\n")
        except Exception as e:
            print(f"[WARN] Could not load cache from {output_path_early}: {e}", file=sys.stderr)

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
        d = scan_folder(folder)
        direct = []
        for name in d:
            sub_path = os.path.join(folder, name)
            if os.path.isdir(sub_path) and args.media_type == "movie" and is_category_folder(sub_path):
                sub = scan_folder(sub_path)
                print(f"  {len(sub):3d} entries in {sub_path} (Unterordner)")
                all_entries.extend((sub_path, x) for x in sub)
            else:
                direct.append(name)
        if direct:
            print(f"  {len(direct):3d} entries in {folder}")
            all_entries.extend((folder, x) for x in direct)

    seen_names = set()
    unique_entries = []
    for folder, raw in all_entries:
        if raw not in seen_names:
            seen_names.add(raw)
            unique_entries.append((folder, raw))
        else:
            print(f"[SKIP] Duplicate: {raw!r}")
    print(f"\nTotal: {len(unique_entries)} entries to process.\n")

    folder_map = {r: f for f, r in unique_entries}
    dirs       = [r for _, r in unique_entries]

    scan_start    = datetime.now()
    records       = []
    errors        = []
    scan_log      = []
    missing_log   = []
    complete_log  = []

    for i, raw in enumerate(dirs, 1):
        folder = folder_map[raw]

        if raw in cache:
            record = cache[raw]
            print(f"[{i:03}/{len(dirs):03}] {raw!r:50s} → CACHED ({record['title']})")
            records.append(record)
            if record.get('imdb', 'N/A') == 'N/A':
                errors.append({"folder": raw, "searched_as": record.get('title', raw),
                               "year_used": record.get('year', ''), "override": True,
                               "matched_title": record.get('title', ''), "matched_year": record.get('year', ''),
                               "imdb": "N/A", "found": False})
            continue

        resolved   = resolve_title(raw)
        cleaned    = resolved[0]
        year       = resolved[1]
        direct_id  = resolved[2] if len(resolved) > 2 else ""   # optional direct IMDb ID
        stem_clean = re.sub(r'\s*\[.*', '', os.path.splitext(raw)[0]).strip()
        override_used = any(stem_clean.lower().startswith(k.replace('.', ' ').lower()) for k in TITLE_OVERRIDES)
        tag = " [OVERRIDE]" if override_used else ""
        print(f"[{i:03}/{len(dirs):03}] {raw!r:50s} → {cleaned!r} ({year}){tag}")

        omdb_result  = None
        tmdb_result  = None
        imdb_id      = ""
        rtype        = "movie" if args.media_type == "movie" else "tv"

        if direct_id:
            # Direct IMDb ID in override → try OMDb first
            imdb_id     = direct_id
            omdb_result = omdb_by_id(imdb_id, omdb_key)
            time.sleep(REQUEST_DELAY)
            # Always fetch TMDB to get tmdb_id (needed for TMDB button and episode check)
            tmdb_result, tmdb_imdb_id, rtype = tmdb_find(cleaned, tmdb_key, args.media_type, year=year)
            if not omdb_result and tmdb_imdb_id:
                # OMDb doesn't have direct_id yet → fall back to TMDB-derived imdb_id
                imdb_id     = tmdb_imdb_id
                omdb_result = omdb_by_id(imdb_id, omdb_key)
                time.sleep(REQUEST_DELAY)
        else:
            # Step 1: TMDB search with year for precision, fallback without
            tmdb_result, imdb_id, rtype = tmdb_find(cleaned, tmdb_key, args.media_type, year=year)

            if imdb_id:
                # Step 2: OMDb lookup by IMDb ID (exact, no ambiguity)
                omdb_result = omdb_by_id(imdb_id, omdb_key)
                time.sleep(REQUEST_DELAY)

            if not omdb_result and not tmdb_result:
                # Step 3: Last resort — direct OMDb title search
                omdb_result = omdb_by_title(cleaned, omdb_key, args.media_type)
                time.sleep(REQUEST_DELAY)

        record = build_record(raw, tmdb_result, omdb_result)
        record['display_name'] = clean_name(raw)[0]  # cleaned folder/file name as German display title
        has_imdb = record['imdb'] != 'N/A'
        found_it = has_imdb or bool(record.get('tmdb_id'))  # "found" for cache purposes
        status = f"IMDb: {record['imdb']}" if has_imdb else ("TMDB only" if found_it else "NOT FOUND")
        print(f"           → {record['title']} ({record['year']}) | {status}")

        scan_log.append({
            "index":         i,
            "folder":        raw,
            "searched_as":   cleaned,
            "year_used":     year,
            "override":      override_used,
            "matched_title": record['title'],
            "matched_year":  record['year'],
            "imdb":          record['imdb'],
            "found":         found_it,
        })
        if not has_imdb:
            errors.append(scan_log[-1])

        # Episode check (series only)
        if args.check_episodes and tmdb_result and rtype == "tv":
            tmdb_id = tmdb_result.get("id")
            total_seasons = int(record.get("seasons") or 0) if record.get("seasons", "N/A") != "N/A" else 0
            if tmdb_id and total_seasons > 0:
                series_path = os.path.join(folder, raw)
                if not os.path.isdir(series_path):
                    continue
                ep_info = check_missing_episodes(series_path, tmdb_id, total_seasons, tmdb_key)
                record["episode_check"] = ep_info
                if ep_info["has_missing"]:
                    gaps = []
                    for sn, det in ep_info["season_details"].items():
                        if det["missing"]:
                            gaps.append(f"S{int(sn):02d}: missing {det['missing']}")
                    print(f"           ⚠ Missing: {'; '.join(gaps)}")
                    missing_log.append({
                        "folder":   raw,
                        "title":    record["title"],
                        "year":     record["year"],
                        "seasons":  ep_info["season_details"],
                    })
                else:
                    disk_s = ep_info["seasons_on_disk"]
                    if disk_s:
                        print(f"           ✓ All episodes present (seasons {disk_s})")
                        total_eps = sum(det["expected"] for det in ep_info["season_details"].values())
                        complete_log.append({
                            "title":        record["title"],
                            "year":         record["year"],
                            "imdb":         record["imdb"],
                            "num_seasons":  len(ep_info["season_details"]),
                            "total_eps":    total_eps,
                        })
                    else:
                        print(f"           ? No episode files detected on disk")

        records.append(record)
        time.sleep(REQUEST_DELAY)

    output_path = output_path_early
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    found     = sum(1 for r in records if r["imdb"] != "N/A")
    not_found = len(records) - found
    scan_end  = datetime.now()
    duration  = scan_end - scan_start

    print(f"\nDone! {found} rated, {not_found} not found.")
    print(f"Output → {output_path}")

    out_dir = os.path.dirname(os.path.dirname(output_path))

    # ── Scan log (every entry) ──────────────────────────────────────────────
    logs_dir = os.path.join(out_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    date_str  = scan_end.strftime("%Y-%m-%d_%H-%M-%S")
    scan_path = os.path.join(logs_dir, f"{date_str}_scanlog.txt")
    with open(scan_path, "w", encoding="utf-8") as sf:
        sf.write("fetch_ratings — scan log\n")
        sf.write(f"Date       : {scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
        sf.write(f"Duration   : {str(duration).split('.')[0]}\n")
        sf.write(f"Mode       : --type {args.media_type}\n")
        sf.write(f"Output     : {output_path}\n")
        sf.write(f"Folders    :\n")
        for p in folders:
            sf.write(f"  • {p}\n")
        sf.write(f"Total      : {len(scan_log)} scanned | {found} rated | {not_found} not found\n")
        sf.write("=" * 70 + "\n\n")
        for e in scan_log:
            ov  = " [OVERRIDE]" if e["override"] else ""
            ok  = "✓" if e["found"] else "✗"
            sf.write(f"[{e['index']:03}] {ok} {e['folder']}\n")
            sf.write(f"       searched : {e['searched_as']!r}")
            if e["year_used"]:
                sf.write(f"  year={e['year_used']}")
            sf.write(f"{ov}\n")
            if e["found"]:
                sf.write(f"       result  : {e['matched_title']} ({e['matched_year']}) | IMDb {e['imdb']}\n")
            else:
                sf.write(f"       result  : NOT FOUND\n")
            sf.write("\n")
    print(f"📋 Scan log  → {scan_path}")

    # ── Error log (only NOT FOUND) ──────────────────────────────────────────
    error_path = os.path.join(logs_dir, f"{date_str}_error.txt")
    with open(error_path, "w", encoding="utf-8") as ef:
        ef.write("fetch_ratings — error log\n")
        ef.write(f"Date    : {scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
        ef.write(f"Output  : {output_path}\n")
        ef.write(f"Total   : {len(scan_log)} scanned | {found} rated | {not_found} not found\n")
        ef.write("=" * 70 + "\n\n")
        if errors:
            for e in errors:
                ef.write(f"NOT FOUND: {e['folder']}\n")
                ef.write(f"  Searched as : {e['searched_as']!r}")
                if e["year_used"]:
                    ef.write(f"  (year: {e['year_used']})")
                ef.write("\n")
                ef.write(f"  → Add to TITLE_OVERRIDES to fix\n\n")
        else:
            ef.write("No errors — all entries found!\n")
    if errors:
        print(f"⚠  Error log → {error_path}  ({not_found} not found)")
    else:
        print(f"✓  No errors — {error_path}")

    # ── Missing episodes log ────────────────────────────────────────────────
    if args.check_episodes:
        missing_path = os.path.join(logs_dir, f"{date_str}_missing_episodes.txt")
        with open(missing_path, "w", encoding="utf-8") as mf:
            mf.write("fetch_ratings — missing episodes log\n")
            mf.write(f"Date    : {scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
            mf.write(f"Total   : {len(missing_log)} series with missing episodes\n")
            mf.write("=" * 70 + "\n\n")
            if missing_log:
                for entry in missing_log:
                    mf.write(f"{entry['title']} ({entry['year']})  [{entry['folder']}]\n")
                    for sn, det in entry["seasons"].items():
                        if det["missing"]:
                            mf.write(f"  S{int(sn):02d}: {det['found']}/{det['expected']} vorhanden — fehlt: {det['missing']}\n")
                    mf.write("\n")
            else:
                mf.write("Keine fehlenden Episoden gefunden!\n")
        if missing_log:
            print(f"⚠  Missing log → {missing_path}  ({len(missing_log)} Serien betroffen)")
        else:
            print(f"✓  Keine fehlenden Episoden — {missing_path}")

        # ── Complete series log ─────────────────────────────────────────────
        complete_path = os.path.join(logs_dir, f"{date_str}_complete.txt")
        with open(complete_path, "w", encoding="utf-8") as cf:
            cf.write("fetch_ratings — complete series log\n")
            cf.write(f"Date    : {scan_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
            cf.write(f"Total   : {len(complete_log)} vollständige Serien\n")
            cf.write("=" * 70 + "\n\n")
            if complete_log:
                for entry in sorted(complete_log, key=lambda x: x["title"].lower()):
                    cf.write(f"{entry['title']} ({entry['year']})\n")
                    cf.write(f"  IMDb-Bewertung : {entry['imdb']}\n")
                    cf.write(f"  Staffeln       : {entry['num_seasons']}\n")
                    cf.write(f"  Folgen gesamt  : {entry['total_eps']}\n")
                    cf.write("\n")
            else:
                cf.write("Keine vollständigen Serien gefunden.\n")
        print(f"✓  Complete log → {complete_path}  ({len(complete_log)} Serien)")

    print(f"\nOpen series_viewer.html to explore the results.")


if __name__ == "__main__":
    main()

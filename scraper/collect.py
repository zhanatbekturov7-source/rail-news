"""
КТТ Rail-News: сборщик новостей о ЖД-транспорте и логистике.

Забирает материалы из Google News (тематические запросы) и прямых RSS,
фильтрует, размечает рубриками и регионами, убирает дубли и пишет
site/data/news.json — его читает сайт. Старые новости сохраняются
в том же файле, поэтому архив копится между запусками.

Запуск: python scraper/collect.py
"""

from __future__ import annotations

import calendar
import os
import hashlib
import html
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote_plus

import feedparser
import requests
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config.yaml"
OUT_PATH = ROOT / "site" / "data" / "news.json"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 rail-news-bot"
)
TIMEOUT = 25

GN_PARAMS = {
    "ru": "hl=ru&gl=KZ&ceid=KZ:ru",
    "en": "hl=en-US&gl=US&ceid=US:en",
}


# --------------------------------------------------------------------------
# Ключевые слова
# --------------------------------------------------------------------------

def compile_terms(terms: list[str]) -> re.Pattern | None:
    """Слово с '=' или короче 4 символов ищется целиком, остальное — как начало слова."""
    parts = []
    for raw in terms:
        t = str(raw).strip().lower()
        if not t:
            continue
        whole = t.startswith("=") or len(t) <= 3
        t = t.lstrip("=")
        esc = re.escape(t)
        parts.append(rf"(?<!\w){esc}(?!\w)" if whole else rf"(?<!\w){esc}")
    return re.compile("|".join(parts), re.IGNORECASE) if parts else None


def tag(text: str, groups: dict[str, re.Pattern]) -> list[str]:
    return [name for name, rx in groups.items() if rx and rx.search(text)]


# --------------------------------------------------------------------------
# Загрузка
# --------------------------------------------------------------------------

def google_news_url(query: str, lang: str) -> str:
    q = query if "when:" in query else f"{query} when:3d"
    return f"https://news.google.com/rss/search?q={quote_plus(q)}&{GN_PARAMS[lang]}"


def fetch(url: str) -> feedparser.FeedParserDict:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    resp.raise_for_status()
    parsed = feedparser.parse(resp.content)
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"не удалось разобрать ленту: {parsed.bozo_exception}")
    return parsed


def clean_text(value: str | None, limit: int = 320) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        text = text[:limit].rsplit(" ", 1)[0] + "…"
    return text


def entry_time(entry) -> str:
    for key in ("published_parsed", "updated_parsed"):
        st = entry.get(key)
        if st:
            ts = calendar.timegm(st)
            # защита от дат из будущего
            ts = min(ts, time.time())
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_entries(parsed, source: dict) -> list[dict]:
    items = []
    is_gn = source["kind"] == "google"
    for e in parsed.entries:
        title = clean_text(e.get("title"), 400)
        link = e.get("link", "").strip()
        if not title or not link:
            continue

        outlet = source["name"]
        if is_gn:
            src = e.get("source") or {}
            outlet = (src.get("title") or "").strip()
            # Google News добавляет " - Издание" в конец заголовка
            if outlet and title.endswith(f" - {outlet}"):
                title = title[: -(len(outlet) + 3)].strip()
            elif " - " in title and not outlet:
                title, outlet = title.rsplit(" - ", 1)
            summary = ""  # у Google News описание = заголовок + ссылки
        else:
            summary = clean_text(e.get("summary") or e.get("description"))
            if summary.lower().startswith(title.lower()[:60]):
                summary = ""

        items.append({
            "title": title,
            "url": link,
            "source": outlet or source["name"],
            "published": entry_time(e),
            "summary": summary,
            "lang": source["lang"],
            "via": "Google News" if is_gn else source["name"],
            "_filter": source.get("filter", False),
            "q_topics": [source["topic"]] if source.get("topic") else [],
        })
    return items


def load_source(source: dict) -> tuple[dict, list[dict]]:
    started = time.time()
    status = {"name": source["name"], "kind": source["kind"], "ok": False, "count": 0, "error": ""}
    try:
        parsed = fetch(source["url"])
        items = parse_entries(parsed, source)
        status.update(ok=True, count=len(items))
        return status, items
    except Exception as exc:  # noqa: BLE001 — один упавший источник не должен ронять сборку
        status["error"] = f"{type(exc).__name__}: {exc}"[:200]
        return status, []
    finally:
        status["seconds"] = round(time.time() - started, 1)


# --------------------------------------------------------------------------
# Дедупликация
# --------------------------------------------------------------------------

WORD_RX = re.compile(r"\w+", re.UNICODE)


def title_tokens(title: str) -> frozenset[str]:
    return frozenset(w for w in WORD_RX.findall(title.lower()) if len(w) > 2)


def item_id(item: dict) -> str:
    key = " ".join(sorted(title_tokens(item["title"]))) or item["url"]
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def is_near_duplicate(a: frozenset, b: frozenset) -> bool:
    if not a or not b:
        return False
    inter = len(a & b)
    return inter / len(a | b) >= 0.75 or inter / min(len(a), len(b)) >= 0.9


def deduplicate(items: list[dict]) -> list[dict]:
    """Оставляет одну запись на сюжет. Приоритет — запись с описанием и более ранней датой."""
    items.sort(key=lambda x: (x["published"]))
    kept: list[dict] = []
    seen_urls: set[str] = set()
    recent: list[tuple[frozenset, datetime, dict]] = []

    for it in items:
        if it["url"] in seen_urls:
            continue
        toks = title_tokens(it["title"])
        ts = datetime.fromisoformat(it["published"])
        dup = None
        for toks2, ts2, other in recent:
            if abs((ts - ts2).total_seconds()) <= 72 * 3600 and is_near_duplicate(toks, toks2):
                dup = other
                break
        if dup:
            if not dup.get("summary") and it.get("summary"):
                dup["summary"] = it["summary"]
            for t in it.get("q_topics") or []:
                if t not in dup.setdefault("q_topics", []):
                    dup["q_topics"].append(t)
            others = dup.setdefault("also", [])
            if it["source"] != dup["source"] and it["source"] not in others and len(others) < 5:
                others.append(it["source"])
            continue
        seen_urls.add(it["url"])
        kept.append(it)
        recent.append((toks, ts, it))
        recent = [r for r in recent if (ts - r[1]).total_seconds() <= 72 * 3600]
    return kept


# --------------------------------------------------------------------------
# Основной сценарий
# --------------------------------------------------------------------------

def main() -> int:
    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))

    topics_cfg = cfg.get("topics") or []
    topic_names = [t["name"] for t in topics_cfg]
    topics = {t["name"]: compile_terms(t.get("keywords") or []) for t in topics_cfg}
    regions = {name: compile_terms(t) for name, t in (cfg.get("regions") or {}).items()}
    relevance = compile_terms(cfg.get("relevance") or [])
    default_topic = cfg.get("default_topic") or (topic_names[-1] if topic_names else "Прочее")

    sources = []
    for t in topics_cfg:
        for lang in ("ru", "en"):
            for q in t.get(f"search_{lang}") or []:
                sources.append({
                    "name": f"Google News: {q}",
                    "url": google_news_url(q, lang),
                    "lang": lang,
                    "kind": "google",
                    "filter": False,
                    "topic": t["name"],
                })
    for f in cfg.get("feeds") or []:
        sources.append({**f, "kind": "rss", "lang": f.get("lang", "ru")})

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(load_source, sources))

    statuses = [s for s, _ in results]
    fresh: list[dict] = []
    for _, items in results:
        for it in items:
            text = f"{it['title']} {it['summary']}"
            if it.pop("_filter") and relevance and not relevance.search(text):
                continue
            fresh.append(it)

    # Подмешиваем архив прошлых запусков
    archive: list[dict] = []
    if OUT_PATH.exists():
        try:
            archive = json.loads(OUT_PATH.read_text(encoding="utf-8")).get("items", [])
        except (json.JSONDecodeError, OSError):
            print("Архив повреждён, начинаю с нуля", file=sys.stderr)

    cutoff = datetime.now(timezone.utc) - timedelta(days=int(cfg.get("keep_days", 45)))
    merged = [
        it for it in archive + fresh
        if datetime.fromisoformat(it["published"]) >= cutoff
    ]
    merged = deduplicate(merged)
    for it in merged:
        it["id"] = it.get("id") or item_id(it)
        text = f"{it['title']} {it.get('summary', '')}"
        # тема по словам в тексте; если слов нет — тема запроса, который нашёл новость
        found = set(tag(text, topics)) or {t for t in it.get("q_topics", []) if t in topics}
        it["categories"] = [t for t in topic_names if t in found] or [default_topic]
        it["regions"] = tag(text, regions)
    merged.sort(key=lambda x: x["published"], reverse=True)
    merged = merged[: int(cfg.get("max_items", 3000))]

    ok = sum(1 for s in statuses if s["ok"])
    if ok == 0 and archive:
        print("Ни один источник не ответил — архив оставлен без изменений", file=sys.stderr)
        return 1

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "new_this_run": len({i["url"] for i in fresh} - {i["url"] for i in archive}),
        "title": cfg.get("site_title", "КТТ Rail-News"),
        "topics": topic_names,
        "regions": list(regions.keys()),
        "repo": os.environ.get("GITHUB_REPOSITORY", ""),
        "items": merged,
        "sources": statuses,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"Источники: {ok}/{len(statuses)} ответили")
    for s in statuses:
        mark = "ok " if s["ok"] else "ERR"
        print(f"  [{mark}] {s['name'][:70]:70} {s['count']:>4}  {s['error']}")
    print(f"Свежих записей: {len(fresh)}, в ленте после чистки: {len(merged)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

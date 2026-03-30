from __future__ import annotations

import csv
import datetime as dt
import difflib
import hashlib
import html
import logging
import re
import sys
from pathlib import Path
from typing import Iterable, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_FILE = PROJECT_DIR / "data" / "urls.csv"
SAVE_DIR = PROJECT_DIR / "saved_data"
TEXT_DIR = SAVE_DIR / "text"
LOG_DIR = PROJECT_DIR / "output"
LOG_FILE = LOG_DIR / "diff.log"


def make_snapshot_name(description: str, url: str) -> str:
    """
    description を基準にスナップショット名を決定する。
    description が無い場合のみ URL のハッシュを使う。
    """
    base = description.strip()
    if base:
        # ファイル名として問題になる文字だけを置換。description を最大限維持する。
        safe = re.sub(r'[\\/:*?"<>|]+', "_", base)
        safe = safe.strip().rstrip(".")
        if safe:
            return f"{safe}.txt"

    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"url_{digest}.txt"


def legacy_ascii_slug(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", text.strip())
    slug = slug.strip("-_")
    return slug.lower() or "url"


def find_legacy_snapshot(description: str) -> Path | None:
    """
    旧命名（NN_slug.txt）を探索して、description ベース命名へ移行しやすくする。
    """
    if not description:
        return None
    slug = legacy_ascii_slug(description)
    candidates = sorted(TEXT_DIR.glob(f"*_{slug}.txt"))
    return candidates[-1] if candidates else None


def read_urls(csv_path: Path) -> Iterable[Tuple[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = (row.get("url") or "").strip()
            description = (row.get("description") or "").strip()
            if not url:
                continue
            yield url, description


def fetch_html(url: str) -> str:
    headers = {"User-Agent": "html-diff-bot/1.0 (+script)"}
    req = Request(url, headers=headers)
    with urlopen(req, timeout=20) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")


def build_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("html_diff")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # mode="a" で差分ログを追記し続ける
    file_handler = logging.FileHandler(log_path, encoding="utf-8", mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def html_to_text(html_content: str) -> str:
    text = html_content

    text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<noscript\b[^>]*>.*?</noscript>", " ", text, flags=re.I | re.S)

    text = re.sub(
        r"</?(div|section|article|main|aside|header|footer|nav|li|ul|ol|p|br|h[1-6]|table|tr|td|th)\b[^>]*>",
        "\n",
        text,
        flags=re.I,
    )

    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)

    lines = []
    for line in text.splitlines():
        normalized = re.sub(r"\s+", " ", line).strip()
        if normalized:
            lines.append(normalized)

    return "\n".join(lines)


def diff_snapshots(old: str, new: str) -> str:
    return "\n".join(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            fromfile="previous",
            tofile="current",
            lineterm="",
        )
    )


def process_url(url: str, description: str, logger: logging.Logger) -> None:
    label = description or url
    filename = make_snapshot_name(description, url)
    text_path = TEXT_DIR / filename

    try:
        html_new = fetch_html(url)
    except (HTTPError, URLError) as exc:
        logger.error("Failed to fetch %s (%s): %s", url, label, exc)
        return
    except Exception:
        logger.exception("Unexpected error while fetching %s", url)
        return

    text_new = html_to_text(html_new)

    old_text = None
    source_path = text_path
    if text_path.exists():
        old_text = text_path.read_text(encoding="utf-8", errors="replace")
    else:
        legacy = find_legacy_snapshot(description)
        if legacy and legacy.exists():
            old_text = legacy.read_text(encoding="utf-8", errors="replace")
            source_path = legacy

    if old_text is None:
        logger.info("%s: new text snapshot saved as %s", label, text_path.name)
    elif old_text == text_new:
        logger.info("%s: no text change", label)
    else:
        diff = diff_snapshots(old_text, text_new)
        logger.info("%s: content changed; writing diff below", label)
        logger.info("Diff for %s:\n%s", source_path.name, diff)

    text_path.write_text(text_new, encoding="utf-8")


def main() -> None:
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = build_logger(LOG_FILE)
    logger.info("Run started (UTC %s)", dt.datetime.utcnow().isoformat())

    if not DATA_FILE.exists():
        logger.error("CSV file not found at %s", DATA_FILE)
        return

    for url, description in read_urls(DATA_FILE):
        process_url(url, description, logger)

    logger.info("Run finished")


if __name__ == "__main__":
    main()

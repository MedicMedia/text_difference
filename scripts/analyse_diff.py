from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List
from urllib import error, request


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_DIR / "output"
LOG_PATH = OUTPUT_DIR / "clean_diff.log"
DOTENV_PATH = PROJECT_DIR / ".env.local"
DATA_FILE = PROJECT_DIR / "data" / "urls.csv"

DEFAULT_MODEL = "gemini-2.5-flash"
OUTPUT_MD = OUTPUT_DIR / "gemini_summary.md"
TOO_MANY_MESSAGE = "変更が多いので、直接サイトを確認してください。"

TS_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[[^\]]+\] (?P<msg>.*)$"
)


@dataclass
class TargetRow:
    description: str
    url: str


@dataclass
class CleanEntry:
    description: str
    timestamp: str = ""
    from_timestamp: str = ""
    to_timestamp: str = ""
    status: str = "no_change"
    url: str = ""
    filename: str = ""
    diff_lines: List[str] = field(default_factory=list)


def load_api_key() -> str:
    env_key = os.getenv("GEMINI_API_KEY")
    if env_key:
        return env_key.strip()

    if DOTENV_PATH.exists():
        for line in DOTENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("GEMINI_API_KEY"):
                _, _, val = line.partition("=")
                return val.strip().strip("\"'")

    raise RuntimeError("GEMINI_API_KEY が環境変数にも .env.local にも見つかりませんでした。")


def load_targets(csv_path: Path) -> List[TargetRow]:
    rows: List[TargetRow] = []
    if not csv_path.exists():
        return rows
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = (row.get("url") or "").strip()
            description = (row.get("description") or "").strip()
            if not url:
                continue
            rows.append(TargetRow(description=description or url, url=url))
    return rows


def parse_clean_log(lines: List[str]) -> Dict[str, CleanEntry]:
    entries: Dict[str, CleanEntry] = {}
    current: CleanEntry | None = None
    collecting_diff = False

    def finalize() -> None:
        nonlocal current, collecting_diff
        if current is not None:
            entries[current.description] = current
        current = None
        collecting_diff = False

    for line in lines:
        m = TS_RE.match(line)
        if m:
            msg = m.group("msg").strip()
            ts = m.group("ts")

            if msg.startswith("URL: "):
                if current is not None:
                    current.url = msg.split(":", 1)[1].strip()
                continue

            if msg.startswith("Change Window: "):
                if current is not None:
                    body = msg.split(":", 1)[1].strip()
                    if "->" in body:
                        left, right = body.split("->", 1)
                        current.from_timestamp = left.strip()
                        current.to_timestamp = right.strip()
                continue

            if msg.startswith("Diff for "):
                if current is not None:
                    current.filename = msg.split("Diff for ", 1)[1].rstrip(":").strip()
                    collecting_diff = True
                continue

            # 新しいターゲット行
            finalize()

            if ": " not in msg:
                continue
            description, status = msg.split(": ", 1)
            description = description.strip()
            status = status.strip().lower()

            entry = CleanEntry(description=description, timestamp=ts)
            if "no significant content changed" in status or "no content changed" in status:
                entry.status = "no_change"
            elif "content changed" in status:
                entry.status = "changed"
            else:
                entry.status = "no_change"
            current = entry
            continue

        if current is not None and collecting_diff:
            current.diff_lines.append(line)

    finalize()
    return entries


def sanitize_text(text: str) -> str:
    t = text.replace("|", " ").replace("\n", " ").strip()
    t = re.sub(r"\s+", " ", t)
    return t


def filter_diff_lines(diff_lines: List[str]) -> List[str]:
    out: List[str] = []
    for line in diff_lines:
        s = line.rstrip()
        if not s:
            continue
        if s.startswith(("---", "+++", "@@")):
            continue
        if s.startswith("+") or s.startswith("-"):
            out.append(s)
    return out


def truncate_lines(lines: List[str], max_chars: int) -> str:
    text = "\n".join(lines)
    if len(text) <= max_chars:
        return text
    half = max(1, max_chars // 2)
    return text[:half] + "\n...\n[truncated]\n...\n" + text[-half:]


def build_prompt(description: str, url: str, timestamp: str, diff_text: str) -> str:
    time_range = timestamp
    if "->" not in time_range and " ～ " not in time_range and " to " not in time_range:
        time_range = f"- -> {timestamp}" if timestamp else "-"

    return "\n".join(
        [
            "次は1つのページの重要なテキスト差分です。",
            "重要な変更だけを抽出してください。",
            "出力は次のどちらかのみ。",
            "1) 重要変更なし: NO_IMPORTANT_CHANGE",
            "2) 重要変更あり: IMPORTANT: <要約1><br><要約2><br><要約3>",
            "各要約は簡潔に『変更前 → 変更後』形式。",
            "最大3件、補足説明は禁止。",
            "対象 description: " + description,
            "対象 URL: " + url,
            "変更期間: " + time_range,
            "差分:",
            diff_text,
        ]
    )


def call_gemini(api_key: str, prompt: str, model: str, timeout: int) -> str:
    body = {"contents": [{"parts": [{"text": prompt}]}]}
    data = json.dumps(body).encode("utf-8")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as e:
        raise RuntimeError(f"Gemini API HTTPError: {e.code} {e.reason}") from e
    except error.URLError as e:
        raise RuntimeError(f"Gemini API URLError: {e.reason}") from e

    candidates = payload.get("candidates") or []
    if not candidates:
        raise RuntimeError(f"Gemini API response has no candidates: {payload}")
    parts = candidates[0].get("content", {}).get("parts", [])
    text_parts = [p.get("text", "") for p in parts if "text" in p]
    return "\n".join(text_parts).strip()


def parse_summary_items(raw: str) -> List[str]:
    if not raw:
        return []
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    for line in lines:
        if line.upper().startswith("NO_IMPORTANT_CHANGE"):
            return []
    for line in lines:
        if line.upper().startswith("IMPORTANT:"):
            body = sanitize_text(line.split(":", 1)[1].strip())
            if not body:
                return []
            items = [p.strip() for p in body.split("<br>") if p.strip()]
            return items[:3]
    fallback = sanitize_text(lines[0] if lines else "")
    return [fallback] if fallback else []


def log_query(target: str, prompt: str) -> None:
    print("\n" + "=" * 30)
    print(f"[Gemini Query] target={target}")
    print("-" * 30)
    print(prompt)
    print("=" * 30)


def log_response(target: str, response: str) -> None:
    print("\n" + "=" * 30)
    print(f"[Gemini Response] target={target}")
    print("-" * 30)
    print(response)
    print("=" * 30)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="clean_diff.log を Gemini で要約し gemini_summary.md を作成する"
    )
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--max-diff-chars",
        type=int,
        default=4500,
        help="Geminiへ送る差分文字数上限",
    )
    parser.add_argument(
        "--max-diff-lines",
        type=int,
        default=260,
        help="Geminiへ送る差分行数上限。超えた場合は直接確認メッセージにする",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not LOG_PATH.exists():
        print(f"ログファイルが見つかりません: {LOG_PATH}", file=sys.stderr)
        sys.exit(1)

    targets = load_targets(DATA_FILE)
    clean_entries = parse_clean_log(LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines())
    if not targets:
        print("urls.csv が空です。")
        OUTPUT_MD.write_text("", encoding="utf-8")
        return

    api_key: str | None = None
    sections: List[str] = []

    for target in targets:
        desc = target.description
        url = target.url
        entry = clean_entries.get(desc, CleanEntry(description=desc, url=url))
        if not entry.url:
            entry.url = url

        block = [
            f"## {desc}",
            f"- URL: {entry.url}",
            (
                f"- 変更期間: {entry.from_timestamp or '-'} -> "
                f"{entry.to_timestamp or (entry.timestamp if entry.status == 'changed' else '-')}"
            ),
        ]

        if entry.status != "changed":
            block.append("- 変更内容: 変更なし")
            sections.append("\n".join(block))
            continue

        diff_lines = filter_diff_lines(entry.diff_lines)
        if len(diff_lines) > args.max_diff_lines:
            block.append(f"- 変更内容: {TOO_MANY_MESSAGE}")
            sections.append("\n".join(block))
            continue

        diff_text = truncate_lines(diff_lines, args.max_diff_chars)
        if len(diff_text) > args.max_diff_chars:
            block.append(f"- 変更内容: {TOO_MANY_MESSAGE}")
            sections.append("\n".join(block))
            continue

        try:
            if api_key is None:
                api_key = load_api_key()
            range_text = (
                f"{entry.from_timestamp or '-'} -> "
                f"{entry.to_timestamp or entry.timestamp}"
            )
            prompt = build_prompt(desc, entry.url, range_text, diff_text)
            log_query(desc, prompt)
            raw = call_gemini(api_key, prompt, args.model, timeout=args.timeout)
            log_response(desc, raw)
            items = parse_summary_items(raw)
        except Exception as exc:
            log_response(desc, f"ERROR: {exc}")
            items = [f"Gemini要約失敗: {sanitize_text(str(exc))}"]

        if not items:
            block.append("- 変更内容: 重要な変更なし")
        else:
            block.append("- 変更内容:")
            for item in items:
                block.append(f"  - {item}")

        sections.append("\n".join(block))

    result = "\n\n".join(sections)
    OUTPUT_MD.write_text(result, encoding="utf-8")
    print(f"要約を出力しました: {OUTPUT_MD}")


if __name__ == "__main__":
    main()

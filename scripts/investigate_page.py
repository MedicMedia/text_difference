from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List
from urllib import error, request


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_DIR / "output"
SUMMARY_MD = OUTPUT_DIR / "gemini_summary.md"
CLEAN_LOG = OUTPUT_DIR / "clean_diff.log"
DOTENV_PATH = PROJECT_DIR / ".env.local"
OUTPUT_MD = OUTPUT_DIR / "investigate_page.md"

DEFAULT_MODEL = "gemini-2.5-flash"
TS_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[[^\]]+\] (?P<msg>.*)$"
)


@dataclass
class SummaryTarget:
    description: str
    url: str = ""
    period: str = "-"
    changed: bool = False
    brief_items: List[str] = field(default_factory=list)


@dataclass
class CleanChange:
    description: str
    timestamp: str
    from_ts: str = "-"
    to_ts: str = "-"
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
                _, _, value = line.partition("=")
                return value.strip().strip("\"'")

    raise RuntimeError("GEMINI_API_KEY が環境変数にも .env.local にも見つかりませんでした。")


def parse_gemini_summary(path: Path) -> List[SummaryTarget]:
    if not path.exists():
        return []

    targets: List[SummaryTarget] = []
    current: SummaryTarget | None = None

    def finalize() -> None:
        nonlocal current
        if current is not None:
            targets.append(current)
        current = None

    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.rstrip()
        stripped = line.strip()

        if stripped.startswith("## "):
            finalize()
            current = SummaryTarget(description=stripped[3:].strip())
            continue

        if current is None:
            continue

        if stripped.startswith("- URL: "):
            current.url = stripped.split(":", 1)[1].strip()
            continue

        if stripped.startswith("- 変更期間: "):
            current.period = stripped.split(":", 1)[1].strip()
            continue

        if stripped.startswith("- 変更内容: "):
            body = stripped.split(":", 1)[1].strip()
            if not body:
                current.changed = True
                continue
            if "変更なし" in body or "重要な変更なし" in body:
                current.changed = False
                continue
            current.changed = True
            current.brief_items.append(body)
            continue

        if stripped.startswith("- ") and line.startswith("  - "):
            item = stripped[2:].strip()
            if item:
                current.changed = True
                current.brief_items.append(item)

    finalize()
    return targets


def parse_clean_diff(path: Path) -> Dict[str, CleanChange]:
    if not path.exists():
        return {}

    latest: Dict[str, CleanChange] = {}
    current: CleanChange | None = None
    collecting_diff = False

    def finalize() -> None:
        nonlocal current, collecting_diff
        if current is not None:
            latest[current.description] = current
        current = None
        collecting_diff = False

    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = TS_RE.match(line)
        if m:
            msg = m.group("msg").strip()
            ts = m.group("ts")

            if msg.startswith("URL: "):
                continue

            if msg.startswith("Change Window: "):
                if current is not None:
                    window = msg.split(":", 1)[1].strip()
                    if "->" in window:
                        left, right = window.split("->", 1)
                        current.from_ts = left.strip()
                        current.to_ts = right.strip()
                continue

            if msg.startswith("Diff for "):
                if current is not None:
                    collecting_diff = True
                continue

            finalize()
            marker = ": content changed"
            idx = msg.lower().find(marker)
            if idx < 0:
                continue
            desc = msg[:idx].strip()
            current = CleanChange(description=desc, timestamp=ts)
            continue

        if current is not None and collecting_diff:
            if line.startswith(("---", "+++", "@@")):
                continue
            if line.startswith("+") or line.startswith("-"):
                current.diff_lines.append(line)

    finalize()
    return latest


def fetch_html(url: str) -> str:
    req = request.Request(url, headers={"User-Agent": "text-difference-investigator/1.0"})
    with request.urlopen(req, timeout=20) as resp:
        raw = resp.read()
        charset = resp.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")


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
    lines = []
    for ln in text.splitlines():
        normalized = re.sub(r"\s+", " ", ln).strip()
        if normalized:
            lines.append(normalized)
    return "\n".join(lines)


def truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    half = max(1, max_chars // 2)
    return text[:half] + "\n...\n[truncated]\n...\n" + text[-half:]


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
    except error.HTTPError as exc:
        raise RuntimeError(f"Gemini API HTTPError: {exc.code} {exc.reason}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Gemini API URLError: {exc.reason}") from exc

    candidates = payload.get("candidates") or []
    if not candidates:
        raise RuntimeError(f"Gemini API response has no candidates: {payload}")
    parts = candidates[0].get("content", {}).get("parts", [])
    return "\n".join(p.get("text", "") for p in parts if "text" in p).strip()


def build_prompt(target: SummaryTarget, change: CleanChange, page_text: str) -> str:
    brief = "\n".join(f"- {x}" for x in target.brief_items) if target.brief_items else "- (なし)"
    diff_text = "\n".join(change.diff_lines) if change.diff_lines else "(差分ログなし)"
    return "\n".join(
        [
            "あなたはWebページ差分調査のアナリストです。",
            "次の情報をもとに、何がどう変わったかを日本語で詳しく要約してください。",
            "出力形式はMarkdownで、以下を厳守。",
            "1) 先頭に短い総括1文",
            "2) 続いて箇条書き3〜6件",
            "3) 各箇条書きは『変更前 → 変更後』または『新規追加: ...』形式",
            "4) 推測は避け、根拠が弱い場合は『断定不可』と書く",
            "",
            f"対象: {target.description}",
            f"URL: {target.url}",
            f"変更期間: {change.from_ts} -> {change.to_ts}",
            "",
            "[既存の簡易要約]",
            brief,
            "",
            "[clean_diff.log の差分]",
            truncate(diff_text, 4500),
            "",
            "[再取得した最新ページ本文抜粋]",
            truncate(page_text, 5000),
        ]
    )


def normalize_result(raw: str) -> List[str]:
    lines = [ln.rstrip() for ln in raw.splitlines()]
    return [ln for ln in lines if ln.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="gemini_summary.md の変更対象を再調査して詳細要約を出力する"
    )
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary_targets = parse_gemini_summary(SUMMARY_MD)
    changed_targets = [t for t in summary_targets if t.changed]
    clean_changes = parse_clean_diff(CLEAN_LOG)

    sections: List[str] = []
    header = [
        "# 変更詳細レポート",
        f"- 生成日時: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 元ファイル: {SUMMARY_MD.name}",
        "",
    ]

    if not changed_targets:
        body = header + ["変更ありの対象はありませんでした。"]
        OUTPUT_MD.write_text("\n".join(body), encoding="utf-8")
        print(f"詳細レポートを出力しました: {OUTPUT_MD}")
        return

    api_key = load_api_key()

    for target in changed_targets:
        if not target.url:
            sections.append(
                "\n".join(
                    [
                        f"## {target.description}",
                        "- URL: (不明)",
                        "- 詳細要約:",
                        "  - URLが取得できないため調査できませんでした。",
                    ]
                )
            )
            continue

        change = clean_changes.get(
            target.description,
            CleanChange(
                description=target.description,
                timestamp="-",
                from_ts="-",
                to_ts=target.period,
                diff_lines=[],
            ),
        )
        if change.to_ts == "-":
            change.to_ts = target.period

        block = [f"## {target.description}", f"- URL: {target.url}", f"- 変更期間: {change.from_ts} -> {change.to_ts}"]
        try:
            page_text = html_to_text(fetch_html(target.url))
            prompt = build_prompt(target, change, page_text)
            raw = call_gemini(api_key, prompt, args.model, timeout=args.timeout)
            lines = normalize_result(raw)
            if not lines:
                lines = ["総括を生成できませんでした。", "- 断定不可: 応答が空でした。"]
        except Exception as exc:
            fallback = target.brief_items or ["詳細要約に失敗しました。"]
            lines = ["総括: 調査中にエラーが発生したため簡易要約を再掲します。"]
            lines.extend(f"- {item}" for item in fallback)
            lines.append(f"- エラー: {str(exc).strip()}")

        block.append("- 詳細要約:")
        for line in lines:
            if line.startswith("- "):
                block.append(f"  {line}")
            else:
                block.append(f"  - {line}")
        sections.append("\n".join(block))

    OUTPUT_MD.write_text("\n".join(header) + "\n\n" + "\n\n".join(sections), encoding="utf-8")
    print(f"詳細レポートを出力しました: {OUTPUT_MD}")


if __name__ == "__main__":
    main()

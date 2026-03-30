from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple
from urllib import error, request
from urllib.parse import urljoin, urlparse


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

CATEGORIES = [
    "法令・制度改正",
    "薬",
    "看護（臓器別以外）",
    "栄養",
    "介護",
    "リハビリ",
    "消化器",
    "肝胆膵脾",
    "循環器",
    "代謝・内分泌",
    "腎・泌尿器",
    "免疫・アレ膠",
    "血液",
    "感染症",
    "呼吸器",
    "神経",
    "救急",
    "麻酔科",
    "小児科",
    "産科",
    "婦人科",
    "乳腺",
    "眼科",
    "耳鼻咽喉科",
    "整形外科",
    "精神科",
    "皮膚科",
    "放射線科",
    "公衆衛生（法令以外）",
    "基礎医学",
    "その他",
]

ANCHOR_RE = re.compile(
    r"""<a\b[^>]*href\s*=\s*(["'])(?P<href>.*?)\1[^>]*>(?P<text>.*?)</a>""",
    flags=re.I | re.S,
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


@dataclass
class InvestigateItem:
    category: str
    title: str
    start_date: str
    detail: str
    reference_url: str


@dataclass
class RelatedMaterial:
    reference_urls: List[str]
    text_snippets: List[str]


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


def fetch_html(url: str, timeout: int = 20) -> str:
    req = request.Request(url, headers={"User-Agent": "text-difference-investigator/1.0"})
    with request.urlopen(req, timeout=timeout) as resp:
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


def split_diff_lines(diff_lines: List[str]) -> Tuple[List[str], List[str]]:
    adds: List[str] = []
    removes: List[str] = []
    for line in diff_lines:
        if line.startswith("+"):
            text = line[1:].strip()
            if text:
                adds.append(text)
        elif line.startswith("-"):
            text = line[1:].strip()
            if text:
                removes.append(text)
    return adds, removes


def build_change_points(adds: List[str], removes: List[str], max_items: int = 30) -> List[str]:
    points: List[str] = []
    pair_count = min(len(adds), len(removes))
    for i in range(pair_count):
        points.append(f"変更: {removes[i]} -> {adds[i]}")
        if len(points) >= max_items:
            return points
    for text in adds[pair_count:]:
        points.append(f"新規追加: {text}")
        if len(points) >= max_items:
            return points
    for text in removes[pair_count:]:
        points.append(f"削除: {text}")
        if len(points) >= max_items:
            return points
    return points


def clean_anchor_text(text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", text)
    t = html.unescape(t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_links_from_html(base_url: str, html_content: str) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    for m in ANCHOR_RE.finditer(html_content):
        href = m.group("href").strip()
        if not href or href.startswith(("javascript:", "mailto:", "#")):
            continue
        abs_url = urljoin(base_url, href)
        if not abs_url.startswith(("http://", "https://")):
            continue
        text = clean_anchor_text(m.group("text"))
        links.append((abs_url, text))
    return links


def dedupe_urls(urls: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for url in urls:
        key = url.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def extract_url_from_line(text: str) -> List[str]:
    return re.findall(r"https?://[^\s)>\]\"']+", text)


def select_related_links(
    base_url: str,
    adds: List[str],
    anchors: List[Tuple[str, str]],
    max_links: int = 12,
) -> List[str]:
    keywords = ["ガイドライン", "正誤表", "guideline", "recommendation", "pdf", "改訂", "更新"]
    joined_adds = " ".join(adds)

    picked: List[str] = []
    picked.append(base_url)

    for line in adds:
        for url in extract_url_from_line(line):
            picked.append(url)

    for href, text in anchors:
        hay = f"{text} {href}".lower()
        if any(k in hay for k in ["guideline", "pdf"]):
            picked.append(href)
            continue
        if any(k in text for k in keywords):
            picked.append(href)
            continue
        if text and text in joined_adds:
            picked.append(href)

    urls = dedupe_urls(picked)
    return urls[:max_links]


def collect_related_material(
    base_url: str,
    html_content: str,
    adds: List[str],
    timeout: int,
) -> RelatedMaterial:
    anchors = extract_links_from_html(base_url, html_content)
    reference_urls = select_related_links(base_url, adds, anchors)

    text_snippets: List[str] = []
    html_fetch_budget = 3
    html_count = 0

    for url in reference_urls:
        lower = url.lower()
        if lower.endswith(".pdf"):
            continue
        if html_count >= html_fetch_budget:
            break
        try:
            linked_html = fetch_html(url, timeout=timeout)
            linked_text = html_to_text(linked_html)
            if linked_text:
                parsed = urlparse(url)
                source_label = f"{parsed.netloc}{parsed.path}"
                snippet = truncate(linked_text, 2500)
                text_snippets.append(f"[{source_label}]\n{snippet}")
                html_count += 1
        except Exception:
            continue

    return RelatedMaterial(reference_urls=reference_urls, text_snippets=text_snippets)


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


def build_prompt(
    target: SummaryTarget,
    change: CleanChange,
    page_text: str,
    change_points: List[str],
    related: RelatedMaterial,
) -> str:
    brief = "\n".join(f"- {x}" for x in target.brief_items) if target.brief_items else "- (なし)"
    points_text = "\n".join(f"- {x}" for x in change_points) if change_points else "- (抽出なし)"
    refs_text = "\n".join(f"- {u}" for u in related.reference_urls) if related.reference_urls else "- (なし)"
    snippet_text = (
        "\n\n".join(related.text_snippets) if related.text_snippets else "(追加参照ページ本文の取得なし)"
    )
    categories_text = "、".join(CATEGORIES)
    return "\n".join(
        [
            "以下をもとに、変更内容を医療向けの更新情報として抽出してください。",
            "出力は JSON のみ。説明文やMarkdownは禁止。",
            "配列形式で、各要素は次のキーを必須とする:",
            "分野, タイトル, 運用開始日, 詳細, 参照URL",
            "分野は次のいずれかのみ:",
            categories_text,
            "運用開始日は YYYY/MM/DD 形式。日付不明なら '-'。",
            "必ず『新規追加・更新・削除』の情報だけを書く。既存情報や不変情報は出力禁止。",
            "ガイドライン更新が疑われる場合は、改訂/追加点を具体化すること。",
            "参照URLは可能なら詳細ページやPDFのURLを優先すること。",
            "有意な更新が1件も無ければ空配列 [] を返すこと。",
            "",
            f"対象: {target.description}",
            f"URL: {target.url}",
            f"変更期間: {change.from_ts} -> {change.to_ts}",
            "",
            "[既存の簡易要約]",
            brief,
            "",
            "[抽出した変更ポイント（追加/更新/削除）]",
            truncate(points_text, 5000),
            "",
            "[候補参照URL（PDF含む）]",
            refs_text,
            "",
            "[再取得した最新ページ本文抜粋]",
            truncate(page_text, 7000),
            "",
            "[関連ページ本文抜粋]",
            truncate(snippet_text, 7000),
        ]
    )


def extract_json_candidate(raw: str) -> str:
    text = raw.strip()
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, flags=re.S)
    if fenced:
        return fenced.group(1).strip()
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text


def norm(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_items(raw: str, fallback_url: str) -> List[InvestigateItem]:
    candidate = extract_json_candidate(raw)
    data = json.loads(candidate)

    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            rows = data["items"]
        else:
            rows = [data]
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    out: List[InvestigateItem] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        category = norm(row.get("分野")) or "その他"
        if category not in CATEGORIES:
            category = "その他"

        title = norm(row.get("タイトル")) or "（タイトル不明）"
        start_date = norm(row.get("運用開始日")) or "-"
        detail = norm(row.get("詳細")) or "（詳細なし）"
        reference_url = norm(row.get("参照URL")) or fallback_url

        out.append(
            InvestigateItem(
                category=category,
                title=title,
                start_date=start_date,
                detail=detail,
                reference_url=reference_url,
            )
        )
    return out


def is_non_change_item(item: InvestigateItem) -> bool:
    text = f"{item.title} {item.detail}"
    keywords = ["変更なし", "更新なし", "該当なし", "有意な更新なし", "不変"]
    return any(k in text for k in keywords)


def render_items(items: List[InvestigateItem]) -> List[str]:
    lines: List[str] = []
    for i, item in enumerate(items, start=1):
        lines.append(f"### {i}")
        lines.append(f"- 分野: {item.category}")
        lines.append(f"- タイトル: {item.title}")
        lines.append(f"- 運用開始日: {item.start_date}")
        lines.append(f"- 詳細: {item.detail}")
        lines.append(f"- 参照URL: {item.reference_url}")
        lines.append("")
    if lines and not lines[-1]:
        lines.pop()
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(
        description="gemini_summary.md の変更対象を再調査し、指定項目で出力する"
    )
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary_targets = parse_gemini_summary(SUMMARY_MD)
    changed_targets = [t for t in summary_targets if t.changed]
    clean_changes = parse_clean_diff(CLEAN_LOG)

    output_lines = [
        "# 変更詳細レポート",
        f"- 生成日時: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 元ファイル: {SUMMARY_MD.name}",
        "",
    ]

    if not changed_targets:
        output_lines.append("変更ありの対象はありませんでした。")
        OUTPUT_MD.write_text("\n".join(output_lines), encoding="utf-8")
        print(f"詳細レポートを出力しました: {OUTPUT_MD}")
        return

    api_key = load_api_key()

    section_count = 0

    for target in changed_targets:
        section_lines: List[str] = []
        section_lines.append(f"## {target.description}")
        section_lines.append(f"- 参照元URL: {target.url or '(不明)'}")

        if not target.url:
            fallback = [
                InvestigateItem(
                    category="その他",
                    title="URL不明のため調査不可",
                    start_date="-",
                    detail="gemini_summary.md にURLが無く再調査できませんでした。",
                    reference_url="-",
                )
            ]
            section_lines.extend(render_items(fallback))
            output_lines.extend(section_lines + [""])
            section_count += 1
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

        try:
            html_content = fetch_html(target.url, timeout=args.timeout)
            page_text = html_to_text(html_content)
            adds, removes = split_diff_lines(change.diff_lines)
            change_points = build_change_points(adds, removes, max_items=40)
            related = collect_related_material(
                base_url=target.url,
                html_content=html_content,
                adds=adds,
                timeout=args.timeout,
            )
            prompt = build_prompt(target, change, page_text, change_points, related)
            raw = call_gemini(api_key, prompt, args.model, timeout=args.timeout)
            items = parse_items(raw, fallback_url=target.url)
            items = [x for x in items if not is_non_change_item(x)]
            if not items:
                continue
        except Exception as exc:
            items = [
                InvestigateItem(
                    category="その他",
                    title="調査処理でエラー",
                    start_date="-",
                    detail=f"再取得または要約に失敗しました: {str(exc).strip()}",
                    reference_url=target.url,
                )
            ]

        section_lines.extend(render_items(items))
        output_lines.extend(section_lines)
        output_lines.append("")
        section_count += 1

    if section_count == 0:
        output_lines.append("新規追加・変更として抽出できる項目はありませんでした。")

    OUTPUT_MD.write_text("\n".join(output_lines).rstrip() + "\n", encoding="utf-8")
    print(f"詳細レポートを出力しました: {OUTPUT_MD}")


if __name__ == "__main__":
    main()

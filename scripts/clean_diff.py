from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_FILE = PROJECT_DIR / "data" / "urls.csv"
OUTPUT_DIR = PROJECT_DIR / "output"
INPUT_LOG = OUTPUT_DIR / "diff.log"
OUTPUT_LOG = OUTPUT_DIR / "clean_diff.log"

TS_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[[^\]]+\] (?P<msg>.*)$"
)
DIFF_FOR_RE = re.compile(r"^Diff for\s+([^:]+):\s*$")


@dataclass
class ChangeEvent:
    target: str
    timestamp_raw: str
    timestamp: dt.datetime
    previous_check_raw: str = ""
    filename: str = ""
    diff_lines: List[str] = field(default_factory=list)


@dataclass
class TargetMeta:
    description: str
    url: str


def normalize_text(text: str) -> str:
    t = unicodedata.normalize("NFKC", text)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def load_targets(csv_path: Path) -> List[TargetMeta]:
    targets: List[TargetMeta] = []
    if not csv_path.exists():
        return targets

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = (row.get("url") or "").strip()
            description = (row.get("description") or "").strip()
            if not url:
                continue
            label = description or url
            targets.append(TargetMeta(description=label, url=url))
    return targets


def parse_diff_log(
    log_path: Path,
) -> Tuple[DefaultDict[str, List[ChangeEvent]], Dict[str, Tuple[str, str]]]:
    history: DefaultDict[str, List[ChangeEvent]] = defaultdict(list)
    last_check: Dict[str, str] = {}
    latest_window: Dict[str, Tuple[str, str]] = {}
    current: ChangeEvent | None = None
    collecting_diff = False

    def finalize_current() -> None:
        nonlocal current, collecting_diff
        if current is not None:
            history[current.target].append(current)
        current = None
        collecting_diff = False

    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    for line in lines:
        m = TS_RE.match(line)
        if m:
            msg = m.group("msg").strip()
            msg_lower = msg.lower()
            ts_raw = m.group("ts")
            ts = dt.datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S,%f")

            if current is not None:
                m_diff = DIFF_FOR_RE.match(msg)
                if m_diff:
                    current.filename = m_diff.group(1).strip()
                    collecting_diff = True
                    continue

            if "content changed; writing diff below" in msg_lower:
                finalize_current()
                marker = ": content changed"
                idx = msg_lower.find(marker)
                target = msg[:idx].strip() if idx >= 0 else msg.split(":", 1)[0].strip()
                prev = last_check.get(target, "")
                current = ChangeEvent(
                    target=target,
                    timestamp_raw=ts_raw,
                    timestamp=ts,
                    previous_check_raw=prev,
                )
                latest_window[target] = (prev, ts_raw)
                last_check[target] = ts_raw
                continue

            if "no text change" in msg_lower:
                finalize_current()
                marker = ": no text change"
                idx = msg_lower.find(marker)
                target = msg[:idx].strip() if idx >= 0 else msg.split(":", 1)[0].strip()
                prev = last_check.get(target, "")
                latest_window[target] = (prev, ts_raw)
                last_check[target] = ts_raw
                continue

            if "new text snapshot saved as" in msg_lower:
                finalize_current()
                marker = ": new text snapshot saved as"
                idx = msg_lower.find(marker)
                target = msg[:idx].strip() if idx >= 0 else msg.split(":", 1)[0].strip()
                prev = last_check.get(target, "")
                latest_window[target] = (prev, ts_raw)
                last_check[target] = ts_raw
                continue

            finalize_current()
            continue

        if current is not None and collecting_diff:
            current.diff_lines.append(line)

    finalize_current()
    return history, latest_window


def split_delta_lines(diff_lines: Iterable[str]) -> List[Tuple[str, str]]:
    deltas: List[Tuple[str, str]] = []
    for raw in diff_lines:
        if raw.startswith(("---", "+++", "@@")):
            continue
        if raw.startswith("+") and not raw.startswith("+++"):
            deltas.append(("+", raw[1:].strip()))
        elif raw.startswith("-") and not raw.startswith("---"):
            deltas.append(("-", raw[1:].strip()))
    return deltas


def cancel_exact_pairs(deltas: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    adds = Counter(normalize_text(t) for s, t in deltas if s == "+")
    removes = Counter(normalize_text(t) for s, t in deltas if s == "-")
    cancellable = {k: min(adds[k], removes[k]) for k in adds}

    used_add = Counter()
    used_remove = Counter()
    kept: List[Tuple[str, str]] = []
    for sign, text in deltas:
        key = normalize_text(text)
        if sign == "+" and cancellable.get(key, 0) > used_add[key]:
            used_add[key] += 1
            continue
        if sign == "-" and cancellable.get(key, 0) > used_remove[key]:
            used_remove[key] += 1
            continue
        kept.append((sign, text))
    return kept


def clean_delta_lines(diff_lines: Iterable[str]) -> List[Tuple[str, str]]:
    # 並べ替えによる + / - 相殺のみを行う
    deltas = split_delta_lines(diff_lines)
    return cancel_exact_pairs(deltas)


def now_ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def build_clean_log(
    targets: List[TargetMeta],
    history: Dict[str, List[ChangeEvent]],
    latest_window: Dict[str, Tuple[str, str]],
) -> str:
    lines: List[str] = []

    known_targets = {t.description for t in targets}
    extras = sorted(set(history.keys()) - known_targets)
    ordered: List[TargetMeta] = targets + [TargetMeta(description=e, url="") for e in extras]

    for target_meta in ordered:
        desc = target_meta.description
        url = target_meta.url
        events = history.get(desc, [])
        win_prev, win_curr = latest_window.get(desc, ("", ""))
        latest_check_window_text = f"{win_prev or '-'} -> {win_curr or '-'}"

        if not events:
            ts = win_curr or now_ts()
            lines.append(f"{ts} [INFO] {desc}: no significant content changed")
            if url:
                lines.append(f"{ts} [INFO] URL: {url}")
            if win_curr:
                lines.append(f"{ts} [INFO] Change Window: {latest_check_window_text}")
            continue

        latest = events[-1]
        cleaned = clean_delta_lines(latest.diff_lines)
        changed_event_window_text = f"{latest.previous_check_raw or '-'} -> {latest.timestamp_raw}"

        if not cleaned:
            ts = win_curr or latest.timestamp_raw
            lines.append(f"{ts} [INFO] {desc}: no significant content changed")
            if url:
                lines.append(f"{ts} [INFO] URL: {url}")
            lines.append(f"{ts} [INFO] Change Window: {latest_check_window_text}")
            continue

        lines.append(f"{latest.timestamp_raw} [INFO] {desc}: content changed (cleaned)")
        if url:
            lines.append(f"{latest.timestamp_raw} [INFO] URL: {url}")
        lines.append(f"{latest.timestamp_raw} [INFO] Change Window: {changed_event_window_text}")
        lines.append(f"{latest.timestamp_raw} [INFO] Diff for {latest.filename or f'{desc}.txt'}:")
        lines.append("--- previous")
        lines.append("+++ current")
        lines.append("@@ cleaned @@")
        for sign, text in cleaned:
            lines.append(f"{sign}{text}")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "diff.log から description ごとの直近 content changed を抽出し、"
            "並べ替えノイズを除外して clean_diff.log を生成する"
        )
    )
    parser.add_argument("--input-log", type=Path, default=INPUT_LOG)
    parser.add_argument("--output-log", type=Path, default=OUTPUT_LOG)
    args = parser.parse_args()

    if not args.input_log.exists():
        raise FileNotFoundError(f"入力ログが見つかりません: {args.input_log}")

    args.output_log.parent.mkdir(parents=True, exist_ok=True)

    targets = load_targets(DATA_FILE)
    history, latest_window = parse_diff_log(args.input_log)
    content = build_clean_log(targets=targets, history=history, latest_window=latest_window)
    args.output_log.write_text(content, encoding="utf-8")
    print(f"clean diff を出力しました: {args.output_log}")


if __name__ == "__main__":
    main()

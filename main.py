from __future__ import annotations

import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def run_step(script_name: str) -> None:
    result = subprocess.run([sys.executable, script_name], cwd=BASE_DIR)
    if result.returncode != 0:
        print(f"{script_name} でエラーが発生しました")
        sys.exit(1)


def main() -> None:
    run_step("scripts/detect_diff.py")
    run_step("scripts/clean_diff.py")
    run_step("scripts/analyse_diff.py")
    print("すべての処理が完了しました")


if __name__ == "__main__":
    main()

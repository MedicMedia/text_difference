# text_difference

## 使い方
1. `data/urls.csv` に監視したい URL を `url,description` 形式で記入します。
2. `main.py` を実行すると、`scripts/detect_diff.py` → `scripts/clean_diff.py` → `scripts/analyse_diff.py` の順で動きます。
3. `scripts/detect_diff.py` は `description` をキーにテキストスナップショットを保存し、差分を `output/diff.log` に追記します。
4. `scripts/clean_diff.py` は `output/diff.log` から URL（description）ごとの直近 `content changed` を抽出し、並べ替え差分を除外して `output/clean_diff.log` を生成します。
5. `scripts/analyse_diff.py` は `output/clean_diff.log` を Gemini で要約し、`output/gemini_summary.md` を生成します。

## 補足
1. `urls.csv` の行順が変わっても、`description` で紐づけるため動作します。
2. `scripts/analyse_diff.py` 実行時は `GEMINI_API_KEY`（環境変数または `.env.local`）が必要です。

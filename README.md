# es-diff

🔍 A command-line tool to compare two Elasticsearch indices and output differences to a CSV file.

## 🚀 Features

- Scroll through two indices and deeply compare their documents.
- Exclude specific fields from comparison (e.g., timestamps, IDs).

## ⬇️ Installation via Homebrew

```bash
brew tap huseyingokdag/es-diff
brew install es-diff
```

## 🧰 Usage

```bash
es-diff \
  --host http://localhost:9200 \
  --index-a YOUR_INDEX_A \
  --index-b YOUR_INDEX_B \
  [--doc-type DOC_TYPE] \
  [--output-csv OUTPUT_FILENAME] \
  [--scroll-size N] \
  [--scroll-time 2m] \
  [--exclude-path "root['someField']"] \
  [--exclude-path "root['nested']['someTimestamp']"]
```

### Example

```bash
es-diff \
  --host http://localhost:9200 \
  --index-a users_20250708 \
  --index-b users_20250709 \
  --doc-type education \
  --exclude-path "root['someDocId']" \
  --exclude-path "root['someTimestamp']"
```

By default, the CSV file will be named:

YYYY-MM-DD_HH-MM-SS-<index_a>-by-<index_b>.csv

## ✅ Requirements

- Python 3.7+
- Access to an Elasticsearch cluster (Tested on Elasticsearch v5.3.2 and v7.10.1)

- Dependencies:
    - elasticsearch
    - deepdiff
    - tqdm

Install via pip:

```bash
pip install -r requirements.txt
```

## 📄 License

This project is licensed under the MIT License.

## 📦 Third‑Party Dependencies

This tool uses the following Python packages:
- elasticsearch (v5.5.6) — Apache‑2.0
- deepdiff — MIT (v8.5.0)
- tqdm — MIT + MPL‑2.0 (v4.67.1)

See [THIRD_PARTY_LICENSES.md](./THIRD_PARTY_LICENSES.md) for full license texts.

## 🛠️ Development

Clone the repo and install dependencies:

```bash
git clone https://github.com/huseyingokdag/es-diff.git
cd es-diff/es_diff
pip install -r requirements.txt
python cli.py --help
```

## 🔧 Contributing

Contributions are welcome! Please fork the repo, implement your enhancements, and open a pull request.

# es-diff

🔍 A command-line tool to compare two Elasticsearch indices and output differences to a CSV file.


## 🚀 Features

- Scroll through two indices and deeply compare their documents.
- Exclude specific fields from comparison (e.g., timestamps, IDs).
- Automatic timestamped output fixtures.
- Robust argument validation and clear error messages.

---

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
  [--exclude-path "root['fieldA']"] \
  [--exclude-path "root['nested']['ts']"]
```

### Example

```bash
es-diff \
  --host http://localhost:9200 \
  --index-a routing_20250708 \
  --index-b routing_20250709 \
  --doc-type attribute \
  --exclude-path "root['indexId']" \
  --exclude-path "root['ts']"
```

By default, the CSV file will be named:

es-diff-out-<index_a>+by+<index_b>+YYYY-MM-DD_HH-MM-SS.csv

## ✅ Requirements

- Python 3.7+

- Access to an Elasticsearch cluster

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

## 📦 Third-Party Dependencies

This tool uses the following Python packages:

elasticsearch – Apache License 2.0

deepdiff – MIT License

tqdm – Mozilla Public License 2.0

See THIRD_PARTY_LICENSES.md for complete license texts.

## 🛠️ Development

Clone the repo and install dependencies:

```bash
git clone https://github.com/huseyingokdag/es-diff.git
cd es-diff
pip install -r requirements.txt
python es_compare.py --help
```

## 🔧 Contributing

Contributions are welcome! Please fork the repo, implement your enhancements, and open a pull request.
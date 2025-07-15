import time
import tracemalloc
import json
import csv
import argparse
import sys
from elasticsearch5 import Elasticsearch, exceptions as es_exceptions
from deepdiff import DeepDiff
from tqdm import tqdm
from datetime import datetime
import re
from dataclasses import dataclass
from es_diff.version import __version__

@dataclass
class Config:
    host: str
    index_a: str
    index_b: str
    doc_type: str
    output_csv: str
    scroll_size: int
    scroll_time: str
    exclude_paths: set[str] | None

def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare two Elasticsearch indices and output differences to a CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--version', action='version', version=f'es-diff {__version__}')

    # Group required arguments
    required = parser.add_argument_group("🔒 Required arguments")
    required.add_argument("--host", required=True, help="Elasticsearch host (e.g., http://localhost:9200)")
    required.add_argument("--index-a", required=True, help="First index to compare")
    required.add_argument("--index-b", required=True, help="Second index to compare")
    
    # Group optional arguments
    optional = parser.add_argument_group("⚙️ Optional arguments")
    optional.add_argument("--doc-type", default="_doc", help="Document type used in Elasticsearch")
    optional.add_argument("--output-csv", help="Output CSV file name. If not provided, will be generated as '<timestamp>-<index_a>-by-<index_b>.csv'")
    optional.add_argument("--scroll-size", type=int, default=1000, help="Number of docs per scroll batch")
    optional.add_argument("--scroll-time", default="2m", help="Scroll context lifetime (e.g., 2m, 30s, 1h)")
    optional.add_argument(
        "--exclude-path",
        action="append",
        help="Exclude path for DeepDiff (e.g., root['timestamp']). Can be used multiple times."
    )    
    
    args = parser.parse_args()

    # Validations
    if args.index_a == args.index_b:
        print("Error: Index A and Index B must be different.")
        sys.exit(1)

    if not args.host.startswith(("http://", "https://")):
        print("Error: --host must start with http:// or https://")
        sys.exit(1)

    if args.scroll_size <= 0:
        print("Error: --scroll-size must be a positive integer.")
        sys.exit(1)

    if not re.match(r"^\d+[smh]$", args.scroll_time):
        print("Error: --scroll-time must be in format like '2m', '30s', or '1h'.")
        sys.exit(1)

    # Set timestamped CSV file name if not provided
    if not args.output_csv:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        safe_index_a = re.sub(r'\W+', '_', args.index_a)
        safe_index_b = re.sub(r'\W+', '_', args.index_b)
        args.output_csv = f"{timestamp}-{safe_index_a}-by-{safe_index_b}.csv"

    return args

def convert_types_to_strings(obj):
    if isinstance(obj, dict):
        return {k: convert_types_to_strings(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_types_to_strings(v) for v in obj]
    elif isinstance(obj, set):
        return list(obj)  # convert sets to lists
    elif isinstance(obj, tuple):
        return tuple(convert_types_to_strings(v) for v in obj)
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)  # fallback for unknown types like SetOrdered

def get_total_docs(es, index, cfg):
    resp = es.count(index=index, doc_type=cfg.doc_type)
    return resp['count']

def compare_indices(es, cfg):
    with open(cfg.output_csv, mode="w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["doc_id", "difference_type", "diff_details"])
        writer.writeheader()

        tracemalloc.start()

        total_docs_a = get_total_docs(es, cfg.index_a, cfg)
        total_docs_b = get_total_docs(es, cfg.index_b, cfg)

        processed_ids = set()
        total_docs_processed = 0

        resp = es.search(index=cfg.index_a, doc_type=cfg.doc_type, size=cfg.scroll_size, scroll=cfg.scroll_time, body={"query": {"match_all": {}}})
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

        pbar_a = tqdm(total=total_docs_a, desc=f"Scanning {cfg.index_a}")

        while hits:
            batch_start_time = time.time()
            ids = [hit['_id'] for hit in hits]

            docs_b = es.mget(body={"ids": ids}, index=cfg.index_b, doc_type=cfg.doc_type)['docs']
            b_docs_by_id = {doc['_id']: doc for doc in docs_b if doc['found']}

            for doc_a in hits:
                doc_id = doc_a['_id']
                source_a = doc_a['_source']
                processed_ids.add(doc_id)

                doc_b = b_docs_by_id.get(doc_id)
                if doc_b:
                    source_b = doc_b['_source']
                    diff = DeepDiff(source_a, source_b, ignore_order=True, exclude_paths=cfg.exclude_paths)
                    if diff:
                        safe_diff = convert_types_to_strings(diff)
                        writer.writerow({
                            "doc_id": doc_id,
                            "difference_type": "field_difference",
                            "diff_details": json.dumps(safe_diff, ensure_ascii=False)
                        })
                else:
                    writer.writerow({
                        "doc_id": doc_id,
                        "difference_type": "missing_in_one_index",
                        "diff_details": f"Present in: {cfg.index_a}"
                    })

            batch_end_time = time.time()
            total_docs_processed += len(hits)
            current, peak = tracemalloc.get_traced_memory()

            pbar_a.update(len(hits))
            pbar_a.set_postfix({
                "Batch time (s)": f"{batch_end_time - batch_start_time:.2f}",
                "Mem curr (MB)": f"{current / 1024 / 1024:.2f}",
                "Mem peak (MB)": f"{peak / 1024 / 1024:.2f}"
            })

            resp = es.scroll(scroll_id=scroll_id, scroll=cfg.scroll_time)
            scroll_id = resp['_scroll_id']
            hits = resp['hits']['hits']

        pbar_a.close()

        resp = es.search(index=cfg.index_b, doc_type=cfg.doc_type, size=cfg.scroll_size, scroll=cfg.scroll_time, body={"query": {"match_all": {}}})
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

        pbar_b = tqdm(total=total_docs_b, desc=f"Scanning {cfg.index_b}")

        while hits:
            batch_start_time = time.time()
            for doc_b in hits:
                doc_id = doc_b['_id']
                if doc_id not in processed_ids:
                    writer.writerow({
                        "doc_id": doc_id,
                        "difference_type": "missing_in_one_index",
                        "diff_details": f"Present in: {cfg.index_b}"
                    })

            batch_end_time = time.time()
            current, peak = tracemalloc.get_traced_memory()

            pbar_b.update(len(hits))
            pbar_b.set_postfix({
                "Batch time (s)": f"{batch_end_time - batch_start_time:.2f}",
                "Mem curr (MB)": f"{current / 1024 / 1024:.2f}",
                "Mem peak (MB)": f"{peak / 1024 / 1024:.2f}"
            })

            resp = es.scroll(scroll_id=scroll_id, scroll=cfg.scroll_time)
            scroll_id = resp['_scroll_id']
            hits = resp['hits']['hits']

        pbar_b.close()
        tracemalloc.stop()

    print(f"Comparison complete. Results saved in {cfg.output_csv}")

def main():
    args = parse_args()

    cfg = Config(
        host=args.host,
        index_a=args.index_a,
        index_b=args.index_b,
        doc_type=args.doc_type,
        output_csv=args.output_csv,
        scroll_size=args.scroll_size,
        scroll_time=args.scroll_time,
        exclude_paths=set(args.exclude_path) if args.exclude_path else None
    )

    # Try to connect to Elasticsearch
    try:
        es = Elasticsearch(cfg.host)
        if not es.ping():
            print(f"❌ Error: Could not connect to Elasticsearch at {cfg.host}")
            sys.exit(1)
    except es_exceptions.ElasticsearchException as e:
        print(f"❌ Connection error: {e}")
        sys.exit(1)

    # Check that indices exist
    for index in [cfg.index_a, cfg.index_b]:
        if not es.indices.exists(index=index):
            print(f"❌ Error: Index '{index}' does not exist.")
            sys.exit(1)

    print(f"🔍 Comparing '{cfg.index_a}' with '{cfg.index_b}' on {cfg.host}")
    print(f"📁 Output will be saved to: {cfg.output_csv}")

    es_client = Elasticsearch([cfg.host])
    start_time = time.time()
    compare_indices(es_client, cfg)
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
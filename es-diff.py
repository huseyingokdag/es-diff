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

def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare two Elasticsearch indices and output differences to a CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

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

def get_total_docs(es, index):
    resp = es.count(index=index, doc_type=DOC_TYPE)
    return resp['count']

def compare_indices(es, index_a, index_b, output_csv):
    with open(output_csv, mode="w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["doc_id", "difference_type", "diff_details"])
        writer.writeheader()

        tracemalloc.start()

        total_docs_a = get_total_docs(es, index_a)
        total_docs_b = get_total_docs(es, index_b)

        processed_ids = set()
        total_docs_processed = 0

        resp = es.search(index=index_a, doc_type=DOC_TYPE, size=SCROLL_SIZE, scroll=SCROLL_TIME, body={"query": {"match_all": {}}})
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

        pbar_a = tqdm(total=total_docs_a, desc=f"Scanning {index_a}")

        while hits:
            batch_start_time = time.time()
            ids = [hit['_id'] for hit in hits]

            docs_b = es.mget(body={"ids": ids}, index=index_b, doc_type=DOC_TYPE)['docs']
            b_docs_by_id = {doc['_id']: doc for doc in docs_b if doc['found']}

            for doc_a in hits:
                doc_id = doc_a['_id']
                source_a = doc_a['_source']
                processed_ids.add(doc_id)

                doc_b = b_docs_by_id.get(doc_id)
                if doc_b:
                    source_b = doc_b['_source']
                    diff = DeepDiff(source_a, source_b, ignore_order=True, exclude_paths=EXCLUDE_PATHS)
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
                        "diff_details": f"Present in: {index_a}"
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

            resp = es.scroll(scroll_id=scroll_id, scroll=SCROLL_TIME)
            scroll_id = resp['_scroll_id']
            hits = resp['hits']['hits']

        pbar_a.close()

        resp = es.search(index=index_b, doc_type=DOC_TYPE, size=SCROLL_SIZE, scroll=SCROLL_TIME, body={"query": {"match_all": {}}})
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

        pbar_b = tqdm(total=total_docs_b, desc=f"Scanning {index_b}")

        while hits:
            batch_start_time = time.time()
            for doc_b in hits:
                doc_id = doc_b['_id']
                if doc_id not in processed_ids:
                    writer.writerow({
                        "doc_id": doc_id,
                        "difference_type": "missing_in_one_index",
                        "diff_details": f"Present in: {index_b}"
                    })

            batch_end_time = time.time()
            current, peak = tracemalloc.get_traced_memory()

            pbar_b.update(len(hits))
            pbar_b.set_postfix({
                "Batch time (s)": f"{batch_end_time - batch_start_time:.2f}",
                "Mem curr (MB)": f"{current / 1024 / 1024:.2f}",
                "Mem peak (MB)": f"{peak / 1024 / 1024:.2f}"
            })

            resp = es.scroll(scroll_id=scroll_id, scroll=SCROLL_TIME)
            scroll_id = resp['_scroll_id']
            hits = resp['hits']['hits']

        pbar_b.close()
        tracemalloc.stop()

    print(f"Comparison complete. Results saved in {output_csv}")

if __name__ == "__main__":
    args = parse_args()

    # Try to connect to Elasticsearch
    try:
        es = Elasticsearch(args.host)
        if not es.ping():
            print(f"❌ Error: Could not connect to Elasticsearch at {args.host}")
            sys.exit(1)
    except es_exceptions.ElasticsearchException as e:
        print(f"❌ Connection error: {e}")
        sys.exit(1)

    # Check that indices exist
    for index in [args.index_a, args.index_b]:
        if not es.indices.exists(index=index):
            print(f"❌ Error: Index '{index}' does not exist.")
            sys.exit(1)

    print(f"🔍 Comparing '{args.index_a}' with '{args.index_b}' on {args.host}")
    print(f"📁 Output will be saved to: {args.output_csv}")

    ES_HOST = args.host
    INDEX_A = args.index_a
    INDEX_B = args.index_b
    DOC_TYPE = args.doc_type
    OUTPUT_CSV = args.output_csv
    SCROLL_SIZE = args.scroll_size
    SCROLL_TIME = args.scroll_time
    EXCLUDE_PATHS = set(args.exclude_path) if args.exclude_path else None

    es_client = Elasticsearch([ES_HOST])
    start_time = time.time()
    compare_indices(es_client, INDEX_A, INDEX_B, OUTPUT_CSV)
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
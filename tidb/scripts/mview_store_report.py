#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from mview_report_common import format_store_text, summarize_store


def main():
    parser = argparse.ArgumentParser(description='Summarize a TiDB MV Jepsen store directory')
    parser.add_argument('store_dir', help='Path to the Jepsen store directory')
    parser.add_argument('--json', action='store_true', help='Emit machine-readable JSON summary')
    args = parser.parse_args()

    report = summarize_store(Path(args.store_dir))
    if not report.get('reportable_store'):
        parser.exit(
            1,
            f"{args.store_dir} is not a reportable TiDB MV Jepsen store directory: "
            "missing manifest, runtime logs, and workload summaries\n",
        )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(format_store_text(report))


if __name__ == '__main__':
    main()

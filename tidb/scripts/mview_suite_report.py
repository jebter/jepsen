#!/usr/bin/env python3
import argparse
import json

from mview_report_common import aggregate_suite, format_suite_text


def main():
    parser = argparse.ArgumentParser(description='Aggregate multiple TiDB MV Jepsen store directories into one suite report')
    parser.add_argument('store_dirs', nargs='+', help='Store directories to aggregate')
    parser.add_argument('--suite-name', default='mview-suite', help='Logical suite name to attach to the report')
    parser.add_argument('--json', action='store_true', help='Emit machine-readable JSON summary')
    args = parser.parse_args()

    try:
        summary = aggregate_suite(args.suite_name, args.store_dirs)
    except ValueError as exc:
        parser.exit(1, f"{exc}\n")
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(format_suite_text(summary))


if __name__ == '__main__':
    main()

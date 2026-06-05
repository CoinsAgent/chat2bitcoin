#!/usr/bin/env python3
"""Count addresses in ClickHouse `bitcoin.addresses` table grouped by partition.

Usage examples:
  python3 ClickHouse/count_addresses_by_partition.py --host localhost --port 9000
  python3 ClickHouse/count_addresses_by_partition.py --host ch.example --user readonly --password secret --format csv

Connects using `clickhouse_driver.Client`. If the package is missing, the script prints install instructions.
"""
import argparse
import json
import sys


def parse_args():
    p = argparse.ArgumentParser(description="Count rows and distinct addresses per partition in bitcoin.addresses")
    p.add_argument("--host", default="192.168.2.241", help="ClickHouse host")
    p.add_argument("--port", type=int, default=9000, help="ClickHouse native port")
    p.add_argument("--user", default="default", help="ClickHouse user")
    p.add_argument("--password", default="", help="ClickHouse password")
    p.add_argument("--table", default="bitcoin.addresses", help="Fully-qualified table name (default: bitcoin.addresses)")
    p.add_argument(
        "--partition",
        action="append",
        help=(
            "Partition(s) to filter by (YYYYMM). Can be provided multiple times or as a comma-separated list, "
            "e.g. --partition 202601 --partition 202602 or --partition 202601,202602"
        ),
    )
    p.add_argument("--format", choices=["table", "csv", "json"], default="table", help="Output format")
    p.add_argument("--per-block", action="store_true", help="Group counts per block within each partition (adds block_height and block_hash)")
    p.add_argument("--distinct-only", action="store_true", help="Only print distinct address counts per partition (no row counts)")
    return p.parse_args()


def build_query(table, partitions=None, per_block=False):
    where = ""
    if partitions:
        # partitions should be integers like 202601
        vals = ",".join(str(int(p)) for p in partitions)
        where = f"WHERE address_month IN ({vals})\n"

    if per_block:
        return f"""
SELECT
    address_month AS partition,
    block_height,
    block_hash,
    count() AS rows,
    countDistinct(address) AS unique_addresses
FROM {table}
{where}GROUP BY partition, block_height, block_hash
ORDER BY partition, block_height
"""

    return f"""
SELECT
    address_month AS partition,
    count() AS rows,
    countDistinct(address) AS unique_addresses
FROM {table}
{where}GROUP BY partition
ORDER BY partition
"""


def main():
    args = parse_args()

    try:
        from clickhouse_driver import Client
    except Exception:
        print("Missing dependency: install with: pip install clickhouse-driver", file=sys.stderr)
        sys.exit(2)

    client = Client(host=args.host, port=args.port, user=args.user, password=args.password)

    # normalize partition argument(s)
    partitions = None
    if args.partition:
        parts = []
        for item in args.partition:
            for p in str(item).split(','):
                s = p.strip()
                if s:
                    parts.append(s)
        if parts:
            partitions = parts

    query = build_query(args.table, partitions=partitions, per_block=args.per_block)
    rows = client.execute(query)

    if args.format == "json":
        out = []
        if args.per_block:
            for partition, block_height, block_hash, row_count, unique_count in rows:
                out.append({
                    "partition": partition,
                    "block_height": block_height,
                    "block_hash": block_hash,
                    "rows": row_count,
                    "unique_addresses": unique_count,
                })
        else:
            for partition, row_count, unique_count in rows:
                out.append({"partition": partition, "rows": row_count, "unique_addresses": unique_count})
        print(json.dumps(out, indent=2))
        return

    if args.format == "csv":
        # simple CSV header
        if args.per_block:
            if args.distinct_only:
                print("partition,block_height,block_hash,unique_addresses")
                for partition, block_height, block_hash, row_count, unique_count in rows:
                    print(f"{partition},{block_height},{block_hash},{unique_count}")
            else:
                print("partition,block_height,block_hash,rows,unique_addresses")
                for partition, block_height, block_hash, row_count, unique_count in rows:
                    print(f"{partition},{block_height},{block_hash},{row_count},{unique_count}")
        else:
            if args.distinct_only:
                print("partition,unique_addresses")
                for partition, row_count, unique_count in rows:
                    print(f"{partition},{unique_count}")
            else:
                print("partition,rows,unique_addresses")
                for partition, row_count, unique_count in rows:
                    print(f"{partition},{row_count},{unique_count}")
        return

    # default table format
    # print a simple aligned table
    if args.per_block:
        if args.distinct_only:
            print(f"{'partition':>10}  {'block_height':>12}  {'block_hash':>66}  {'unique_addresses':>18}")
            print('-' * 112)
            for partition, block_height, block_hash, row_count, unique_count in rows:
                print(f"{partition:>10}  {block_height:12}  {block_hash:66}  {unique_count:18}")
        else:
            print(f"{'partition':>10}  {'block_height':>12}  {'block_hash':>66}  {'rows':>12}  {'unique_addresses':>18}")
            print('-' * 134)
            for partition, block_height, block_hash, row_count, unique_count in rows:
                print(f"{partition:>10}  {block_height:12}  {block_hash:66}  {row_count:12}  {unique_count:18}")
    else:
        if args.distinct_only:
            print(f"{'partition':>10}  {'unique_addresses':>18}")
            print('-' * 32)
            for partition, row_count, unique_count in rows:
                print(f"{partition:>10}  {unique_count:18}")
        else:
            print(f"{'partition':>10}  {'rows':>12}  {'unique_addresses':>18}")
            print('-' * 46)
            for partition, row_count, unique_count in rows:
                print(f"{partition:>10}  {row_count:12}  {unique_count:18}")


if __name__ == '__main__':
    main()

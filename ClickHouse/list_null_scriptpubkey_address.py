#!/usr/bin/env python3
"""
List rows where scriptPubKey address is NULL/empty in ClickHouse.

By default, checks both:
- bitcoin.outputs.scriptPubKey_address
- bitcoin.inputs.prevout_scriptPubKey_address

Usage:
  python3 list_null_scriptpubkey_address.py
  python3 list_null_scriptpubkey_address.py --block-number 909090
  python3 list_null_scriptpubkey_address.py --table outputs --limit 50
"""

import argparse
import json
import requests as req


CLICKHOUSE_HTTP_URL = "http://192.168.2.241:8123"
CLICKHOUSE_DATABASE = "bitcoin"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""
REQUEST_TIMEOUT = 60


class ClickHouse:
    def __init__(self, url, database, user, password):
        self.url = url
        self.database = database
        self.user = user
        self.has_password = bool(password)
        self.session = req.Session()
        if self.has_password:
            self.session.auth = (user, password)

    def _params(self):
        params = {"database": self.database, "default_format": "JSONEachRow"}
        if not self.has_password:
            params["user"] = self.user
        return params

    def query_rows(self, sql):
        resp = self.session.post(
            self.url,
            data=sql.encode(),
            params=self._params(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        text = resp.text.strip()
        if not text:
            return []
        return [json.loads(line) for line in text.splitlines()]


def main():
    parser = argparse.ArgumentParser(description="List NULL/empty scriptPubKey addresses.")
    parser.add_argument("--table", choices=["outputs", "inputs", "both"], default="both")
    parser.add_argument("--block-number", type=int, default=None)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    ch = ClickHouse(
        CLICKHOUSE_HTTP_URL,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_USER,
        CLICKHOUSE_PASSWORD,
    )

    block_filter = ""
    if args.block_number is not None:
        block_filter = f" AND block_height = {args.block_number}"

    if args.table in ("outputs", "both"):
        sql = f"""
        SELECT
            block_height, block_hash, txid, vout_index, n,
            scriptPubKey_type, scriptPubKey_address
        FROM bitcoin.outputs
        WHERE (isNull(scriptPubKey_address) OR scriptPubKey_address = '')
          {block_filter}
        ORDER BY block_height, txid, vout_index
        LIMIT {args.limit}
        """
        rows = ch.query_rows(sql)
        print(f"outputs NULL/empty scriptPubKey_address rows: {len(rows)}")
        for r in rows:
            print(json.dumps(r, ensure_ascii=False))

    if args.table in ("inputs", "both"):
        sql = f"""
        SELECT
            block_height, block_hash, txid, vin_index,
            prevout_scriptPubKey_type, prevout_scriptPubKey_address
        FROM bitcoin.inputs
        WHERE (isNull(prevout_scriptPubKey_address) OR prevout_scriptPubKey_address = '')
          {block_filter}
        ORDER BY block_height, txid, vin_index
        LIMIT {args.limit}
        """
        rows = ch.query_rows(sql)
        print(f"inputs NULL/empty prevout_scriptPubKey_address rows: {len(rows)}")
        for r in rows:
            print(json.dumps(r, ensure_ascii=False))


if __name__ == "__main__":
    main()

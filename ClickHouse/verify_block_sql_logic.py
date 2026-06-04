#!/usr/bin/env python3
"""
Verify SQL unfolding logic for Bitcoin blocks in ClickHouse.

Checks:
1) blocks.nTx == number of rows in transactions for the block height
2) sum(length(transactions.vin)) == number of rows in inputs for the block height
3) sum(length(transactions.vout)) == number of rows in outputs for the block height
4) count(addresses) == non-empty input addresses + non-empty output addresses
5) count(addresses) == count(inputs) + count(outputs) - null/empty input addresses - null/empty output addresses

Usage:
  python3 verify_block_sql_logic.py --block-number 909090
  python3 verify_block_sql_logic.py --start 909090 --stop 909100
"""

import argparse
import json
import sys
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
        params = {"database": self.database}
        if not self.has_password:
            params["user"] = self.user
        return params

    def query_row(self, sql):
        params = self._params()
        params["default_format"] = "JSONEachRow"
        resp = self.session.post(
            self.url,
            data=sql.encode(),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        text = resp.text.strip()
        if not text:
            return None
        return json.loads(text.splitlines()[0])


def as_int(value):
    return int(value or 0)


def verify_one_block(ch, block_number):
    row = ch.query_row(
        f"""
        SELECT
            b.height AS block_number,
            b.nTx AS block_nTx,
            (SELECT count() FROM bitcoin.transactions WHERE block_height = b.height) AS tx_count,
            (SELECT sum(length(vin)) FROM bitcoin.transactions WHERE block_height = b.height) AS tx_vin_sum,
            (SELECT count() FROM bitcoin.inputs WHERE block_height = b.height) AS inputs_count,
            (SELECT sum(length(vout)) FROM bitcoin.transactions WHERE block_height = b.height) AS tx_vout_sum,
            (SELECT count() FROM bitcoin.outputs WHERE block_height = b.height) AS outputs_count,
            (
                SELECT count()
                FROM bitcoin.inputs
                WHERE block_height = b.height
                  AND isNotNull(prevout_scriptPubKey_address)
                  AND prevout_scriptPubKey_address != ''
            ) AS input_address_count,
            (
                SELECT count()
                FROM bitcoin.inputs
                WHERE block_height = b.height
                  AND (isNull(prevout_scriptPubKey_address) OR prevout_scriptPubKey_address = '')
            ) AS input_null_address_count,
            (
                SELECT count()
                FROM bitcoin.outputs
                WHERE block_height = b.height
                  AND isNotNull(scriptPubKey_address)
                  AND scriptPubKey_address != ''
            ) AS output_address_count,
            (
                SELECT count()
                FROM bitcoin.outputs
                WHERE block_height = b.height
                  AND (isNull(scriptPubKey_address) OR scriptPubKey_address = '')
            ) AS output_null_address_count,
            (SELECT count() FROM bitcoin.addresses WHERE block_height = b.height) AS addresses_count
        FROM bitcoin.blocks AS b
        WHERE b.height = {block_number}
        LIMIT 1
        """
    )

    if row is None:
        print(f"Block {block_number} not found in bitcoin.blocks")
        return False

    block_nTx = as_int(row["block_nTx"])
    tx_count = as_int(row["tx_count"])
    tx_vin_sum = as_int(row["tx_vin_sum"])
    inputs_count = as_int(row["inputs_count"])
    tx_vout_sum = as_int(row["tx_vout_sum"])
    outputs_count = as_int(row["outputs_count"])
    input_address_count = as_int(row["input_address_count"])
    input_null_address_count = as_int(row["input_null_address_count"])
    output_address_count = as_int(row["output_address_count"])
    output_null_address_count = as_int(row["output_null_address_count"])
    addresses_count = as_int(row["addresses_count"])
    expected_addresses_count = input_address_count + output_address_count
    expected_addresses_count_by_total_minus_null = (
        inputs_count + outputs_count - input_null_address_count - output_null_address_count
    )

    checks = [
        ("block.nTx == transactions count", block_nTx, tx_count),
        ("sum(transactions.vin) == inputs count", tx_vin_sum, inputs_count),
        ("sum(transactions.vout) == outputs count", tx_vout_sum, outputs_count),
        ("addresses count == input prevout addresses + output addresses", addresses_count, expected_addresses_count),
        (
            "addresses count == inputs + outputs - null/empty input/output addresses",
            addresses_count,
            expected_addresses_count_by_total_minus_null,
        ),
    ]

    print(f"Block {block_number} verification")
    for name, left, right in checks:
        ok = left == right
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name}: {left} vs {right}")
        if not ok:
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Verify SQL logic for one block or a block range.")
    parser.add_argument("--block-number", type=int, default=None, help="Single Bitcoin block height")
    parser.add_argument("--start", type=int, default=None, help="Start block height (inclusive)")
    parser.add_argument("--stop", type=int, default=None, help="Stop block height (inclusive)")
    args = parser.parse_args()

    single_mode = args.block_number is not None
    range_mode = args.start is not None or args.stop is not None

    if single_mode and range_mode:
        print("Use either --block-number OR --start/--stop, not both.")
        sys.exit(2)

    if single_mode:
        start = args.block_number
        stop = args.block_number
    else:
        if args.start is None or args.stop is None:
            print("Range mode requires both --start and --stop.")
            sys.exit(2)
        start = args.start
        stop = args.stop

    if start > stop:
        print(f"Invalid range: start ({start}) cannot be greater than stop ({stop}).")
        sys.exit(2)

    ch = ClickHouse(
        CLICKHOUSE_HTTP_URL,
        CLICKHOUSE_DATABASE,
        CLICKHOUSE_USER,
        CLICKHOUSE_PASSWORD,
    )

    for block_number in range(start, stop + 1):
        ok = verify_one_block(ch, block_number)
        if not ok:
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()

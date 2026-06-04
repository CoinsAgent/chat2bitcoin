#!/usr/bin/env python3
"""
Bitcoin Full Node ↔ ClickHouse Sync Consistency Verifier
=========================================================
Compares Bitcoin Core RPC `getblock` (verbosity=3) data with ClickHouse
(`blocks`, `transactions`, `inputs`, `outputs`) to verify sync correctness.

Usage examples:
  python3 verify_sync_consistency.py
  python3 verify_sync_consistency.py --start 100000 --stop 100100
  python3 verify_sync_consistency.py --sample 20
  python3 verify_sync_consistency.py --fail-fast --strict
  python3 verify_sync_consistency.py --json-report report.json
"""

import argparse
import json
import logging
import random
import sys
from decimal import Decimal

import requests as req


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================
BITCOIN_RPC_URL = "http://192.168.2.241:8332"
BITCOIN_RPC_USER = "bitcoin"
BITCOIN_RPC_PASSWORD = "passw0rd"

CLICKHOUSE_HTTP_URL = "http://192.168.2.241:8123"
CLICKHOUSE_DATABASE = "bitcoin"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""

REQUEST_TIMEOUT = 60


# ============================================================
# Helpers
# ============================================================
def btc_amount_to_dec(value):
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal("0.00000001"))


def row_to_map(rows, key):
    out = {}
    for r in rows:
        out[r[key]] = r
    return out


# ============================================================
# Bitcoin RPC Client
# ============================================================
class BitcoinRPC:
    def __init__(self, url, user, password):
        self.url = url
        self.session = req.Session()
        self.session.auth = (user, password)
        self.session.headers.update({"Content-Type": "application/json"})

    def call(self, method, params=None):
        payload = {
            "jsonrpc": "1.0",
            "id": "consistency-verifier",
            "method": method,
            "params": params or [],
        }
        resp = self.session.post(self.url, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        if body.get("error"):
            raise RuntimeError(f"RPC error {method}: {body['error']}")
        return body["result"]

    def getblockcount(self):
        return self.call("getblockcount")

    def getblockhash(self, height):
        return self.call("getblockhash", [height])

    def getblock(self, blockhash, verbosity=3):
        return self.call("getblock", [blockhash, verbosity])


# ============================================================
# ClickHouse HTTP Client
# ============================================================
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

    def query_rows(self, sql):
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
            return []
        return [json.loads(line) for line in text.split("\n")]

    def query_scalar(self, sql):
        resp = self.session.post(
            self.url,
            data=sql.encode(),
            params=self._params(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text.strip()


# ============================================================
# Verifier
# ============================================================
class Verifier:
    def __init__(self, btc, ch, strict=False, fail_fast=False):
        self.btc = btc
        self.ch = ch
        self.strict = strict
        self.fail_fast = fail_fast

        self.passed = 0
        self.warnings = 0
        self.failed = 0
        self.results = []

    def check(self, ok, code, message, context=None, level="FAIL"):
        record = {
            "ok": bool(ok),
            "code": code,
            "message": message,
            "context": context or {},
            "level": level if not ok else "PASS",
        }
        self.results.append(record)

        if ok:
            self.passed += 1
            return

        if level == "WARN":
            self.warnings += 1
        else:
            self.failed += 1

        if self.fail_fast and level != "WARN":
            raise RuntimeError(f"Fail-fast: {code} - {message}")

    def _eq(self, left, right, code, message, context=None, warn=False):
        ok = (left == right)
        level = "WARN" if warn else "FAIL"
        self.check(ok, code, message if ok else f"{message}: {left} != {right}", context, level=level)

    def verify_block(self, height):
        blockhash = self.btc.getblockhash(height)
        b = self.btc.getblock(blockhash, verbosity=3)

        ch_block_rows = self.ch.query_rows(
            f"""
            SELECT hash, confirmations, height, version, versionHex, merkleroot,
                   time, mediantime, nonce, bits, difficulty, chainwork, nTx,
                   previousblockhash, nextblockhash, strippedsize, size, weight
            FROM blocks FINAL
            WHERE height = {height}
            """
        )

        self.check(len(ch_block_rows) == 1,
                   "BLOCK_ROW_EXISTS",
                   "Exactly one block row exists in ClickHouse",
                   {"height": height, "rows": len(ch_block_rows)})

        if len(ch_block_rows) != 1:
            return

        cb = ch_block_rows[0]

        # Core block fields
        self._eq(cb["hash"], b["hash"], "BLOCK_HASH", "Block hash matches", {"height": height})
        self._eq(int(cb["height"]), int(b["height"]), "BLOCK_HEIGHT", "Block height matches", {"height": height})
        self._eq(int(cb["time"]), int(b["time"]), "BLOCK_TIME", "Block time matches", {"height": height})
        self._eq(int(cb["mediantime"]), int(b["mediantime"]), "BLOCK_MEDIANTIME", "Block mediantime matches", {"height": height})
        self._eq(int(cb["nTx"]), int(b["nTx"]), "BLOCK_NTX", "Block nTx matches", {"height": height})
        self._eq(int(cb["size"]), int(b["size"]), "BLOCK_SIZE", "Block size matches", {"height": height})
        self._eq(int(cb["weight"]), int(b["weight"]), "BLOCK_WEIGHT", "Block weight matches", {"height": height})
        self._eq(int(cb["strippedsize"]), int(b["strippedsize"]), "BLOCK_STRIPPEDSIZE", "Block strippedsize matches", {"height": height})
        self._eq(cb["versionHex"], b["versionHex"], "BLOCK_VERSIONHEX", "Block versionHex matches", {"height": height})
        self._eq(cb["merkleroot"], b["merkleroot"], "BLOCK_MERKLEROOT", "Block merkleroot matches", {"height": height})
        self._eq(cb["bits"], b["bits"], "BLOCK_BITS", "Block bits matches", {"height": height})
        self._eq(cb["chainwork"], b["chainwork"], "BLOCK_CHAINWORK", "Block chainwork matches", {"height": height})

        # Floating compare for difficulty
        rpc_diff = float(b["difficulty"])
        ch_diff = float(cb["difficulty"])
        self.check(abs(rpc_diff - ch_diff) < 1e-12,
                   "BLOCK_DIFFICULTY",
                   "Block difficulty matches",
                   {"height": height, "rpc": rpc_diff, "ch": ch_diff})

        # Optional links
        self._eq(cb.get("previousblockhash", "") or "", b.get("previousblockhash", "") or "",
                 "BLOCK_PREVHASH", "Previous block hash matches", {"height": height})
        self._eq(cb.get("nextblockhash", "") or "", b.get("nextblockhash", "") or "",
                 "BLOCK_NEXTHASH", "Next block hash matches", {"height": height})

        # Transactions in CH
        ch_txs = self.ch.query_rows(
            f"""
            SELECT txid, hash, version, size, vsize, weight, locktime, fee
            FROM transactions FINAL
            WHERE block_height = {height}
            """
        )

        self._eq(len(ch_txs), len(b["tx"]), "TX_COUNT", "Transaction count matches", {"height": height})
        if len(ch_txs) != len(b["tx"]):
            return

        tx_rpc_map = row_to_map(b["tx"], "txid")
        tx_ch_map = row_to_map(ch_txs, "txid")

        # txid set check
        self._eq(set(tx_ch_map.keys()), set(tx_rpc_map.keys()), "TXID_SET", "Transaction txid set matches", {"height": height})

        # Per tx checks + aggregate sums
        rpc_out_sum = Decimal("0")
        rpc_prevout_sum = Decimal("0")
        ch_out_sum = Decimal("0")
        ch_prevout_sum = Decimal("0")

        for txid, tx in tx_rpc_map.items():
            ch_tx = tx_ch_map.get(txid)
            if ch_tx is None:
                self.check(False, "TX_MISSING", "Transaction missing in ClickHouse", {"height": height, "txid": txid})
                continue

            self._eq(ch_tx["hash"], tx["hash"], "TX_HASH", "Transaction hash matches", {"height": height, "txid": txid})
            self._eq(int(ch_tx["version"]), int(tx["version"]), "TX_VERSION", "Transaction version matches", {"height": height, "txid": txid})
            self._eq(int(ch_tx["size"]), int(tx["size"]), "TX_SIZE", "Transaction size matches", {"height": height, "txid": txid})
            self._eq(int(ch_tx["vsize"]), int(tx["vsize"]), "TX_VSIZE", "Transaction vsize matches", {"height": height, "txid": txid})
            self._eq(int(ch_tx["weight"]), int(tx["weight"]), "TX_WEIGHT", "Transaction weight matches", {"height": height, "txid": txid})
            self._eq(int(ch_tx["locktime"]), int(tx["locktime"]), "TX_LOCKTIME", "Transaction locktime matches", {"height": height, "txid": txid})

            rpc_fee = btc_amount_to_dec(tx.get("fee"))
            ch_fee = btc_amount_to_dec(ch_tx.get("fee"))
            self._eq(ch_fee, rpc_fee, "TX_FEE", "Transaction fee matches", {"height": height, "txid": txid}, warn=not self.strict)

            # Inputs count check
            ch_in_count = int(self.ch.query_scalar(
                f"SELECT count() FROM inputs FINAL WHERE block_height = {height} AND txid = '{txid}'"
            ))
            self._eq(ch_in_count, len(tx["vin"]), "VIN_COUNT", "Transaction vin count matches", {"height": height, "txid": txid})

            # Outputs count/value checks
            ch_out_rows = self.ch.query_rows(
                f"""
                SELECT vout_index, n, value, scriptPubKey_address, scriptPubKey_type
                FROM outputs FINAL
                WHERE block_height = {height} AND txid = '{txid}'
                ORDER BY vout_index
                """
            )
            self._eq(len(ch_out_rows), len(tx["vout"]), "VOUT_COUNT", "Transaction vout count matches", {"height": height, "txid": txid})

            # Compare by n
            ch_vout_map = {int(r["n"]): r for r in ch_out_rows}
            rpc_vout_map = {int(v["n"]): v for v in tx["vout"]}
            self._eq(set(ch_vout_map.keys()), set(rpc_vout_map.keys()), "VOUT_N_SET", "Transaction vout n set matches", {"height": height, "txid": txid})

            for n, rv in rpc_vout_map.items():
                cv = ch_vout_map.get(n)
                if cv is None:
                    self.check(False, "VOUT_MISSING", "Output missing in ClickHouse", {"height": height, "txid": txid, "n": n})
                    continue

                rpc_val = btc_amount_to_dec(rv["value"])
                ch_val = btc_amount_to_dec(cv["value"])
                self._eq(ch_val, rpc_val, "VOUT_VALUE", "Output value matches", {"height": height, "txid": txid, "n": n})

                rpc_spk = rv.get("scriptPubKey", {})
                rpc_addr = rpc_spk.get("address")
                rpc_type = rpc_spk.get("type")
                ch_addr = cv.get("scriptPubKey_address")
                ch_type = cv.get("scriptPubKey_type")

                self._eq(ch_addr, rpc_addr, "VOUT_ADDRESS", "Output address matches", {"height": height, "txid": txid, "n": n}, warn=not self.strict)
                self._eq(ch_type, rpc_type, "VOUT_TYPE", "Output script type matches", {"height": height, "txid": txid, "n": n}, warn=False)

                rpc_out_sum += rpc_val
                ch_out_sum += ch_val

            # Sum prevout values from RPC vin where present
            for vin in tx["vin"]:
                prevout = vin.get("prevout")
                if prevout and prevout.get("value") is not None:
                    rpc_prevout_sum += btc_amount_to_dec(prevout.get("value"))

            ch_prev = self.ch.query_scalar(
                f"""
                SELECT ifNull(sum(prevout_value), toDecimal64(0, 8))
                FROM inputs FINAL
                WHERE block_height = {height} AND txid = '{txid}' AND prevout_value IS NOT NULL
                """
            )
            ch_prevout_sum += btc_amount_to_dec(ch_prev)

        # Block-level aggregate checks
        self._eq(ch_out_sum, rpc_out_sum, "BLOCK_SUM_OUTPUTS", "Block total outputs sum matches", {"height": height})
        self._eq(ch_prevout_sum, rpc_prevout_sum, "BLOCK_SUM_PREVOUTS", "Block total prevout inputs sum matches", {"height": height}, warn=not self.strict)

    def summary(self):
        return {
            "passed": self.passed,
            "warnings": self.warnings,
            "failed": self.failed,
            "total": self.passed + self.warnings + self.failed,
            "results": self.results,
        }


# ============================================================
# Main
# ============================================================
def pick_heights(start, stop, sample):
    heights = list(range(start, stop + 1))
    if sample is None or sample >= len(heights):
        return heights
    return sorted(random.sample(heights, sample))


def main():
    parser = argparse.ArgumentParser(description="Verify full-node to ClickHouse sync consistency")
    parser.add_argument("--start", type=int, default=None, help="Start height (default: max(0, tip-200))")
    parser.add_argument("--stop", type=int, default=None, help="Stop height (default: tip)")
    parser.add_argument("--sample", type=int, default=None, help="Randomly sample N blocks in range")
    parser.add_argument("--strict", action="store_true", help="Treat soft mismatches (e.g. optional address/fee) as FAIL")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first FAIL")
    parser.add_argument("--json-report", type=str, default=None, help="Write JSON report to file")
    args = parser.parse_args()

    btc = BitcoinRPC(BITCOIN_RPC_URL, BITCOIN_RPC_USER, BITCOIN_RPC_PASSWORD)
    ch = ClickHouse(CLICKHOUSE_HTTP_URL, CLICKHOUSE_DATABASE, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    tip = btc.getblockcount()
    start = args.start if args.start is not None else max(0, tip - 200)
    stop = args.stop if args.stop is not None else tip

    if start < 0 or stop < 0 or start > stop:
        logger.error(f"Invalid range: start={start}, stop={stop}")
        return 2

    heights = pick_heights(start, stop, args.sample)
    logger.info(f"Tip={tip}. Verifying {len(heights)} block(s) in [{start}, {stop}]")

    v = Verifier(btc, ch, strict=args.strict, fail_fast=args.fail_fast)

    try:
        for i, h in enumerate(heights, 1):
            logger.info(f"[{i}/{len(heights)}] Verify block {h}")
            v.verify_block(h)
    except Exception as e:
        logger.error(f"Verifier stopped: {e}")

    summary = v.summary()
    logger.info(
        f"Summary: PASS={summary['passed']} WARN={summary['warnings']} FAIL={summary['failed']} TOTAL={summary['total']}"
    )

    if args.json_report:
        with open(args.json_report, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"JSON report written: {args.json_report}")

    # Print concise fail/warn details for terminal readability
    for r in summary["results"]:
        if not r["ok"]:
            logger.error(f"{r['level']} {r['code']}: {r['message']} | context={r['context']}")

    if summary["failed"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

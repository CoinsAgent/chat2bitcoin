#!/usr/bin/env python3
"""
Bitcoin Block Syncer: Full Node → ClickHouse
=============================================
Fetches blocks from Bitcoin Core RPC (verbosity=3) and inserts into ClickHouse.
The ClickHouse schema uses Materialized Views to auto-unfold:
  blocks → transactions → inputs → outputs → addresses

Dependencies: requests (only — no clickhouse-driver needed)

Usage:
    python3 sync_blocks.py [--start HEIGHT] [--stop HEIGHT] [--batch-size N]

Examples:
    python3 sync_blocks.py                          # Resume from last synced
    python3 sync_blocks.py --start 0 --stop 100     # Blocks 0-100
    python3 sync_blocks.py --start 199999           # From 199999 to tip
    python3 sync_blocks.py --batch-size 50          # Larger batches
    python3 sync_blocks.py --dry-run                # Fetch & count only
"""

import json
import time
import argparse
import logging
import requests as req
from decimal import Decimal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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

BATCH_SIZE = 10  # blocks per INSERT batch
SLEEP_ON_ERROR = 1
REQUEST_TIMEOUT = 60


def btc_amount_to_str(value):
    """Normalize BTC amount to fixed 8-decimal string for ClickHouse Decimal(20,8)."""
    if value is None:
        return None
    return format(Decimal(str(value)).quantize(Decimal("0.00000001")), "f")

# ============================================================
# Bitcoin RPC Client
# ============================================================
class BitcoinRPC:
    def __init__(self, url, user, password):
        self.url = url
        self.auth = (user, password)
        self.session = req.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    def call(self, method, params=None):
        payload = {
            "jsonrpc": "1.0",
            "id": "syncer",
            "method": method,
            "params": params or []
        }
        resp = self.session.post(self.url, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        result = resp.json()
        if result.get("error"):
            raise Exception(f"RPC error {method}: {result['error']}")
        return result["result"]

    def getblockcount(self):
        return self.call("getblockcount")

    def getblockhash(self, height):
        return self.call("getblockhash", [height])

    def getblock(self, blockhash, verbosity=3):
        return self.call("getblock", [blockhash, verbosity])

# ============================================================
# ClickHouse Client (HTTP interface — no extra deps)
# ============================================================
class ClickHouseSync:
    def __init__(self, url, database, user, password):
        self.url = url
        self.database = database
        self.user = user
        self.has_password = bool(password)
        self.session = req.Session()
        if self.has_password:
            self.session.auth = (user, password)

    def query(self, sql):
        """Execute a query and return the raw text response."""
        params = {"database": self.database}
        if not self.has_password:
            params["user"] = self.user
        resp = self.session.post(
            self.url,
            data=sql.encode(),
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        return resp.text.strip()

    def get_last_synced_height(self) -> int:
        """Get the highest block height already synced."""
        try:
            result = self.query("SELECT MAX(height) FROM blocks")
            if result and result != "":
                return int(result)
            return 0
        except Exception as e:
            logger.warning(f"Could not query last synced height: {e}")
            return 0

    def insert_blocks_json(self, blocks_json: list[dict]):
        """Insert blocks using JSONEachRow format."""
        if not blocks_json:
            return
        # Each element is a dict matching the column names of bitcoin.blocks
        body = "\n".join(json.dumps(b) for b in blocks_json)
        params = {
            "database": self.database,
            "query": "INSERT INTO blocks FORMAT JSONEachRow",
        }
        if not self.has_password:
            params["user"] = self.user
        resp = self.session.post(
            self.url,
            data=body.encode(),
            params=params,
            timeout=300,
        )
        resp.raise_for_status()

    def close(self):
        pass  # HTTP session doesn't need cleanup

# ============================================================
# Data Transformation: JSON → ClickHouse-compatible dicts
# ============================================================

def transform_vin(vin_item: dict) -> dict:
    """Transform a single vin entry."""
    coinbase = vin_item.get("coinbase")
    script_sig = vin_item.get("scriptSig", {})
    prevout = vin_item.get("prevout", {})
    prevout_script = prevout.get("scriptPubKey", {})

    return {
        "coinbase": coinbase,
        "txid": vin_item.get("txid"),
        "vout": vin_item.get("vout"),
        "scriptSig": {
            "asm": script_sig.get("asm"),
            "hex": script_sig.get("hex"),
        },
        "txinwitness": vin_item.get("txinwitness", []),
        "prevout": {
            "generated": prevout.get("generated"),
            "height": prevout.get("height"),
            "value": btc_amount_to_str(prevout.get("value")),
            "scriptPubKey": {
                "asm": prevout_script.get("asm"),
                "desc": prevout_script.get("desc"),
                "hex": prevout_script.get("hex"),
                "address": prevout_script.get("address"),
                "type": prevout_script.get("type"),
            },
        },
        "sequence": vin_item.get("sequence", 0),
    }

def transform_vout(vout_item: dict) -> dict:
    """Transform a single vout entry."""
    script = vout_item.get("scriptPubKey", {})
    return {
        "value": btc_amount_to_str(vout_item["value"]),
        "n": vout_item["n"],
        "scriptPubKey": {
            "asm": script.get("asm"),
            "desc": script.get("desc"),
            "hex": script.get("hex"),
            "address": script.get("address"),
            "type": script.get("type"),
        },
    }

def transform_tx(tx: dict) -> dict:
    """Transform a verbosity=3 transaction."""
    return {
        "txid": tx["txid"],
        "hash": tx["hash"],
        "version": tx["version"],
        "size": tx["size"],
        "vsize": tx["vsize"],
        "weight": tx["weight"],
        "locktime": tx["locktime"],
        "vin": [transform_vin(v) for v in tx["vin"]],
        "vout": [transform_vout(v) for v in tx["vout"]],
        "fee": btc_amount_to_str(tx.get("fee")),
        "hex": tx["hex"],
    }

def transform_block(block: dict) -> dict:
    """Transform a verbosity=3 block into a dict for JSONEachRow insert."""
    return {
        "hash": block["hash"],
        "confirmations": block["confirmations"],
        "height": block["height"],
        "version": block["version"],
        "versionHex": block["versionHex"],
        "merkleroot": block["merkleroot"],
        "time": block["time"],
        "mediantime": block["mediantime"],
        "nonce": block["nonce"],
        "bits": block["bits"],
        "target": block.get("target", ""),
        "difficulty": block["difficulty"],
        "chainwork": block["chainwork"],
        "nTx": block["nTx"],
        "previousblockhash": block.get("previousblockhash", ""),
        "nextblockhash": block.get("nextblockhash", ""),
        "strippedsize": block["strippedsize"],
        "size": block["size"],
        "weight": block["weight"],
        "tx": [transform_tx(tx) for tx in block["tx"]],
        "revision": 0,
    }

# ============================================================
# Main Sync Loop
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Sync Bitcoin blocks from full node to ClickHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 sync_blocks.py                          # Resume from last synced
  python3 sync_blocks.py --start 0 --stop 100     # Blocks 0-100
  python3 sync_blocks.py --start 199999           # From 199999 to tip
  python3 sync_blocks.py --batch-size 50          # Larger batches
  python3 sync_blocks.py --dry-run                # Fetch & count only
        """
    )
    parser.add_argument("--start", type=int, default=None,
                        help="Start height (default: last synced + 1)")
    parser.add_argument("--stop", type=int, default=None,
                        help="Stop height (default: node tip)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help="Blocks per INSERT batch (default: 10)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and transform but do not insert")
    args = parser.parse_args()

    btc = BitcoinRPC(BITCOIN_RPC_URL, BITCOIN_RPC_USER, BITCOIN_RPC_PASSWORD)
    ch = ClickHouseSync(CLICKHOUSE_HTTP_URL, CLICKHOUSE_DATABASE,
                        CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    # Determine range
    tip = btc.getblockcount()
    logger.info(f"Node tip: {tip}")

    last_synced = ch.get_last_synced_height()
    logger.info(f"Last synced height in ClickHouse: {last_synced}")

    start = args.start if args.start is not None else last_synced + 1
    stop = args.stop if args.stop is not None else tip

    if start > stop:
        logger.info(f"Nothing to sync. Start={start}, Stop={stop}, Tip={tip}")
        ch.close()
        return

    total_to_sync = stop - start + 1
    logger.info(f"Syncing blocks {start} → {stop} ({total_to_sync} blocks)")

    batch = []
    t0 = time.time()
    synced_count = 0
    error_count = 0

    for height in range(start, stop + 1):
        try:
            blockhash = btc.getblockhash(height)
            block = btc.getblock(blockhash, verbosity=3)
            batch.append(transform_block(block))
            synced_count += 1

        except Exception as e:
            error_count += 1
            logger.error(f"Failed block {height}: {e}")
            time.sleep(SLEEP_ON_ERROR)
            continue

        # Insert batch
        if len(batch) >= args.batch_size:
            if not args.dry_run:
                ch.insert_blocks_json(batch)
            logger.info(f"  Inserted batch ending at height {height}")
            batch = []

        # Progress report
        if synced_count % 100 == 0:
            elapsed = time.time() - t0
            rate = synced_count / elapsed
            logger.info(f"  Progress: {synced_count}/{total_to_sync} blocks "
                        f"in {elapsed:.1f}s ({rate:.1f} blocks/s)")

    # Flush remaining
    if batch:
        if not args.dry_run:
            ch.insert_blocks_json(batch)
        logger.info(f"Flushed remaining {len(batch)} blocks")

    ch.close()
    total_time = time.time() - t0
    logger.info(f"Done. Synced {synced_count} blocks ({error_count} errors) "
                f"in {total_time:.1f}s")

if __name__ == "__main__":
    main()

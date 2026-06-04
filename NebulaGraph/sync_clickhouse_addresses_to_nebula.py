#!/usr/bin/env python3
"""
Sync ClickHouse bitcoin.addresses into NebulaGraph.

Graph model:
    Address --input_to_tx--> Tx --tx_to_output--> Address

VID convention:
    address vertex VID: addr:<bitcoin_address>
    tx vertex VID:      tx:<txid>

ClickHouse source table:
    bitcoin.addresses

Expected columns:
    address, direction, txid, hash, block_hash, block_height, block_time,
    utxo_txid, utxo_vout, source_index, value, value_delta, revision

Recommended run pattern:
    Run partition by partition using --address-month, for example 202506.

Dependencies:
    pip install clickhouse-connect nebula3-python
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import clickhouse_connect
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


LOGGER = logging.getLogger("sync_clickhouse_addresses_to_nebula")


# -----------------------------
# Config
# -----------------------------

@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: bool = False


@dataclass(frozen=True)
class NebulaConfig:
    hosts: List[Tuple[str, int]]
    username: str
    password: str
    space: str
    timeout_ms: int = 60000
    max_pool_size: int = 10


@dataclass(frozen=True)
class SyncConfig:
    address_month: Optional[int]
    block_height_start: Optional[int]
    block_height_end: Optional[int]
    fetch_limit: int
    insert_batch_size: int
    use_final: bool
    offset_start: int
    max_rows: Optional[int]
    sleep_seconds: float
    dry_run: bool


# -----------------------------
# nGQL literal helpers
# -----------------------------

def ngql_string(value: Any) -> str:
    """Convert a Python value into a safe nGQL string literal."""
    if value is None:
        return '""'
    s = str(value)
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def ngql_int(value: Any) -> str:
    if value is None or value == "":
        return "0"
    return str(int(value))


def ngql_float(value: Any) -> str:
    if value is None or value == "":
        return "0.0"
    if isinstance(value, Decimal):
        return str(float(value))
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "0.0"
    return str(float(value))


def addr_vid(address: str) -> str:
    return f"addr:{address}"


def tx_vid(txid: str) -> str:
    return f"tx:{txid}"


def chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


# -----------------------------
# Direction mapping
# -----------------------------

def normalize_direction(direction: str) -> str:
    """
    Normalize ClickHouse address direction into one of:
      - input
      - output

    Common conventions supported:
      input side:  input, inputs, spend, spent, debit, out
      output side: output, outputs, receive, received, credit, in

    If your table uses another convention, add it here.
    """
    if direction is None:
        raise ValueError("direction is NULL")

    d = str(direction).strip().lower()

    input_aliases = {
        "input",
        "inputs",
        "vin",
        "spend",
        "spent",
        "spent_in",
        "in_spend",
        "debit",
        "out",
    }

    output_aliases = {
        "output",
        "outputs",
        "vout",
        "receive",
        "received",
        "paid_to",
        "credit",
        "in",
    }

    if d in input_aliases:
        return "input"
    if d in output_aliases:
        return "output"

    raise ValueError(
        f"Unknown direction={direction!r}. Update normalize_direction() to match your table convention."
    )


# -----------------------------
# Nebula helpers
# -----------------------------

def open_nebula_session(cfg: NebulaConfig):
    nebula_cfg = Config()
    nebula_cfg.max_connection_pool_size = cfg.max_pool_size
    nebula_cfg.timeout = cfg.timeout_ms

    pool = ConnectionPool()
    if not pool.init(cfg.hosts, nebula_cfg):
        raise RuntimeError(f"Failed to initialize NebulaGraph connection pool: {cfg.hosts}")

    session = pool.get_session(cfg.username, cfg.password)
    result = session.execute(f"USE {cfg.space};")
    if not result.is_succeeded():
        session.release()
        pool.close()
        raise RuntimeError(f"Failed to use Nebula space {cfg.space}: {result.error_msg()}")

    return pool, session


def execute_ngql(session, stmt: str, dry_run: bool = False) -> None:
    stmt = stmt.strip()
    if not stmt:
        return

    if dry_run:
        LOGGER.info("DRY RUN nGQL:\n%s", stmt[:2000] + ("..." if len(stmt) > 2000 else ""))
        return

    result = session.execute(stmt)
    if not result.is_succeeded():
        raise RuntimeError(f"nGQL failed: {result.error_msg()}\nStatement:\n{stmt[:4000]}")


# -----------------------------
# nGQL builders
# -----------------------------

def build_address_vertices(rows: List[Dict[str, Any]]) -> str:
    by_vid: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        address = row.get("address")
        if not address:
            continue
        vid = addr_vid(str(address))
        by_vid[vid] = {"address": str(address)}

    if not by_vid:
        return ""

    values = [
        f'{ngql_string(vid)}:({ngql_string(v["address"])})'
        for vid, v in by_vid.items()
    ]

    return f"""
INSERT VERTEX IF NOT EXISTS address(address)
VALUES {", ".join(values)};
"""


def build_tx_vertices(rows: List[Dict[str, Any]]) -> str:
    by_vid: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        txid = row.get("txid")
        if not txid:
            continue

        vid = tx_vid(str(txid))
        current = {
            "txid": str(txid),
            "hash": row.get("hash") or str(txid),
            "block_hash": row.get("block_hash") or "",
            "block_height": row.get("block_height") or 0,
            "block_time": row.get("block_time") or 0,
        }

        # Keep the first copy. In bitcoin.addresses these fields should be identical per txid.
        by_vid.setdefault(vid, current)

    if not by_vid:
        return ""

    values = []
    for vid, v in by_vid.items():
        values.append(
            f'{ngql_string(vid)}:('
            f'{ngql_string(v["txid"])}, '
            f'{ngql_string(v["hash"])}, '
            f'{ngql_string(v["block_hash"])}, '
            f'{ngql_int(v["block_height"])}, '
            f'{ngql_int(v["block_time"])}'
            f')'
        )

    return f"""
INSERT VERTEX IF NOT EXISTS tx(txid, hash, block_hash, block_height, block_time)
VALUES {", ".join(values)};
"""


def edge_rank(row: Dict[str, Any]) -> int:
    """
    NebulaGraph edge rank distinguishes multiple edges between the same src and dst.
    source_index maps to vin_index for input rows or vout_index for output rows.
    """
    return int(row.get("source_index") or 0)


def edge_values(row: Dict[str, Any]) -> str:
    return (
        f'{ngql_string(row.get("txid") or "")}, '
        f'{ngql_string(row.get("hash") or row.get("txid") or "")}, '
        f'{ngql_string(row.get("block_hash") or "")}, '
        f'{ngql_int(row.get("block_height") or 0)}, '
        f'{ngql_int(row.get("block_time") or 0)}, '
        f'{ngql_string(row.get("utxo_txid") or "")}, '
        f'{ngql_int(row.get("utxo_vout") or 0)}, '
        f'{ngql_int(row.get("source_index") or 0)}, '
        f'{ngql_float(row.get("value") or 0)}, '
        f'{ngql_float(row.get("value_delta") or 0)}, '
        f'{ngql_int(row.get("revision") or 0)}'
    )


def build_input_edges(rows: List[Dict[str, Any]]) -> str:
    values = []

    for row in rows:
        if normalize_direction(row.get("direction")) != "input":
            continue

        src = addr_vid(str(row["address"]))
        dst = tx_vid(str(row["txid"]))
        rank = edge_rank(row)
        values.append(f'{ngql_string(src)} -> {ngql_string(dst)}@{rank}:({edge_values(row)})')

    if not values:
        return ""

    return f"""
INSERT EDGE IF NOT EXISTS input_to_tx(
  txid,
  hash,
  block_hash,
  block_height,
  block_time,
  utxo_txid,
  utxo_vout,
  source_index,
  value,
  value_delta,
  revision
)
VALUES {", ".join(values)};
"""


def build_output_edges(rows: List[Dict[str, Any]]) -> str:
    values = []

    for row in rows:
        if normalize_direction(row.get("direction")) != "output":
            continue

        src = tx_vid(str(row["txid"]))
        dst = addr_vid(str(row["address"]))
        rank = edge_rank(row)
        values.append(f'{ngql_string(src)} -> {ngql_string(dst)}@{rank}:({edge_values(row)})')

    if not values:
        return ""

    return f"""
INSERT EDGE IF NOT EXISTS tx_to_output(
  txid,
  hash,
  block_hash,
  block_height,
  block_time,
  utxo_txid,
  utxo_vout,
  source_index,
  value,
  value_delta,
  revision
)
VALUES {", ".join(values)};
"""


# -----------------------------
# ClickHouse query
# -----------------------------

def open_clickhouse_client(cfg: ClickHouseConfig):
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        secure=cfg.secure,
    )


def build_where_clause(sync: SyncConfig) -> str:
    parts = ["1 = 1"]

    if sync.address_month is not None:
        parts.append(f"address_month = {int(sync.address_month)}")

    if sync.block_height_start is not None:
        parts.append(f"block_height >= {int(sync.block_height_start)}")

    if sync.block_height_end is not None:
        parts.append(f"block_height <= {int(sync.block_height_end)}")

    return " AND ".join(parts)


def fetch_block_heights(ch, sync: SyncConfig) -> List[int]:
    final_suffix = " FINAL" if sync.use_final else ""

    sql = f"""
SELECT DISTINCT block_height
FROM bitcoin.addresses{final_suffix}
WHERE {build_where_clause(sync)}
ORDER BY block_height
"""

    result = ch.query(sql)
    heights = [int(row[0]) for row in result.result_rows]
    if sync.offset_start > 0:
        return heights[sync.offset_start :]
    return heights


def fetch_address_rows_for_block(
    ch,
    sync: SyncConfig,
    block_height: int,
    offset: int,
    remaining_rows: Optional[int],
) -> List[Dict[str, Any]]:
    final_suffix = " FINAL" if sync.use_final else ""
    limit = sync.fetch_limit

    if remaining_rows is not None:
        if remaining_rows <= 0:
            return []
        limit = min(limit, remaining_rows)

    sql = f"""
SELECT
    address,
    direction,
    txid,
    hash,
    block_hash,
    block_height,
    block_time,
    utxo_txid,
    utxo_vout,
    source_index,
    value,
    value_delta,
    revision
FROM bitcoin.addresses{final_suffix}
WHERE {build_where_clause(sync)}
  AND block_height = {int(block_height)}
ORDER BY txid, direction, source_index, address
LIMIT {limit}
OFFSET {int(offset)}
"""

    result = ch.query(sql)
    columns = result.column_names
    return [dict(zip(columns, row)) for row in result.result_rows]


def write_rows_to_nebula(session, rows: List[Dict[str, Any]], sync_cfg: SyncConfig) -> int:
    batches = 0

    for batch in chunked(rows, sync_cfg.insert_batch_size):
        batch_rows = list(batch)

        statements = [
            build_address_vertices(batch_rows),
            build_tx_vertices(batch_rows),
            build_input_edges(batch_rows),
            build_output_edges(batch_rows),
        ]

        for stmt in statements:
            execute_ngql(session, stmt, dry_run=sync_cfg.dry_run)

        batches += 1

    return batches


# -----------------------------
# Main sync
# -----------------------------

def sync_clickhouse_addresses_to_nebula(
    ch_cfg: ClickHouseConfig,
    nebula_cfg: NebulaConfig,
    sync_cfg: SyncConfig,
) -> None:
    ch = open_clickhouse_client(ch_cfg)
    pool = None
    session = None

    try:
        pool, session = open_nebula_session(nebula_cfg)

        block_heights = fetch_block_heights(ch, sync_cfg)
        total_rows = 0
        total_batches = 0
        started = time.time()

        LOGGER.info("Found %s block heights to sync", len(block_heights))

        stop_requested = False

        for block_height in block_heights:
            block_offset = 0
            block_rows = 0

            while True:
                remaining_rows = None
                if sync_cfg.max_rows is not None:
                    remaining_rows = sync_cfg.max_rows - total_rows
                    if remaining_rows <= 0:
                        LOGGER.info("Reached max_rows=%s", sync_cfg.max_rows)
                        stop_requested = True
                        break

                rows = fetch_address_rows_for_block(
                    ch,
                    sync_cfg,
                    block_height,
                    block_offset,
                    remaining_rows,
                )
                if not rows:
                    break

                LOGGER.info(
                    "Fetched %s rows from ClickHouse for block_height=%s offset=%s",
                    len(rows),
                    block_height,
                    block_offset,
                )

                total_batches += write_rows_to_nebula(session, rows, sync_cfg)
                total_rows += len(rows)
                block_rows += len(rows)
                block_offset += len(rows)

            elapsed = time.time() - started
            speed = total_rows / elapsed if elapsed > 0 else 0.0
            LOGGER.info(
                "Block synced: block_height=%s block_rows=%s total_rows=%s total_batches=%s speed=%.2f rows/sec",
                block_height,
                block_rows,
                total_rows,
                total_batches,
                speed,
            )

            if sync_cfg.sleep_seconds > 0:
                time.sleep(sync_cfg.sleep_seconds)

            if stop_requested:
                break

        LOGGER.info("Sync finished. total_rows=%s total_batches=%s", total_rows, total_batches)

    finally:
        if session is not None:
            session.release()
        if pool is not None:
            pool.close()
        try:
            ch.close()
        except Exception:
            pass


# -----------------------------
# CLI
# -----------------------------

def parse_nebula_hosts(raw: str) -> List[Tuple[str, int]]:
    hosts = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"Invalid Nebula host format: {item}. Expected host:port")
        host, port = item.rsplit(":", 1)
        hosts.append((host, int(port)))
    if not hosts:
        raise ValueError("At least one Nebula host is required")
    return hosts


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync ClickHouse bitcoin.addresses to NebulaGraph Address -> Tx -> Address graph."
    )

    parser.add_argument("--ch-host", default="192.168.2.241")
    parser.add_argument("--ch-port", type=int, default=8123)
    parser.add_argument("--ch-user", default="default")
    parser.add_argument("--ch-password", default="")
    parser.add_argument("--ch-database", default="bitcoin")
    parser.add_argument("--ch-secure", action="store_true")

    parser.add_argument("--nebula-hosts", default="192.168.2.65:9669", help="Comma-separated host:port list")
    parser.add_argument("--nebula-user", default="root")
    parser.add_argument("--nebula-password", default="nebula")
    parser.add_argument("--nebula-space", default="bitcoin")
    parser.add_argument("--nebula-timeout-ms", type=int, default=60000)

    parser.add_argument("--address-month", type=int, default=None, help="Optional partition filter, e.g. 202506")
    parser.add_argument("--block-height-start", type=int, default=None)
    parser.add_argument("--block-height-end", type=int, default=None)

    parser.add_argument("--fetch-limit", type=int, default=10000)
    parser.add_argument("--insert-batch-size", type=int, default=500)
    parser.add_argument("--offset-start", type=int, default=0, help="Skip this many block heights before syncing")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)

    parser.add_argument("--no-final", action="store_true", help="Do not use FINAL in ClickHouse SELECT")
    parser.add_argument("--dry-run", action="store_true", help="Print generated nGQL instead of executing")
    parser.add_argument("--log-level", default="INFO")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    ch_cfg = ClickHouseConfig(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
        secure=args.ch_secure,
    )

    nebula_cfg = NebulaConfig(
        hosts=parse_nebula_hosts(args.nebula_hosts),
        username=args.nebula_user,
        password=args.nebula_password,
        space=args.nebula_space,
        timeout_ms=args.nebula_timeout_ms,
    )

    sync_cfg = SyncConfig(
        address_month=args.address_month,
        block_height_start=args.block_height_start,
        block_height_end=args.block_height_end,
        fetch_limit=args.fetch_limit,
        insert_batch_size=args.insert_batch_size,
        use_final=not args.no_final,
        offset_start=args.offset_start,
        max_rows=args.max_rows,
        sleep_seconds=args.sleep_seconds,
        dry_run=args.dry_run,
    )

    LOGGER.info("Starting sync with config: %s %s %s", ch_cfg, nebula_cfg, sync_cfg)
    sync_clickhouse_addresses_to_nebula(ch_cfg, nebula_cfg, sync_cfg)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted by user")
        raise SystemExit(130)

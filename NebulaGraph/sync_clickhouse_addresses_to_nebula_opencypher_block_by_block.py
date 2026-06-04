#!/usr/bin/env python3
"""
Sync ClickHouse bitcoin.addresses to NebulaGraph with openCypher-style writes.

Graph model:
    (:address)-[:input_to_tx]->(:tx)-[:tx_to_output]->(:address)

This script mirrors sync_clickhouse_addresses_to_nebula.py, but generates
openCypher-style MERGE/MATCH statements instead of native nGQL INSERT statements.

Important:
    NebulaGraph support for openCypher write clauses depends on the server
    version. If MERGE/CREATE writes are rejected, use --dry-run with
    --cypher-output to export the generated Cypher, or use the native nGQL
    sync script instead.

Dependencies:
    pip install clickhouse-connect nebula3-python

Examples:
    python3 NebulaGraph/sync_clickhouse_addresses_to_nebula_opencypher_block_by_block.py \
      --block-height-start 909090 \
      --block-height-end 909090 \
      --dry-run

    python3 NebulaGraph/sync_clickhouse_addresses_to_nebula_opencypher_block_by_block.py \
      --block-height-start 909090 \
      --block-height-end 909100 \
      --cypher-output /tmp/bitcoin_addresses.cypher
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, TextIO, Tuple

import clickhouse_connect
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


LOGGER = logging.getLogger("sync_clickhouse_addresses_to_nebula_opencypher")


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
    cypher_batch_size: int
    use_final: bool
    offset_start: int
    max_rows: Optional[int]
    sleep_seconds: float
    dry_run: bool
    print_cypher: bool
    cypher_output: Optional[str]


def cypher_string(value: Any) -> str:
    if value is None:
        return "null"
    s = str(value)
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def cypher_int(value: Any) -> str:
    if value is None or value == "":
        return "0"
    return str(int(value))


def cypher_float(value: Any) -> str:
    if value is None or value == "":
        return "0.0"
    if isinstance(value, Decimal):
        return str(float(value))
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return "0.0"
    return str(float(value))


def cypher_value(value: Any) -> str:
    if isinstance(value, dict):
        return cypher_map(value)
    if isinstance(value, str) or value is None:
        return cypher_string(value)
    if isinstance(value, int):
        return cypher_int(value)
    if isinstance(value, float) or isinstance(value, Decimal):
        return cypher_float(value)
    return cypher_string(value)


def cypher_map(props: Dict[str, Any]) -> str:
    parts = [f"{key}: {cypher_value(value)}" for key, value in props.items()]
    return "{" + ", ".join(parts) + "}"


def chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def normalize_direction(direction: str) -> str:
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
    raise ValueError(f"Unknown direction={direction!r}")


def open_clickhouse_client(cfg: ClickHouseConfig):
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        secure=cfg.secure,
    )


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


def execute_cypher(session, stmt: str, sync: SyncConfig, output: Optional[TextIO]) -> None:
    stmt = stmt.strip()
    if not stmt:
        return

    if output is not None:
        output.write(stmt)
        output.write("\n\n")
        output.flush()

    if sync.print_cypher:
        print(stmt)
        print()

    if sync.dry_run:
        LOGGER.info("DRY RUN Cypher:\n%s", stmt[:2000] + ("..." if len(stmt) > 2000 else ""))
        return

    result = session.execute(stmt)
    if not result.is_succeeded():
        raise RuntimeError(f"Cypher failed: {result.error_msg()}\nStatement:\n{stmt[:4000]}")


def unique_addresses(rows: List[Dict[str, Any]]) -> List[str]:
    seen = set()
    addresses = []
    for row in rows:
        address = row.get("address")
        if not address or address in seen:
            continue
        seen.add(address)
        addresses.append(str(address))
    return addresses


def unique_transactions(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_txid: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        txid = row.get("txid")
        if not txid:
            continue
        by_txid.setdefault(
            str(txid),
            {
                "txid": str(txid),
                "hash": row.get("hash") or str(txid),
                "block_hash": row.get("block_hash") or "",
                "block_height": int(row.get("block_height") or 0),
                "block_time": int(row.get("block_time") or 0),
            },
        )
    return list(by_txid.values())


def edge_props(row: Dict[str, Any], direction: str) -> Dict[str, Any]:
    return {
        "direction": direction,
        "txid": row.get("txid") or "",
        "hash": row.get("hash") or row.get("txid") or "",
        "block_hash": row.get("block_hash") or "",
        "block_height": int(row.get("block_height") or 0),
        "block_time": int(row.get("block_time") or 0),
        "utxo_txid": row.get("utxo_txid") or "",
        "utxo_vout": int(row.get("utxo_vout") or 0),
        "source_index": int(row.get("source_index") or 0),
        "value": float(row.get("value") or 0),
        "value_delta": float(row.get("value_delta") or 0),
        "revision": int(row.get("revision") or 0),
    }


def build_address_vertex_cypher(rows: List[Dict[str, Any]]) -> str:
    values = [cypher_map({"address": address}) for address in unique_addresses(rows)]
    if not values:
        return ""

    return f"""
UNWIND [{", ".join(values)}] AS row
MERGE (a:address {{address: row.address}});
"""


def build_tx_vertex_cypher(rows: List[Dict[str, Any]]) -> str:
    values = [cypher_map(tx) for tx in unique_transactions(rows)]
    if not values:
        return ""

    return f"""
UNWIND [{", ".join(values)}] AS row
MERGE (t:tx {{txid: row.txid}})
SET t.hash = row.hash,
    t.block_hash = row.block_hash,
    t.block_height = row.block_height,
    t.block_time = row.block_time;
"""


def build_input_edge_cypher(rows: List[Dict[str, Any]]) -> str:
    values = []
    for row in rows:
        if normalize_direction(row.get("direction")) != "input":
            continue
        props = edge_props(row, "input")
        values.append(
            cypher_map(
                {
                    "address": row.get("address") or "",
                    "txid": row.get("txid") or "",
                    "rank": int(row.get("source_index") or 0),
                    "props": props,
                }
            )
        )

    if not values:
        return ""

    return f"""
UNWIND [{", ".join(values)}] AS row
MATCH (a:address {{address: row.address}})
MATCH (t:tx {{txid: row.txid}})
MERGE (a)-[e:input_to_tx {{source_index: row.rank}}]->(t)
SET e += row.props;
"""


def build_output_edge_cypher(rows: List[Dict[str, Any]]) -> str:
    values = []
    for row in rows:
        if normalize_direction(row.get("direction")) != "output":
            continue
        props = edge_props(row, "output")
        values.append(
            cypher_map(
                {
                    "address": row.get("address") or "",
                    "txid": row.get("txid") or "",
                    "rank": int(row.get("source_index") or 0),
                    "props": props,
                }
            )
        )

    if not values:
        return ""

    return f"""
UNWIND [{", ".join(values)}] AS row
MATCH (t:tx {{txid: row.txid}})
MATCH (a:address {{address: row.address}})
MERGE (t)-[e:tx_to_output {{source_index: row.rank}}]->(a)
SET e += row.props;
"""


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


def write_rows_as_cypher(
    session,
    rows: List[Dict[str, Any]],
    sync: SyncConfig,
    output: Optional[TextIO],
) -> int:
    batches = 0

    for batch in chunked(rows, sync.cypher_batch_size):
        batch_rows = list(batch)
        statements = [
            build_address_vertex_cypher(batch_rows),
            build_tx_vertex_cypher(batch_rows),
            build_input_edge_cypher(batch_rows),
            build_output_edge_cypher(batch_rows),
        ]

        for stmt in statements:
            execute_cypher(session, stmt, sync, output)

        batches += 1

    return batches


def sync_clickhouse_addresses_to_nebula_opencypher(
    ch_cfg: ClickHouseConfig,
    nebula_cfg: NebulaConfig,
    sync: SyncConfig,
) -> None:
    ch = open_clickhouse_client(ch_cfg)
    pool = None
    session = None
    output = None

    try:
        if not sync.dry_run:
            pool, session = open_nebula_session(nebula_cfg)

        if sync.cypher_output:
            output_path = Path(sync.cypher_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output = output_path.open("w", encoding="utf-8")
            output.write(f"USE {nebula_cfg.space};\n\n")

        block_heights = fetch_block_heights(ch, sync)
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
                if sync.max_rows is not None:
                    remaining_rows = sync.max_rows - total_rows
                    if remaining_rows <= 0:
                        LOGGER.info("Reached max_rows=%s", sync.max_rows)
                        stop_requested = True
                        break

                rows = fetch_address_rows_for_block(
                    ch,
                    sync,
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

                total_batches += write_rows_as_cypher(session, rows, sync, output)
                total_rows += len(rows)
                block_rows += len(rows)
                block_offset += len(rows)

            elapsed = time.time() - started
            speed = total_rows / elapsed if elapsed > 0 else 0.0
            LOGGER.info(
                "Block processed: block_height=%s block_rows=%s total_rows=%s total_batches=%s speed=%.2f rows/sec",
                block_height,
                block_rows,
                total_rows,
                total_batches,
                speed,
            )

            if sync.sleep_seconds > 0:
                time.sleep(sync.sleep_seconds)

            if stop_requested:
                break

        LOGGER.info("Sync finished. total_rows=%s total_batches=%s", total_rows, total_batches)

    finally:
        if output is not None:
            output.close()
        if session is not None:
            session.release()
        if pool is not None:
            pool.close()
        try:
            ch.close()
        except Exception:
            pass


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
        description="Sync ClickHouse bitcoin.addresses to NebulaGraph using openCypher-style writes."
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
    parser.add_argument("--cypher-batch-size", type=int, default=500)
    parser.add_argument("--offset-start", type=int, default=0, help="Skip this many block heights before syncing")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)

    parser.add_argument("--no-final", action="store_true", help="Do not use FINAL in ClickHouse SELECT")
    parser.add_argument("--dry-run", action="store_true", help="Generate Cypher but do not execute it")
    parser.add_argument("--print-cypher", action="store_true", help="Print generated Cypher to stdout")
    parser.add_argument("--cypher-output", default=None, help="Optional path to write generated Cypher statements")
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

    sync = SyncConfig(
        address_month=args.address_month,
        block_height_start=args.block_height_start,
        block_height_end=args.block_height_end,
        fetch_limit=args.fetch_limit,
        cypher_batch_size=args.cypher_batch_size,
        use_final=not args.no_final,
        offset_start=args.offset_start,
        max_rows=args.max_rows,
        sleep_seconds=args.sleep_seconds,
        dry_run=args.dry_run,
        print_cypher=args.print_cypher,
        cypher_output=args.cypher_output,
    )

    LOGGER.info("Starting openCypher sync with config: %s %s %s", ch_cfg, nebula_cfg, sync)
    sync_clickhouse_addresses_to_nebula_opencypher(ch_cfg, nebula_cfg, sync)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted by user")
        raise SystemExit(130)

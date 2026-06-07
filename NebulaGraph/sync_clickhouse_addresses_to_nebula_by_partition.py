#!/usr/bin/env python3
"""Sync ClickHouse bitcoin.addresses to NebulaGraph block by block.

Auto-discovers partitions and blocks from ClickHouse (which syncs from Bitcoin full node every 3 min).
Starts from a given partition and finds all new partitions and blocks added since last run.

Syncs address, tx vertices and edges (input_to_tx, tx_to_output) for each block in each partition.

Usage examples:
  python3 sync_clickhouse_addresses_to_nebula_by_partition.py --start-partition 202001
  python3 sync_clickhouse_addresses_to_nebula_by_partition.py --start-partition 202001 --ch-host 192.168.2.241
"""
import argparse
import sys


def parse_args():
    p = argparse.ArgumentParser(description="Sync ClickHouse bitcoin.addresses to NebulaGraph by partition and block")
    p.add_argument("--ch-host", default="192.168.2.241", help="ClickHouse host")
    p.add_argument("--ch-port", type=int, default=9000, help="ClickHouse native port")
    p.add_argument("--ch-user", default="default", help="ClickHouse user")
    p.add_argument("--ch-password", default="", help="ClickHouse password")
    p.add_argument(
        "--start-partition",
        required=True,
        help="Start partition (YYYYMM). Script auto-discovers all partitions >= this value from ClickHouse",
    )
    p.add_argument("--ng-host", default="192.168.2.65", help="NebulaGraph host")
    p.add_argument("--ng-port", type=int, default=9669, help="NebulaGraph port")
    p.add_argument("--ng-user", default="root", help="NebulaGraph user")
    p.add_argument("--ng-password", default="nebula", help="NebulaGraph password")
    p.add_argument("--ng-space", default="bitcoin", help="NebulaGraph space name")
    p.add_argument("--batch-size", type=int, default=1000, help="Batch insert size")
    return p.parse_args()


def discover_partitions(ch_client, start_partition):
    """Auto-discover all partitions >= start_partition from bitcoin.blocks table.
    Returns sorted list of partition strings (YYYYMM format).
    """
    query = f"""
SELECT DISTINCT block_month
FROM bitcoin.blocks
WHERE block_month >= {start_partition}
ORDER BY block_month
"""
    result = ch_client.execute(query)
    # Extract partition values and convert to strings
    partitions = [str(row[0]) for row in result]
    return partitions


def get_blocks_for_partition(ch_client, partition):
    """Retrieve all blocks in a partition from bitcoin.blocks table.
    Returns list of tuples: (block_month, block_height, block_hash)
    """
    query = f"""
SELECT
    block_month,
    height,
    hash
FROM bitcoin.blocks
WHERE block_month = {partition}
ORDER BY height
"""
    return ch_client.execute(query)


def get_addresses_for_block(ch_client, partition, block_height):
    """Retrieve all addresses for a specific partition and block_height.
    Returns list of tuples: (address, direction, txid, hash, block_hash, block_height, 
                             block_time, utxo_txid, utxo_vout, source_index, value, value_delta, revision)
    """
    query = f"""
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
FROM bitcoin.addresses
WHERE address_month = {partition} AND block_height = {block_height}
ORDER BY address, txid
"""
    return ch_client.execute(query)


def insert_vertices_and_edges(ng_session, addresses_data):
    """Insert address, tx vertices and edges into NebulaGraph.
    
    Args:
        ng_session: NebulaGraph session
        addresses_data: list of address rows from ClickHouse
    """
    if not addresses_data:
        return
    
    # Collect unique addresses and transactions
    addresses = set()
    transactions = {}  # txid -> (hash, block_hash, block_height, block_time)
    edges_input = []  # (address, txid, direction, ...)
    edges_output = []
    
    for row in addresses_data:
        (address, direction, txid, hash_val, block_hash, block_height, block_time,
         utxo_txid, utxo_vout, source_index, value, value_delta, revision) = row
        
        addresses.add(address)
        
        if txid not in transactions:
            transactions[txid] = (hash_val, block_hash, block_height, block_time)
        
        if direction == 'input':
            edges_input.append((address, txid, direction, hash_val, block_hash, block_height, 
                               block_time, utxo_txid, utxo_vout, source_index, value, value_delta, revision))
        else:  # output
            edges_output.append((address, txid, direction, hash_val, block_hash, block_height,
                                block_time, utxo_txid, utxo_vout, source_index, value, value_delta, revision))
    
    # Insert address vertices
    for addr in addresses:
        ngql = f"""INSERT VERTEX address(address) VALUES "addr:{addr}":({{"address": "{addr}"}})"""
        try:
            ng_session.execute(ngql)
        except Exception as e:
            print(f"Warning: Failed to insert address vertex {addr}: {e}")
    
    # Insert transaction vertices
    for txid, (hash_val, block_hash, block_height, block_time) in transactions.items():
        ngql = f"""
INSERT VERTEX tx(txid, hash, block_hash, block_height, block_time) 
VALUES "tx:{txid}":(
    {{
        "txid": "{txid}",
        "hash": "{hash_val}",
        "block_hash": "{block_hash}",
        "block_height": {block_height},
        "block_time": {block_time}
    }}
)
"""
        try:
            ng_session.execute(ngql)
        except Exception as e:
            print(f"Warning: Failed to insert tx vertex {txid}: {e}")
    
    # Insert input_to_tx edges (address -> tx) with source_index as rank
    for (address, txid, direction, hash_val, block_hash, block_height, block_time,
         utxo_txid, utxo_vout, source_index, value, value_delta, revision) in edges_input:
        ngql = f"""
INSERT EDGE input_to_tx("direction", "txid", "hash", "block_hash", "block_height", "block_time",
                        "utxo_txid", "utxo_vout", "source_index", "value", "value_delta", "revision")
VALUES "addr:{address}"->"tx:{txid}"@{source_index}:(
    {{
        "direction": "{direction}",
        "txid": "{txid}",
        "hash": "{hash_val}",
        "block_hash": "{block_hash}",
        "block_height": {block_height},
        "block_time": {block_time},
        "utxo_txid": "{utxo_txid}",
        "utxo_vout": {utxo_vout},
        "source_index": {source_index},
        "value": {value},
        "value_delta": {value_delta},
        "revision": {revision}
    }}
)
"""
        try:
            ng_session.execute(ngql)
        except Exception as e:
            print(f"Warning: Failed to insert input_to_tx edge {address}->{txid}@{source_index}: {e}")
    
    # Insert tx_to_output edges (tx -> address) with utxo_vout as rank
    for (address, txid, direction, hash_val, block_hash, block_height, block_time,
         utxo_txid, utxo_vout, source_index, value, value_delta, revision) in edges_output:
        ngql = f"""
INSERT EDGE tx_to_output("direction", "txid", "hash", "block_hash", "block_height", "block_time",
                         "utxo_txid", "utxo_vout", "source_index", "value", "value_delta", "revision")
VALUES "tx:{txid}"->"addr:{address}"@{utxo_vout}:(
    {{
        "direction": "{direction}",
        "txid": "{txid}",
        "hash": "{hash_val}",
        "block_hash": "{block_hash}",
        "block_height": {block_height},
        "block_time": {block_time},
        "utxo_txid": "{utxo_txid}",
        "utxo_vout": {utxo_vout},
        "source_index": {source_index},
        "value": {value},
        "value_delta": {value_delta},
        "revision": {revision}
    }}
)
"""
        try:
            ng_session.execute(ngql)
        except Exception as e:
            print(f"Warning: Failed to insert tx_to_output edge {txid}->{address}@{utxo_vout}: {e}")


def main():
    args = parse_args()

    # Import ClickHouse client
    try:
        from clickhouse_driver import Client as CHClient
    except ImportError:
        print("Missing dependency: pip install clickhouse-driver", file=sys.stderr)
        sys.exit(2)

    # Import NebulaGraph client
    try:
        from nebula3.Config import Config
        from nebula3.gclient.net import ConnectionPool
    except ImportError:
        print("Missing dependency: pip install nebula3-python", file=sys.stderr)
        sys.exit(2)

    # Connect to ClickHouse
    ch_client = CHClient(
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password
    )

    # Connect to NebulaGraph
    config = Config()
    config.max_connection_pool_size = 10
    connection_pool = ConnectionPool()
    connection_pool.init([(args.ng_host, args.ng_port)], config)
    ng_session = connection_pool.get_session(args.ng_user, args.ng_password)
    ng_session.execute(f"USE {args.ng_space}")

    # Auto-discover all partitions >= start_partition from ClickHouse
    print(f"Discovering partitions from ClickHouse starting from {args.start_partition}...")
    partitions = discover_partitions(ch_client, int(args.start_partition))
    
    if not partitions:
        print(f"No partitions found >= {args.start_partition}")
        ng_session.release()
        connection_pool.close()
        sys.exit(0)
    
    print(f"Found {len(partitions)} partition(s): {', '.join(partitions)}")

    # Process each discovered partition
    total_blocks = 0
    total_addresses = 0
    for partition in partitions:
        print(f"Processing partition {partition}...")
        blocks = get_blocks_for_partition(ch_client, int(partition))
        print(f"  Found {len(blocks)} blocks")

        for block_month, block_height, block_hash in blocks:
            print(f"  Syncing block {block_height} (hash: {block_hash[:16]}...)")
            addresses = get_addresses_for_block(ch_client, int(partition), block_height)
            
            if addresses:
                insert_vertices_and_edges(ng_session, addresses)
                total_addresses += len(addresses)
                print(f"    Synced {len(addresses)} address records")
            
            total_blocks += 1

    ng_session.release()
    connection_pool.close()
    print(f"\nSync complete! Total blocks: {total_blocks}, Total address records: {total_addresses}")


if __name__ == '__main__':
    main()

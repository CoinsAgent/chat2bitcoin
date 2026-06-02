-- ClickHouse Bitcoin rich block schema
-- Pipeline:
--   bitcoin.blocks       -> bitcoin.transactions
--   bitcoin.transactions -> bitcoin.inputs
--   bitcoin.transactions -> bitcoin.outputs
--   bitcoin.inputs       -> bitcoin.addresses
--   bitcoin.outputs      -> bitcoin.addresses
--
-- "->" means automatic unfolding through Materialized Views.
--
-- Notes:
-- 1. The top-level block `tx` field stores rich transaction objects, not only txid strings.
-- 2. Optional transaction input fields such as coinbase/scriptSig/prevout are Nullable,
--    because coinbase inputs do not have normal txid/vout/prevout fields.
-- 3. `revision` defaults to 0 as requested.
-- 4. PRIMARY KEY is explicit for all tables.
-- 5. ReplacingMergeTree deduplication key is ORDER BY, not PRIMARY KEY.

CREATE DATABASE IF NOT EXISTS bitcoin;

-- ============================================================
-- 1. Rich blocks table
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.blocks
(
    `hash` String,
    `confirmations` UInt64,
    `height` UInt64,
    `version` Int32,
    `versionHex` String,
    `merkleroot` String,
    `time` UInt64,
    `mediantime` UInt64,
    `nonce` UInt64,
    `bits` String,
    `target` String,
    `difficulty` Float64,
    `chainwork` String,
    `nTx` UInt64,
    `previousblockhash` String,
    `nextblockhash` String,
    `strippedsize` UInt64,
    `size` UInt64,
    `weight` UInt64,

    -- Rich transaction objects returned inside the block.
    `tx` Array(Tuple(
        txid String,
        hash String,
        version Int32,
        size UInt64,
        vsize UInt64,
        weight UInt64,
        locktime UInt64,
        vin Array(Tuple(
            coinbase Nullable(String),
            txid Nullable(String),
            vout Nullable(UInt32),
            scriptSig Tuple(
                asm Nullable(String),
                hex Nullable(String)
            ),
            txinwitness Array(String),
            prevout Tuple(
                generated Nullable(Bool),
                height Nullable(UInt64),
                value Nullable(Decimal(20, 8)),
                scriptPubKey Tuple(
                    asm Nullable(String),
                    desc Nullable(String),
                    hex Nullable(String),
                    address Nullable(String),
                    type Nullable(String)
                )
            ),
            sequence UInt64
        )),
        vout Array(Tuple(
            value Decimal(20, 8),
            n UInt32,
            scriptPubKey Tuple(
                asm Nullable(String),
                desc Nullable(String),
                hex Nullable(String),
                address Nullable(String),
                type Nullable(String)
            )
        )),
        fee Nullable(Decimal(20, 8)),
        hex String
    )),

    -- Derived attributes
    `block_datetime` DateTime MATERIALIZED toDateTime(`time`),
    `block_date` Date MATERIALIZED toDate(`block_datetime`),
    `block_month` UInt32 MATERIALIZED toYYYYMM(`block_datetime`),

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY block_month
PRIMARY KEY (`hash`)
ORDER BY (`hash`);


-- ============================================================
-- 2. Transactions table unfolded from bitcoin.blocks.tx
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.transactions
(
    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,
    `block_mediantime` UInt64,

    -- Transaction source attributes
    `txid` String,
    `hash` String,
    `version` Int32,
    `size` UInt64,
    `vsize` UInt64,
    `weight` UInt64,
    `locktime` UInt64,

    `vin` Array(Tuple(
        coinbase Nullable(String),
        txid Nullable(String),
        vout Nullable(UInt32),
        scriptSig Tuple(
            asm Nullable(String),
            hex Nullable(String)
        ),
        txinwitness Array(String),
        prevout Tuple(
            generated Nullable(Bool),
            height Nullable(UInt64),
            value Nullable(Decimal(20, 8)),
            scriptPubKey Tuple(
                asm Nullable(String),
                desc Nullable(String),
                hex Nullable(String),
                address Nullable(String),
                type Nullable(String)
            )
        ),
        sequence UInt64
    )),

    `vout` Array(Tuple(
        value Decimal(20, 8),
        n UInt32,
        scriptPubKey Tuple(
            asm Nullable(String),
            desc Nullable(String),
            hex Nullable(String),
            address Nullable(String),
            type Nullable(String)
        )
    )),

    `fee` Nullable(Decimal(20, 8)),
    `hex` String,

    -- Derived attributes
    `transaction_datetime` DateTime MATERIALIZED toDateTime(`block_time`),
    `transaction_date` Date MATERIALIZED toDate(`transaction_datetime`),
    `transaction_month` UInt32 MATERIALIZED toYYYYMM(`transaction_datetime`),

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY transaction_month
PRIMARY KEY (`txid`)
ORDER BY (`txid`);


-- ============================================================
-- 3. Inputs table unfolded from bitcoin.transactions.vin
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.inputs
(
    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,

    -- Current transaction context
    `txid` String,
    `hash` String,

    -- Input position
    `vin_index` UInt32,

    -- vin source attributes
    `coinbase` Nullable(String),
    `vin_txid` Nullable(String),
    `vin_vout` Nullable(UInt32),
    `scriptSig_asm` Nullable(String),
    `scriptSig_hex` Nullable(String),
    `txinwitness` Array(String),
    `sequence` UInt64,

    -- prevout source attributes
    `prevout_generated` Nullable(Bool),
    `prevout_height` Nullable(UInt64),
    `prevout_value` Nullable(Decimal(20, 8)),
    `prevout_scriptPubKey_asm` Nullable(String),
    `prevout_scriptPubKey_desc` Nullable(String),
    `prevout_scriptPubKey_hex` Nullable(String),
    `prevout_scriptPubKey_address` Nullable(String),
    `prevout_scriptPubKey_type` Nullable(String),

    -- Derived attributes
    `input_datetime` DateTime MATERIALIZED toDateTime(`block_time`),
    `input_date` Date MATERIALIZED toDate(`input_datetime`),
    `input_month` UInt32 MATERIALIZED toYYYYMM(`input_datetime`),

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY input_month
PRIMARY KEY (`txid`, `vin_index`)
ORDER BY (`txid`, `vin_index`);


-- ============================================================
-- 4. Outputs table unfolded from bitcoin.transactions.vout
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.outputs
(
    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,

    -- Current transaction context
    `txid` String,
    `hash` String,

    -- Output position
    `vout_index` UInt32,

    -- vout source attributes
    `value` Decimal(20, 8),
    `n` UInt32,

    -- scriptPubKey source attributes
    `scriptPubKey_asm` Nullable(String),
    `scriptPubKey_desc` Nullable(String),
    `scriptPubKey_hex` Nullable(String),
    `scriptPubKey_address` Nullable(String),
    `scriptPubKey_type` Nullable(String),

    -- Derived attributes
    `output_datetime` DateTime MATERIALIZED toDateTime(`block_time`),
    `output_date` Date MATERIALIZED toDate(`output_datetime`),
    `output_month` UInt32 MATERIALIZED toYYYYMM(`output_datetime`),

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY output_month
PRIMARY KEY (`txid`, `vout_index`)
ORDER BY (`txid`, `vout_index`);


-- ============================================================
-- 5. Addresses ledger table unfolded from outputs and inputs.prevout
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.addresses
(
    `address` String,
    `direction` LowCardinality(String),

    -- Current transaction context
    `txid` String,
    `hash` String,

    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,

    -- UTXO identity
    `utxo_txid` String,
    `utxo_vout` UInt32,

    -- vout_index for outputs, vin_index for inputs
    `source_index` UInt32,

    -- Value
    `value` Decimal(20, 8),
    `value_delta` Decimal(20, 8),

    -- Derived attributes
    `address_datetime` DateTime MATERIALIZED toDateTime(`block_time`),
    `address_date` Date MATERIALIZED toDate(`address_datetime`),
    `address_month` UInt32 MATERIALIZED toYYYYMM(`address_datetime`),

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY address_month
PRIMARY KEY (`address`, `direction`, `txid`, `source_index`)
ORDER BY (`address`, `direction`, `txid`, `source_index`);


-- ============================================================
-- Materialized View 1:
-- bitcoin.blocks -> bitcoin.transactions
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS bitcoin.mv_blocks_to_transactions
TO bitcoin.transactions
AS
SELECT
    `hash` AS block_hash,
    `height` AS block_height,
    `time` AS block_time,
    `mediantime` AS block_mediantime,

    tx_item.txid AS txid,
    tx_item.hash AS hash,
    tx_item.version AS version,
    tx_item.size AS size,
    tx_item.vsize AS vsize,
    tx_item.weight AS weight,
    tx_item.locktime AS locktime,
    tx_item.vin AS vin,
    tx_item.vout AS vout,
    tx_item.fee AS fee,
    tx_item.hex AS hex,

    revision
FROM bitcoin.blocks
ARRAY JOIN tx AS tx_item;


-- ============================================================
-- Materialized View 2:
-- bitcoin.transactions -> bitcoin.inputs
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS bitcoin.mv_transactions_to_inputs
TO bitcoin.inputs
AS
SELECT
    block_hash,
    block_height,
    block_time,

    txid,
    hash,

    toUInt32(vin_index_raw - 1) AS vin_index,

    vin_item.coinbase AS coinbase,
    vin_item.txid AS vin_txid,
    vin_item.vout AS vin_vout,
    vin_item.scriptSig.asm AS scriptSig_asm,
    vin_item.scriptSig.hex AS scriptSig_hex,
    vin_item.txinwitness AS txinwitness,
    vin_item.sequence AS sequence,

    vin_item.prevout.generated AS prevout_generated,
    vin_item.prevout.height AS prevout_height,
    vin_item.prevout.value AS prevout_value,
    vin_item.prevout.scriptPubKey.asm AS prevout_scriptPubKey_asm,
    vin_item.prevout.scriptPubKey.desc AS prevout_scriptPubKey_desc,
    vin_item.prevout.scriptPubKey.hex AS prevout_scriptPubKey_hex,
    vin_item.prevout.scriptPubKey.address AS prevout_scriptPubKey_address,
    vin_item.prevout.scriptPubKey.type AS prevout_scriptPubKey_type,

    revision
FROM bitcoin.transactions
ARRAY JOIN
    arrayEnumerate(vin) AS vin_index_raw,
    vin AS vin_item;


-- ============================================================
-- Materialized View 3:
-- bitcoin.transactions -> bitcoin.outputs
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS bitcoin.mv_transactions_to_outputs
TO bitcoin.outputs
AS
SELECT
    block_hash,
    block_height,
    block_time,

    txid,
    hash,

    toUInt32(vout_index_raw - 1) AS vout_index,

    vout_item.value AS value,
    vout_item.n AS n,

    vout_item.scriptPubKey.asm AS scriptPubKey_asm,
    vout_item.scriptPubKey.desc AS scriptPubKey_desc,
    vout_item.scriptPubKey.hex AS scriptPubKey_hex,
    vout_item.scriptPubKey.address AS scriptPubKey_address,
    vout_item.scriptPubKey.type AS scriptPubKey_type,

    revision
FROM bitcoin.transactions
ARRAY JOIN
    arrayEnumerate(vout) AS vout_index_raw,
    vout AS vout_item;


-- ============================================================
-- Materialized View 4:
-- bitcoin.outputs -> bitcoin.addresses
-- Positive rows: UTXO created
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS bitcoin.mv_outputs_to_addresses
TO bitcoin.addresses
AS
SELECT
    assumeNotNull(scriptPubKey_address) AS address,
    'output' AS direction,

    txid,
    hash,

    block_hash,
    block_height,
    block_time,

    txid AS utxo_txid,
    vout_index AS utxo_vout,

    vout_index AS source_index,

    value,
    value AS value_delta,

    revision
FROM bitcoin.outputs
WHERE isNotNull(scriptPubKey_address)
  AND scriptPubKey_address != '';


-- ============================================================
-- Materialized View 5:
-- bitcoin.inputs -> bitcoin.addresses
-- Negative rows: previous UTXO spent
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS bitcoin.mv_inputs_to_addresses
TO bitcoin.addresses
AS
SELECT
    assumeNotNull(prevout_scriptPubKey_address) AS address,
    'input' AS direction,

    txid,
    hash,

    block_hash,
    block_height,
    block_time,

    assumeNotNull(vin_txid) AS utxo_txid,
    assumeNotNull(vin_vout) AS utxo_vout,

    vin_index AS source_index,

    assumeNotNull(prevout_value) AS value,
    -assumeNotNull(prevout_value) AS value_delta,

    revision
FROM bitcoin.inputs
WHERE isNotNull(prevout_scriptPubKey_address)
  AND prevout_scriptPubKey_address != ''
  AND isNotNull(vin_txid)
  AND isNotNull(vin_vout)
  AND isNotNull(prevout_value);


-- ============================================================
-- Example queries
-- ============================================================

-- Address balance:
-- SELECT
--     address,
--     sum(value_delta) AS balance
-- FROM bitcoin.addresses FINAL
-- WHERE address = '1811f7UUQAkAejj11dU5cVtKUSTfoSVzdm'
-- GROUP BY address;

-- UTXOs for one address:
-- SELECT
--     o.address,
--     o.utxo_txid,
--     o.utxo_vout,
--     o.value
-- FROM
-- (
--     SELECT address, utxo_txid, utxo_vout, value
--     FROM bitcoin.addresses FINAL
--     WHERE address = '1811f7UUQAkAejj11dU5cVtKUSTfoSVzdm'
--       AND direction = 'output'
-- ) AS o
-- LEFT ANTI JOIN
-- (
--     SELECT address, utxo_txid, utxo_vout
--     FROM bitcoin.addresses FINAL
--     WHERE address = '1811f7UUQAkAejj11dU5cVtKUSTfoSVzdm'
--       AND direction = 'input'
-- ) AS i
-- ON  o.address = i.address
-- AND o.utxo_txid = i.utxo_txid
-- AND o.utxo_vout = i.utxo_vout;

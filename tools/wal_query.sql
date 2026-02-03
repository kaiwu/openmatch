-- SQLite helper to materialize wal_query and add indexes.
-- Assumes a virtual table named "walv" already exists.
-- sqlite3>.load ./build/tools/wal_query

-- sqlite3>CREATE VIRTUAL TABLE walv USING wal_query(
-- sqlite3>  file=/tmp/openmatch.wal,
-- sqlite3>  user_data=64,
-- sqlite3>  aux_data=128,
-- sqlite3>  crc32=1
-- sqlite3>);

-- sqlite3>CREATE VIRTUAL TABLE walv USING wal_query(
-- sqlite3>  pattern=/tmp/openmatch_%06u.wal,
-- sqlite3>  index=0,
-- sqlite3>  user_data=64,
-- sqlite3>  aux_data=128
-- sqlite3>);

DROP TABLE IF EXISTS wal;
CREATE TABLE wal AS SELECT * FROM walv;

CREATE INDEX IF NOT EXISTS wal_idx_seq ON wal(seq);
CREATE INDEX IF NOT EXISTS wal_idx_type ON wal(type);
CREATE INDEX IF NOT EXISTS wal_idx_order_id ON wal(order_id);
CREATE INDEX IF NOT EXISTS wal_idx_product_id ON wal(product_id);
CREATE INDEX IF NOT EXISTS wal_idx_maker_id ON wal(maker_id);
CREATE INDEX IF NOT EXISTS wal_idx_taker_id ON wal(taker_id);
CREATE INDEX IF NOT EXISTS wal_idx_timestamp_ns ON wal(timestamp_ns);
CREATE INDEX IF NOT EXISTS wal_idx_type_product ON wal(type, product_id);

-- Local time with fractional seconds
-- SELECT strftime('%Y-%m-%d %H:%M:%f', timestamp_ns / 1e9, 'unixepoch', 'localtime');
-- UTC
-- SELECT strftime('%Y-%m-%d %H:%M:%f', timestamp_ns / 1e9, 'unixepoch');

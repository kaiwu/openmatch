-- SQLite helper to add indexes for wal_query virtual table.
-- Assumes a virtual table named "wal" already exists.

CREATE INDEX IF NOT EXISTS wal_idx_seq ON wal(seq);
CREATE INDEX IF NOT EXISTS wal_idx_type ON wal(type);
CREATE INDEX IF NOT EXISTS wal_idx_order_id ON wal(order_id);
CREATE INDEX IF NOT EXISTS wal_idx_product_id ON wal(product_id);
CREATE INDEX IF NOT EXISTS wal_idx_maker_id ON wal(maker_id);
CREATE INDEX IF NOT EXISTS wal_idx_taker_id ON wal(taker_id);
CREATE INDEX IF NOT EXISTS wal_idx_timestamp_ns ON wal(timestamp_ns);
CREATE INDEX IF NOT EXISTS wal_idx_type_product ON wal(type, product_id);


CREATE TABLE IF NOT EXISTS auctions (
  uuid TEXT PRIMARY KEY,
  item_name TEXT NOT NULL,
  bin BOOLEAN NOT NULL DEFAULT FALSE,
  start_ts BIGINT NOT NULL,
  end_ts BIGINT NOT NULL,
  starting_bid BIGINT NOT NULL DEFAULT 0,
  highest_bid BIGINT NOT NULL DEFAULT 0,
  tier TEXT,
  item_lore TEXT,
  last_seen_ts BIGINT NOT NULL,
  is_ended BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_auctions_name_end ON auctions (item_name, end_ts);
CREATE INDEX IF NOT EXISTS idx_auctions_end ON auctions (end_ts);

CREATE TABLE IF NOT EXISTS sales (
  uuid TEXT PRIMARY KEY,
  item_name TEXT NOT NULL,
  bin BOOLEAN NOT NULL,
  final_price BIGINT NOT NULL,
  ended_ts BIGINT NOT NULL,
  signature TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sales_sig_ended ON sales (signature, ended_ts);

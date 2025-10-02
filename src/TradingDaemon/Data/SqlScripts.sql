CREATE TABLE IF NOT EXISTS prices (
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    value NUMERIC NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);

CREATE TABLE IF NOT EXISTS weights (
    symbol TEXT PRIMARY KEY,
    value NUMERIC NOT NULL,
    asof TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    quantity NUMERIC NOT NULL,
    price NUMERIC NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    quantity NUMERIC NOT NULL,
    price NUMERIC NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pnls (
    id SERIAL PRIMARY KEY,
    trading_date DATE NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    value NUMERIC NOT NULL
);

-- Example upsert statements
-- INSERT INTO prices (symbol, timestamp, value) VALUES ($1,$2,$3)
-- ON CONFLICT (symbol, timestamp) DO UPDATE SET value = EXCLUDED.value;

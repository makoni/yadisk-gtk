ALTER TABLE items ADD COLUMN parent_path TEXT;
ALTER TABLE items ADD COLUMN last_synced_hash TEXT;
ALTER TABLE items ADD COLUMN last_synced_modified INTEGER;

ALTER TABLE states ADD COLUMN retry_at INTEGER;
ALTER TABLE states ADD COLUMN last_success_at INTEGER;
ALTER TABLE states ADD COLUMN last_error_at INTEGER;
ALTER TABLE states ADD COLUMN dirty INTEGER NOT NULL DEFAULT 0;

ALTER TABLE ops_queue ADD COLUMN payload TEXT;
ALTER TABLE ops_queue ADD COLUMN retry_at INTEGER;
ALTER TABLE ops_queue ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS ops_queue_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    payload TEXT,
    attempt INTEGER NOT NULL,
    retry_at INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    UNIQUE(kind, path)
);

INSERT INTO ops_queue_new (id, kind, path, payload, attempt, retry_at, priority)
SELECT id, kind, path, payload, attempt, retry_at, priority FROM ops_queue;

DROP TABLE ops_queue;
ALTER TABLE ops_queue_new RENAME TO ops_queue;

CREATE INDEX IF NOT EXISTS idx_items_parent_path ON items(parent_path);
CREATE INDEX IF NOT EXISTS idx_states_retry_at ON states(retry_at);

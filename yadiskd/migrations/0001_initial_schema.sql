CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    item_type TEXT NOT NULL,
    size INTEGER,
    modified INTEGER,
    hash TEXT,
    resource_id TEXT
);

CREATE TABLE IF NOT EXISTS states (
    item_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    pinned INTEGER NOT NULL,
    last_error TEXT,
    FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sync_cursor (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    cursor TEXT,
    last_sync INTEGER
);

CREATE TABLE IF NOT EXISTS ops_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    attempt INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    renamed_local TEXT NOT NULL,
    created INTEGER NOT NULL,
    reason TEXT NOT NULL
);

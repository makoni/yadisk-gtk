CREATE INDEX IF NOT EXISTS idx_ops_queue_dequeue ON ops_queue(retry_at, priority DESC, id ASC);

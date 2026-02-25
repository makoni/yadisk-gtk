impl IndexStore {
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn new(database_url: &str) -> Result<Self, IndexError> {
        let pool = SqlitePool::connect(database_url).await?;
        let store = Self { pool };
        store.init().await?;
        Ok(store)
    }

    pub async fn new_default() -> Result<Self, IndexError> {
        let db_path = default_db_path()?;
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;
        let store = Self { pool };
        store.init().await?;
        Ok(store)
    }

    pub async fn init(&self) -> Result<(), IndexError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn upsert_item(&self, item: &ItemInput) -> Result<ItemRecord, IndexError> {
        sqlx::query(
            "\n            INSERT INTO items (\n                path,\n                parent_path,\n                name,\n                item_type,\n                size,\n                modified,\n                hash,\n                resource_id,\n                last_synced_hash,\n                last_synced_modified\n            )\n            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)\n            ON CONFLICT(path) DO UPDATE SET\n                parent_path = excluded.parent_path,\n                name = excluded.name,\n                item_type = excluded.item_type,\n                size = excluded.size,\n                modified = excluded.modified,\n                hash = excluded.hash,\n                resource_id = excluded.resource_id,\n                last_synced_hash = excluded.last_synced_hash,\n                last_synced_modified = excluded.last_synced_modified;\n            ",
        )
        .bind(&item.path)
        .bind(&item.parent_path)
        .bind(&item.name)
        .bind(item.item_type.as_str())
        .bind(item.size)
        .bind(item.modified)
        .bind(&item.hash)
        .bind(&item.resource_id)
        .bind(&item.last_synced_hash)
        .bind(item.last_synced_modified)
        .execute(&self.pool)
        .await?;

        self.get_item_by_path(&item.path)
            .await?
            .ok_or(IndexError::MissingItem)
    }

    pub async fn get_item_by_path(&self, path: &str) -> Result<Option<ItemRecord>, IndexError> {
        let row = sqlx::query(
            "SELECT id, path, parent_path, name, item_type, size, modified, hash, resource_id, last_synced_hash, last_synced_modified FROM items WHERE path = ?1",
        )
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let item_type: String = row.try_get("item_type")?;
        Ok(Some(ItemRecord {
            id: row.try_get("id")?,
            path: row.try_get("path")?,
            parent_path: row.try_get("parent_path")?,
            name: row.try_get("name")?,
            item_type: ItemType::parse(&item_type)?,
            size: row.try_get("size")?,
            modified: row.try_get("modified")?,
            hash: row.try_get("hash")?,
            resource_id: row.try_get("resource_id")?,
            last_synced_hash: row.try_get("last_synced_hash")?,
            last_synced_modified: row.try_get("last_synced_modified")?,
        }))
    }

    pub async fn list_items_by_prefix(&self, prefix: &str) -> Result<Vec<ItemRecord>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT id, path, parent_path, name, item_type, size, modified, hash, resource_id, last_synced_hash, last_synced_modified
             FROM items
             WHERE path = ?1 OR path LIKE ?2 OR path = ?3 OR path LIKE ?4
             ORDER BY path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let item_type: String = row.try_get("item_type")?;
            out.push(ItemRecord {
                id: row.try_get("id")?,
                path: row.try_get("path")?,
                parent_path: row.try_get("parent_path")?,
                name: row.try_get("name")?,
                item_type: ItemType::parse(&item_type)?,
                size: row.try_get("size")?,
                modified: row.try_get("modified")?,
                hash: row.try_get("hash")?,
                resource_id: row.try_get("resource_id")?,
                last_synced_hash: row.try_get("last_synced_hash")?,
                last_synced_modified: row.try_get("last_synced_modified")?,
            });
        }
        Ok(out)
    }

    pub async fn delete_item_by_path(&self, path: &str) -> Result<(), IndexError> {
        sqlx::query("DELETE FROM items WHERE path = ?1")
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_state(
        &self,
        item_id: i64,
        state: FileState,
        pinned: bool,
        last_error: Option<&str>,
    ) -> Result<(), IndexError> {
        self.set_state_with_meta(item_id, state, pinned, last_error, StateMeta::default())
            .await
    }

    pub async fn set_state_with_meta(
        &self,
        item_id: i64,
        state: FileState,
        pinned: bool,
        last_error: Option<&str>,
        meta: StateMeta,
    ) -> Result<(), IndexError> {
        sqlx::query(
            "\n            INSERT INTO states (\n                item_id,\n                state,\n                pinned,\n                last_error,\n                retry_at,\n                last_success_at,\n                last_error_at,\n                dirty\n            )\n            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)\n            ON CONFLICT(item_id) DO UPDATE SET\n                state = excluded.state,\n                pinned = excluded.pinned,\n                last_error = excluded.last_error,\n                retry_at = excluded.retry_at,\n                last_success_at = excluded.last_success_at,\n                last_error_at = excluded.last_error_at,\n                dirty = excluded.dirty;\n            ",
        )
        .bind(item_id)
        .bind(state.as_str())
        .bind(if pinned { 1 } else { 0 })
        .bind(last_error)
        .bind(meta.retry_at)
        .bind(meta.last_success_at)
        .bind(meta.last_error_at)
        .bind(if meta.dirty { 1 } else { 0 })
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_state(&self, item_id: i64) -> Result<Option<StateRecord>, IndexError> {
        let row =
            sqlx::query(
                "SELECT item_id, state, pinned, last_error, retry_at, last_success_at, last_error_at, dirty FROM states WHERE item_id = ?1",
            )
                .bind(item_id)
                .fetch_optional(&self.pool)
                .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let state: String = row.try_get("state")?;
        let pinned: i64 = row.try_get("pinned")?;
        let dirty: i64 = row.try_get("dirty")?;

        Ok(Some(StateRecord {
            item_id: row.try_get("item_id")?,
            state: FileState::parse(&state)?,
            pinned: pinned != 0,
            last_error: row.try_get("last_error")?,
            retry_at: row.try_get("retry_at")?,
            last_success_at: row.try_get("last_success_at")?,
            last_error_at: row.try_get("last_error_at")?,
            dirty: dirty != 0,
        }))
    }

    pub async fn list_states_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, FileState)>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT i.path, s.state
             FROM states s
             JOIN items i ON i.id = s.item_id
             WHERE i.path = ?1 OR i.path LIKE ?2 OR i.path = ?3 OR i.path LIKE ?4
             ORDER BY i.path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let path: String = row.try_get("path")?;
            let state: String = row.try_get("state")?;
            out.push((path, FileState::parse(&state)?));
        }
        Ok(out)
    }

    pub async fn list_path_states_with_pin_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, FileState, bool)>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT i.path, s.state, s.pinned
             FROM states s
             JOIN items i ON i.id = s.item_id
             WHERE i.path = ?1 OR i.path LIKE ?2 OR i.path = ?3 OR i.path LIKE ?4
             ORDER BY i.path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let path: String = row.try_get("path")?;
            let state: String = row.try_get("state")?;
            let pinned: i64 = row.try_get("pinned")?;
            out.push((path, FileState::parse(&state)?, pinned != 0));
        }
        Ok(out)
    }

    pub async fn list_pinned_cloud_only_paths_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT i.path
             FROM states s
             JOIN items i ON i.id = s.item_id
             WHERE (i.path = ?1 OR i.path LIKE ?2 OR i.path = ?3 OR i.path LIKE ?4)
                AND s.pinned = 1
                AND s.state = 'cloud_only'
             ORDER BY i.path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| row.try_get::<String, _>("path").map_err(IndexError::from))
            .collect()
    }

    pub async fn set_pinned(&self, item_id: i64, pinned: bool) -> Result<(), IndexError> {
        sqlx::query("UPDATE states SET pinned = ?1 WHERE item_id = ?2")
            .bind(if pinned { 1 } else { 0 })
            .bind(item_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_sync_cursor(
        &self,
        cursor: Option<&str>,
        last_sync: Option<i64>,
    ) -> Result<(), IndexError> {
        sqlx::query(
            "\n            INSERT INTO sync_cursor (id, cursor, last_sync)\n            VALUES (1, ?1, ?2)\n            ON CONFLICT(id) DO UPDATE SET\n                cursor = excluded.cursor,\n                last_sync = excluded.last_sync;\n            ",
        )
        .bind(cursor)
        .bind(last_sync)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_sync_cursor(&self) -> Result<SyncCursor, IndexError> {
        let row = sqlx::query("SELECT cursor, last_sync FROM sync_cursor WHERE id = 1")
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            Ok(SyncCursor {
                cursor: row.try_get("cursor")?,
                last_sync: row.try_get("last_sync")?,
            })
        } else {
            Ok(SyncCursor {
                cursor: None,
                last_sync: None,
            })
        }
    }

    pub async fn enqueue_op(&self, op: &Operation) -> Result<i64, IndexError> {
        let result = sqlx::query(
            "INSERT INTO ops_queue (kind, path, payload, attempt, retry_at, priority) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(kind, path) DO UPDATE SET
                payload = excluded.payload,
                attempt = MIN(ops_queue.attempt, excluded.attempt),
                retry_at = excluded.retry_at,
                priority = MAX(ops_queue.priority, excluded.priority)",
        )
            .bind(operation_kind_as_str(&op.kind))
            .bind(&op.path)
            .bind(&op.payload)
            .bind(op.attempt)
            .bind(op.retry_at)
            .bind(op.priority)
            .execute(&self.pool)
            .await?;

        Ok(result.last_insert_rowid())
    }

    pub async fn delete_ops_for_path(&self, path: &str) -> Result<(), IndexError> {
        sqlx::query("DELETE FROM ops_queue WHERE path = ?1")
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn requeue_op(
        &self,
        op: &Operation,
        retry_at: i64,
        last_error: Option<&str>,
    ) -> Result<(), IndexError> {
        let mut op = op.clone();
        op.attempt = op.attempt.saturating_add(1);
        op.retry_at = Some(retry_at);
        self.enqueue_op(&op).await?;
        if let Some(item) = self.get_item_by_path(&op.path).await? {
            self.set_state_with_meta(
                item.id,
                FileState::Error,
                true,
                last_error,
                StateMeta {
                    retry_at: Some(retry_at),
                    last_success_at: None,
                    last_error_at: Some(retry_at),
                    dirty: true,
                },
            )
            .await?;
        }
        Ok(())
    }

    pub async fn dequeue_op(&self) -> Result<Option<Operation>, IndexError> {
        let row = sqlx::query(
            "SELECT id, kind, path, payload, attempt, retry_at, priority
             FROM ops_queue
             WHERE retry_at IS NULL OR retry_at <= CAST(strftime('%s','now') AS INTEGER)
             ORDER BY priority DESC, id ASC
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let id: i64 = row.try_get("id")?;
        let kind: String = row.try_get("kind")?;
        let operation = Operation {
            kind: parse_operation_kind(&kind)?,
            path: row.try_get("path")?,
            payload: row.try_get("payload")?,
            attempt: row.try_get("attempt")?,
            retry_at: row.try_get("retry_at")?,
            priority: row.try_get("priority")?,
        };

        sqlx::query("DELETE FROM ops_queue WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(Some(operation))
    }

    pub async fn has_ready_op(&self) -> Result<bool, IndexError> {
        let row = sqlx::query(
            "SELECT 1
             FROM ops_queue
             WHERE retry_at IS NULL OR retry_at <= CAST(strftime('%s','now') AS INTEGER)
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn record_conflict(
        &self,
        path: &str,
        renamed_local: &str,
        created: i64,
        reason: &str,
    ) -> Result<i64, IndexError> {
        let result = sqlx::query(
            "INSERT INTO conflicts (path, renamed_local, created, reason) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(path)
        .bind(renamed_local)
        .bind(created)
        .bind(reason)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    pub async fn list_conflicts(&self) -> Result<Vec<ConflictRecord>, IndexError> {
        let rows = sqlx::query(
            "SELECT id, path, renamed_local, created, reason FROM conflicts ORDER BY id ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(ConflictRecord {
                id: row.try_get("id")?,
                path: row.try_get("path")?,
                renamed_local: row.try_get("renamed_local")?,
                created: row.try_get("created")?,
                reason: row.try_get("reason")?,
            });
        }
        Ok(out)
    }
}

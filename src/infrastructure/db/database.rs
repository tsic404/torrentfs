use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

use super::types::DbError;

pub struct Database {
    pub(crate) conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let mut db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    #[allow(dead_code)]
    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let mut db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    pub(crate) fn run_migrations(&mut self) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;
        let user_version: i64 = tx
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .optional()?
            .unwrap_or(0);

        if user_version < 1 {
            Self::migrate_v1(&tx)?;
            tx.pragma_update(None, "user_version", 2)?;
        } else if user_version == 1 {
            Self::migrate_v2(&tx)?;
            tx.pragma_update(None, "user_version", 2)?;
        }

        if user_version < 3 {
            Self::migrate_v3(&tx)?;
            tx.pragma_update(None, "user_version", 3)?;
        }

        if user_version < 4 {
            Self::migrate_v4(&tx)?;
            tx.pragma_update(None, "user_version", 4)?;
        }

        tx.commit()?;

        // v5 runs outside any transaction so that PRAGMA foreign_keys = OFF
        // takes effect — inside a transaction it is silently ignored, causing
        // DROP TABLE to cascade-delete child rows.
        if user_version < 5 {
            self.conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
            Self::migrate_v5(&self.conn)?;
            self.conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            self.conn.pragma_update(None, "user_version", 5)?;
        }

        // v6 also rebuilds `torrents` (DROP TABLE), so foreign_keys must be
        // OFF — otherwise the DROP cascades into the child tables.
        if user_version < 6 {
            self.conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
            Self::migrate_v6(&self.conn)?;
            self.conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            self.conn.pragma_update(None, "user_version", 6)?;
        }

        if user_version < 3 {
            let paths: Vec<String> = {
                let mut stmt = self.conn.prepare(
                    "SELECT DISTINCT source_path FROM torrent_sources WHERE source_path != ''",
                )?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect::<Result<Vec<_>, _>>()?
            };

            for path in paths {
                if let Err(e) = self.ensure_metadata_directories(&path) {
                    tracing::warn!("Failed to create metadata directories for {}: {}", path, e);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn migrate_v1(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS torrents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                file_count INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'pending',
                source_path TEXT NOT NULL DEFAULT '',
                torrent_data BLOB,
                resume_data BLOB,
                created_at DATETIME NOT NULL DEFAULT (datetime('now')),
                UNIQUE(info_hash, source_path)
            );

            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash);
            CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);
            CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path);

            CREATE TABLE IF NOT EXISTS torrent_directories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL,
                parent_id INTEGER,
                name TEXT NOT NULL,
                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_torrent_id ON torrent_directories(torrent_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_parent_id ON torrent_directories(parent_id);

            CREATE TABLE IF NOT EXISTS directory_closure (
                ancestor_id INTEGER NOT NULL,
                descendant_id INTEGER NOT NULL,
                depth INTEGER NOT NULL,
                PRIMARY KEY (ancestor_id, descendant_id),
                FOREIGN KEY (ancestor_id) REFERENCES torrent_directories(id) ON DELETE CASCADE,
                FOREIGN KEY (descendant_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_closure_descendant ON directory_closure(descendant_id);

            CREATE TABLE IF NOT EXISTS torrent_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL,
                directory_id INTEGER,
                name TEXT NOT NULL,
                path TEXT NOT NULL DEFAULT '',
                size INTEGER NOT NULL,
                first_piece INTEGER NOT NULL DEFAULT 0,
                last_piece INTEGER NOT NULL DEFAULT 0,
                piece_start INTEGER,
                piece_end INTEGER,
                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                FOREIGN KEY (directory_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_torrent_files_torrent_id ON torrent_files(torrent_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_files_directory_id ON torrent_files(directory_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path);",
        )?;
        Ok(())
    }

    pub(crate) fn migrate_v2(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "ALTER TABLE torrents ADD COLUMN file_count INTEGER NOT NULL DEFAULT 1;
             ALTER TABLE torrents ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';
             ALTER TABLE torrents ADD COLUMN torrent_data BLOB;
             ALTER TABLE torrents ADD COLUMN resume_data BLOB;

             CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
             CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);

             ALTER TABLE torrent_files ADD COLUMN path TEXT NOT NULL DEFAULT '';
             ALTER TABLE torrent_files ADD COLUMN first_piece INTEGER NOT NULL DEFAULT 0;
             ALTER TABLE torrent_files ADD COLUMN last_piece INTEGER NOT NULL DEFAULT 0;

             CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path);",
        )?;
        Ok(())
    }

    pub(crate) fn migrate_v3(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata_directories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER,
                name TEXT NOT NULL,
                path TEXT NOT NULL UNIQUE,
                FOREIGN KEY (parent_id) REFERENCES metadata_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_metadata_dirs_parent_id ON metadata_directories(parent_id);
            CREATE INDEX IF NOT EXISTS idx_metadata_dirs_path ON metadata_directories(path);

            CREATE TABLE IF NOT EXISTS metadata_directory_closure (
                ancestor_id INTEGER NOT NULL,
                descendant_id INTEGER NOT NULL,
                depth INTEGER NOT NULL,
                PRIMARY KEY (ancestor_id, descendant_id),
                FOREIGN KEY (ancestor_id) REFERENCES metadata_directories(id) ON DELETE CASCADE,
                FOREIGN KEY (descendant_id) REFERENCES metadata_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_metadata_closure_descendant ON metadata_directory_closure(descendant_id);",
        )?;
        Ok(())
    }

    pub(crate) fn migrate_v4(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "ALTER TABLE torrents ADD COLUMN filename TEXT NOT NULL DEFAULT '';
             UPDATE torrents SET filename = name WHERE filename = '';",
        )?;
        Ok(())
    }

    /// Change UNIQUE constraint from (info_hash, source_path) to (source_path, filename)
    /// so that the same info_hash at different source_paths or with different filenames
    /// produces independent data/ mirrors.
    ///
    /// Caller MUST have disabled foreign_keys before calling this, otherwise
    /// DROP TABLE will cascade-delete child rows in torrent_directories /
    /// directory_closure / torrent_files.
    ///
    /// With foreign_keys OFF, the old UNIQUE(info_hash, source_path) constraint may
    /// have left multiple rows per (source_path, filename). Only MAX(id) per key is
    /// retained; the rest would leave orphaned child rows in torrent_files /
    /// torrent_directories / directory_closure (their torrent_id no longer exists
    /// after DROP TABLE, and re-enabling foreign_keys does not retroactively clean
    /// them). We therefore collect the non-retained ids and explicitly DELETE their
    /// child rows before the DROP. The whole migration runs in a transaction for
    /// crash consistency.
    pub(crate) fn migrate_v5(conn: &Connection) -> Result<(), DbError> {
        let tx = conn.unchecked_transaction()?;

        // Child rows of the non-retained torrents must be removed explicitly;
        // foreign_keys is OFF so DROP TABLE will not cascade.
        tx.execute_batch(
            "CREATE TEMP TABLE _v5_orphans AS
             SELECT id FROM torrents
             WHERE id NOT IN (
                 SELECT MAX(id) FROM torrents GROUP BY source_path, filename
             );",
        )?;

        // directory_closure has no direct torrent_id FK — clean rows whose
        // ancestor/descendant directory ids belong to orphaned torrents.
        // CTE computes the orphan directory id set once.
        tx.execute_batch(
            "WITH orphan_dirs AS (
                 SELECT id FROM torrent_directories
                 WHERE torrent_id IN (SELECT id FROM _v5_orphans)
             )
             DELETE FROM directory_closure
             WHERE ancestor_id IN (SELECT id FROM orphan_dirs)
                OR descendant_id IN (SELECT id FROM orphan_dirs);",
        )?;

        tx.execute_batch(
            "DELETE FROM torrent_directories
             WHERE torrent_id IN (SELECT id FROM _v5_orphans);

             DELETE FROM torrent_files
             WHERE torrent_id IN (SELECT id FROM _v5_orphans);",
        )?;

        tx.execute_batch("DROP TABLE IF EXISTS _v5_orphans;")?;

        tx.execute_batch(
            "CREATE TABLE torrents_v5 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                file_count INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'pending',
                source_path TEXT NOT NULL DEFAULT '',
                torrent_data BLOB,
                resume_data BLOB,
                created_at DATETIME NOT NULL DEFAULT (datetime('now')),
                filename TEXT NOT NULL DEFAULT '',
                UNIQUE(source_path, filename)
            );

            -- Keep the latest row for each (source_path, filename) to satisfy
            -- the new UNIQUE constraint; old UNIQUE(info_hash, source_path) could
            -- have left multiple rows with same source_path+filename.
            INSERT INTO torrents_v5
                (id, info_hash, name, total_size, file_count, status,
                 source_path, torrent_data, resume_data, created_at, filename)
            SELECT id, info_hash, name, total_size, file_count, status,
                   source_path, torrent_data, resume_data, created_at, filename
            FROM torrents
            WHERE id IN (
                SELECT MAX(id) FROM torrents GROUP BY source_path, filename
            );

            DROP TABLE torrents;

            ALTER TABLE torrents_v5 RENAME TO torrents;

            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash);
            CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);
            CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path);",
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Split the merged `(source_path, filename)` torrent row into a
    /// content table (`torrents`) and a display-mapping table
    /// (`torrent_sources`), deduplicating content by exact `.torrent` bytes.
    ///
    /// A single torrent file copied to several `source_path` directories now
    /// produces ONE `torrents` row (shared content + file list) plus one
    /// `torrent_sources` row per directory.  Rows whose `torrent_data` is NULL
    /// cannot be deduplicated and keep one content row each.
    ///
    /// A collapsed content keeps the lowest-id row's `resume_data`/`created_at`
    /// — identical bytes share one `info_hash`, hence one download state, so
    /// the folded rows' resume data is the same logical value.
    ///
    /// Caller MUST have disabled foreign_keys; the whole migration runs in a
    /// single transaction.  Re-keying `torrent_files` / `torrent_directories`
    /// uses a temporary `_old_torrent_id` snapshot column so duplicate-content
    /// collapses never collide with an in-flight id swap.
    pub(crate) fn migrate_v6(conn: &Connection) -> Result<(), DbError> {
        let tx = conn.unchecked_transaction()?;

        // Read every existing row once; content dedup needs Rust-side byte
        // equality, which SQL can't key on portably across NULLs.
        let mut stmt = tx.prepare(
            "SELECT id, info_hash, name, total_size, file_count, status,
                    torrent_data, resume_data, created_at, source_path, filename
             FROM torrents ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<Vec<u8>>>(6)?,
                row.get::<_, Option<Vec<u8>>>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
            ))
        })?;
        let old: Vec<_> = rows.collect::<Result<Vec<_>, _>>()?;
        drop(stmt);

        tx.execute_batch(
            "CREATE TABLE torrents_v6 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                file_count INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'pending',
                torrent_data BLOB,
                resume_data BLOB,
                content_hash TEXT NOT NULL DEFAULT '',
                created_at DATETIME NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE torrent_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL REFERENCES torrents_v6(id) ON DELETE CASCADE,
                source_path TEXT NOT NULL DEFAULT '',
                filename TEXT NOT NULL DEFAULT '',
                UNIQUE(source_path, filename)
            );",
        )?;

        use std::collections::HashMap;

        let mut content_id_by_bytes: HashMap<Vec<u8>, i64> = HashMap::new();
        let mut old_to_new: HashMap<i64, i64> = HashMap::new();
        let mut duplicate_old_ids: Vec<i64> = Vec::new();

        for (
            old_id,
            info_hash,
            name,
            total_size,
            file_count,
            status,
            torrent_data,
            resume_data,
            created_at,
            source_path,
            filename,
        ) in &old
        {
            let content_id = match torrent_data {
                Some(bytes) => {
                    if let Some(&cid) = content_id_by_bytes.get(bytes) {
                        duplicate_old_ids.push(*old_id);
                        cid
                    } else {
                        let content_hash = super::content_hash_of(bytes);
                        tx.execute(
                            "INSERT INTO torrents_v6 (info_hash, name, total_size, file_count, status, torrent_data, resume_data, content_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            params![info_hash, name, total_size, file_count, status, bytes, resume_data, content_hash, created_at],
                        )?;
                        let cid = tx.last_insert_rowid();
                        content_id_by_bytes.insert(bytes.clone(), cid);
                        cid
                    }
                }
                None => {
                    tx.execute(
                        "INSERT INTO torrents_v6 (info_hash, name, total_size, file_count, status, torrent_data, resume_data, content_hash, created_at) VALUES (?, ?, ?, ?, ?, NULL, ?, '', ?)",
                        params![info_hash, name, total_size, file_count, status, resume_data, created_at],
                    )?;
                    tx.last_insert_rowid()
                }
            };

            tx.execute(
                "INSERT INTO torrent_sources (torrent_id, source_path, filename) VALUES (?, ?, ?)",
                params![content_id, source_path, filename],
            )?;

            old_to_new.insert(*old_id, content_id);
        }

        // Drop the duplicated file lists of collapsed contents BEFORE re-keying
        // the survivors; a collapsed content keeps the canonical (lowest-id)
        // row's files/directories.
        let dup_list = duplicate_old_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        if !dup_list.is_empty() {
            tx.execute_batch(&format!(
                "WITH orphan_dirs AS (
                     SELECT id FROM torrent_directories WHERE torrent_id IN ({dup_list})
                 )
                 DELETE FROM directory_closure
                 WHERE ancestor_id IN (SELECT id FROM orphan_dirs)
                    OR descendant_id IN (SELECT id FROM orphan_dirs);

                 DELETE FROM torrent_directories WHERE torrent_id IN ({dup_list});
                 DELETE FROM torrent_files WHERE torrent_id IN ({dup_list});"
            ))?;
        }

        // Re-key surviving file/dir rows from the old content id to the new
        // (deduplicated) content id.  A temporary `_old_torrent_id` snapshot
        // column makes the mapping read from a stable value, so a collapse
        // (old ids A,B → new id C) can never corrupt another row mid-update.
        tx.execute_batch(
            "CREATE TEMP TABLE _v6_remap(old_id INTEGER PRIMARY KEY, new_id INTEGER NOT NULL);",
        )?;
        for (old_id, new_id) in &old_to_new {
            tx.execute(
                "INSERT INTO _v6_remap (old_id, new_id) VALUES (?, ?)",
                params![old_id, new_id],
            )?;
        }

        tx.execute_batch(
            "ALTER TABLE torrent_files ADD COLUMN _old_torrent_id INTEGER;
             ALTER TABLE torrent_directories ADD COLUMN _old_torrent_id INTEGER;
             UPDATE torrent_files SET _old_torrent_id = torrent_id;
             UPDATE torrent_directories SET _old_torrent_id = torrent_id;
             UPDATE torrent_files SET torrent_id = (SELECT new_id FROM _v6_remap WHERE old_id = torrent_files._old_torrent_id);
             UPDATE torrent_directories SET torrent_id = (SELECT new_id FROM _v6_remap WHERE old_id = torrent_directories._old_torrent_id);
             ALTER TABLE torrent_files DROP COLUMN _old_torrent_id;
             ALTER TABLE torrent_directories DROP COLUMN _old_torrent_id;
             DROP TABLE _v6_remap;",
        )?;

        // Swap in the content table and rebuild the useful indexes.
        tx.execute_batch(
            "DROP TABLE torrents;
             ALTER TABLE torrents_v6 RENAME TO torrents;
             CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash);
             CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
             CREATE INDEX IF NOT EXISTS idx_torrents_content_hash ON torrents(content_hash);
             CREATE INDEX IF NOT EXISTS idx_torrent_sources_torrent_id ON torrent_sources(torrent_id);
             CREATE INDEX IF NOT EXISTS idx_torrent_sources_source_path ON torrent_sources(source_path);",
        )?;

        tx.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn rebuild_metadata_directories(&mut self) -> Result<(), DbError> {
        let paths: Vec<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT source_path FROM torrent_sources WHERE source_path != ''",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        for path in paths {
            self.ensure_metadata_directories(&path)?;
        }

        Ok(())
    }
}

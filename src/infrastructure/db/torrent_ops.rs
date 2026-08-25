use rusqlite::{params, OptionalExtension};

use super::database::Database;
use super::types::{DbError, FileEntry, InsertTorrentResult, Torrent, TorrentStatus};

impl Database {
    #[allow(dead_code)]
    pub fn insert_torrent(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
    ) -> Result<InsertTorrentResult, DbError> {
        let existing: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM torrents WHERE source_path = ? AND filename = ?",
                params![source_path, filename],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if let Some(id) = existing {
            return Ok(InsertTorrentResult::Duplicate(id));
        }

        self.conn.execute(
            "INSERT INTO torrents (source_path, name, filename, total_size, info_hash, file_count, status) VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            params![source_path, name, filename, total_size, info_hash, file_count],
        )?;

        let id = self.conn.last_insert_rowid();

        if !source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    source_path,
                    e
                );
            }
        }

        Ok(InsertTorrentResult::Inserted(id))
    }

    pub fn set_torrent_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET torrent_data = ? WHERE id = ?",
            params![data, torrent_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn set_resume_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET resume_data = ? WHERE id = ?",
            params![data, torrent_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn set_torrent_status(
        &mut self,
        torrent_id: i64,
        status: &TorrentStatus,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET status = ? WHERE id = ?",
            params![status.as_str(), torrent_id],
        )?;
        Ok(())
    }

    /// Insert torrent and its files atomically in a single transaction.
    /// This prevents orphaned torrent records without file entries.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_torrent_with_files(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
        files: &[FileEntry],
    ) -> Result<InsertTorrentResult, DbError> {
        let tx = self.conn.transaction()?;

        // Check for existing torrent with same source_path and filename
        let existing: Option<i64> = tx
            .query_row(
                "SELECT id FROM torrents WHERE source_path = ? AND filename = ?",
                params![source_path, filename],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if let Some(id) = existing {
            return Ok(InsertTorrentResult::Duplicate(id));
        }

        // Insert torrent record
        tx.execute(
            "INSERT INTO torrents (source_path, name, filename, total_size, info_hash, file_count, status) VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            params![source_path, name, filename, total_size, info_hash, file_count],
        )?;
        let torrent_id = tx.last_insert_rowid();

        // Insert files in the same transaction
        let mut dir_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();

        for file_entry in files {
            let path_parts: Vec<&str> = file_entry.path.split('/').collect();
            if path_parts.is_empty() {
                continue;
            }

            let mut current_parent_id: Option<i64> = None;

            for (i, part) in path_parts.iter().enumerate() {
                let is_file = i == path_parts.len() - 1;
                let current_path = path_parts[..=i].join("/");

                if is_file {
                    tx.execute(
                        "INSERT INTO torrent_files (torrent_id, directory_id, name, path, size) VALUES (?, ?, ?, ?, ?)",
                        params![torrent_id, current_parent_id, part, &file_entry.path, file_entry.size],
                    )?;
                } else {
                    if let Some(&cached_id) = dir_cache.get(&current_path) {
                        current_parent_id = Some(cached_id);
                        continue;
                    }

                    let existing_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                            params![torrent_id, current_parent_id, part],
                            |row| row.get(0),
                        )
                        .optional()?
                        .flatten();

                    if let Some(id) = existing_id {
                        dir_cache.insert(current_path.clone(), id);
                        current_parent_id = Some(id);
                        continue;
                    }

                    tx.execute(
                        "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?, ?, ?)",
                        params![torrent_id, current_parent_id, part],
                    )?;
                    let dir_id = tx.last_insert_rowid();

                    tx.execute(
                        "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
                        params![dir_id, dir_id],
                    )?;

                    if let Some(parent_id) = current_parent_id {
                        tx.execute(
                            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
                             SELECT ancestor_id, ?, depth + 1 FROM directory_closure WHERE descendant_id = ?",
                            params![dir_id, parent_id],
                        )?;
                    }

                    dir_cache.insert(current_path.clone(), dir_id);
                    current_parent_id = Some(dir_id);
                }
            }
        }

        tx.commit()?;

        // Ensure metadata directories exist for the source_path
        if !source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    source_path,
                    e
                );
            }
        }

        Ok(InsertTorrentResult::Inserted(torrent_id))
    }

    /// Replace the content of an existing torrent row in place (TSI-2381).
    ///
    /// Used when a `.torrent` file is overwritten with different content
    /// under the same `(source_path, filename)`: the row keeps its id and
    /// location, but its name, info_hash, size, file list, and raw bytes are
    /// re-pointed at the new torrent. Atomic: the row and its files are
    /// updated inside one transaction so a crash cannot leave a torrent
    /// without matching file entries.
    #[allow(clippy::too_many_arguments)]
    pub fn replace_torrent_content(
        &mut self,
        torrent_id: i64,
        name: &str,
        info_hash: &str,
        total_size: i64,
        file_count: i64,
        files: &[FileEntry],
        data: &[u8],
    ) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;

        tx.execute(
            "UPDATE torrents SET name = ?, info_hash = ?, total_size = ?, file_count = ?, \
             torrent_data = ?, status = 'pending', resume_data = NULL WHERE id = ?",
            params![name, info_hash, total_size, file_count, data, torrent_id],
        )?;

        // Rebuild the file/directory tree from scratch for this torrent.
        tx.execute(
            "DELETE FROM directory_closure WHERE descendant_id IN \
             (SELECT id FROM torrent_directories WHERE torrent_id = ?1) \
             OR ancestor_id IN (SELECT id FROM torrent_directories WHERE torrent_id = ?1)",
            params![torrent_id],
        )?;
        tx.execute(
            "DELETE FROM torrent_directories WHERE torrent_id = ?",
            params![torrent_id],
        )?;
        tx.execute(
            "DELETE FROM torrent_files WHERE torrent_id = ?",
            params![torrent_id],
        )?;

        let mut dir_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();

        for file_entry in files {
            let path_parts: Vec<&str> = file_entry.path.split('/').collect();
            if path_parts.is_empty() {
                continue;
            }

            let mut current_parent_id: Option<i64> = None;

            for (i, part) in path_parts.iter().enumerate() {
                let is_file = i == path_parts.len() - 1;
                let current_path = path_parts[..=i].join("/");

                if is_file {
                    tx.execute(
                        "INSERT INTO torrent_files (torrent_id, directory_id, name, path, size) VALUES (?, ?, ?, ?, ?)",
                        params![torrent_id, current_parent_id, part, &file_entry.path, file_entry.size],
                    )?;
                } else {
                    if let Some(&cached_id) = dir_cache.get(&current_path) {
                        current_parent_id = Some(cached_id);
                        continue;
                    }

                    let existing_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                            params![torrent_id, current_parent_id, part],
                            |row| row.get(0),
                        )
                        .optional()?
                        .flatten();

                    if let Some(id) = existing_id {
                        dir_cache.insert(current_path.clone(), id);
                        current_parent_id = Some(id);
                        continue;
                    }

                    tx.execute(
                        "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?, ?, ?)",
                        params![torrent_id, current_parent_id, part],
                    )?;
                    let dir_id = tx.last_insert_rowid();

                    tx.execute(
                        "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
                        params![dir_id, dir_id],
                    )?;

                    if let Some(parent_id) = current_parent_id {
                        tx.execute(
                            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
                             SELECT ancestor_id, ?, depth + 1 FROM directory_closure WHERE descendant_id = ?",
                            params![dir_id, parent_id],
                        )?;
                    }

                    dir_cache.insert(current_path.clone(), dir_id);
                    current_parent_id = Some(dir_id);
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn get_torrent_by_source_path(
        &self,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE source_path = ?",
                params![source_path],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    #[allow(dead_code)]
    pub fn get_torrent_by_info_hash(&self, info_hash: &str) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE info_hash = ?",
                params![info_hash],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn delete_torrent(&mut self, torrent_id: i64) -> Result<(), DbError> {
        self.conn
            .execute("DELETE FROM torrents WHERE id = ?", params![torrent_id])?;
        Ok(())
    }

    pub fn get_all_torrents(&self) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
             FROM torrents ORDER BY id",
        )?;

        let torrents = stmt
            .query_map([], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    filename: row.get(3)?,
                    total_size: row.get(4)?,
                    info_hash: row.get(5)?,
                    file_count: row.get(6)?,
                    status: row.get::<_, String>(7)?.into(),
                    torrent_data: row.get(8)?,
                    resume_data: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    #[allow(dead_code)]
    pub fn get_torrents_by_status(&self, status: &TorrentStatus) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
             FROM torrents WHERE status = ? ORDER BY id",
        )?;

        let torrents = stmt
            .query_map(params![status.as_str()], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    filename: row.get(3)?,
                    total_size: row.get(4)?,
                    info_hash: row.get(5)?,
                    file_count: row.get(6)?,
                    status: row.get::<_, String>(7)?.into(),
                    torrent_data: row.get(8)?,
                    resume_data: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    pub fn get_torrents_by_source_path(&self, source_path: &str) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
             FROM torrents WHERE source_path = ? ORDER BY id",
        )?;

        let torrents = stmt
            .query_map(params![source_path], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    filename: row.get(3)?,
                    total_size: row.get(4)?,
                    info_hash: row.get(5)?,
                    file_count: row.get(6)?,
                    status: row.get::<_, String>(7)?.into(),
                    torrent_data: row.get(8)?,
                    resume_data: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    /// Get all torrents whose source_path matches exactly or is a child of the given prefix.
    /// For example, with prefix "os", this returns torrents with source_path "os",
    /// "os/linux", "os/bsd", etc.
    pub fn get_torrents_by_source_path_prefix(
        &self,
        source_path: &str,
    ) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
             FROM torrents WHERE source_path = ?1 OR source_path LIKE ?2 ESCAPE '\\' ORDER BY id",
        )?;
        // Escape LIKE metacharacters (%, _) and the escape character itself so that
        // source_path is matched literally rather than treated as a glob pattern.
        let escaped_path = super::escape_like_pattern(source_path);
        let pattern = if source_path.is_empty() {
            "%".to_string()
        } else {
            format!("{}/%", escaped_path)
        };
        let torrents = stmt
            .query_map(params![source_path, pattern], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    filename: row.get(3)?,
                    total_size: row.get(4)?,
                    info_hash: row.get(5)?,
                    file_count: row.get(6)?,
                    status: row.get::<_, String>(7)?.into(),
                    torrent_data: row.get(8)?,
                    resume_data: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    /// Get counts of torrents grouped by status.
    /// Returns (pending, downloading, seeding, error, total).
    pub fn get_torrent_counts_by_status(&self) -> Result<(i64, i64, i64, i64, i64), DbError> {
        let mut pending: i64 = 0;
        let mut downloading: i64 = 0;
        let mut seeding: i64 = 0;
        let mut error: i64 = 0;

        let mut stmt = self
            .conn
            .prepare("SELECT status, COUNT(*) as cnt FROM torrents GROUP BY status")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (status, cnt) = row?;
            match status.as_str() {
                "pending" => pending = cnt,
                "downloading" => downloading = cnt,
                "seeding" => seeding = cnt,
                "error" => error = cnt,
                _ => {}
            }
        }
        let total = pending + downloading + seeding + error;
        Ok((pending, downloading, seeding, error, total))
    }

    /// Get all torrents that share a given info_hash.
    pub fn get_torrents_by_infohash(
        &self,
        info_hash: &str,
    ) -> Result<Vec<(i64, String, String, String)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, filename, source_path FROM torrents WHERE info_hash = ? ORDER BY id",
        )?;
        let rows = stmt.query_map(params![info_hash], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (id, name, filename, source_path) = row?;
            result.push((id, name, filename, source_path));
        }
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn get_torrent_id_by_name_and_source_path(
        &self,
        name: &str,
        source_path: &str,
    ) -> Result<Option<i64>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id FROM torrents WHERE name = ? AND source_path = ?",
                params![name, source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        Ok(result)
    }

    pub fn get_torrent_by_id(&self, id: i64) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE id = ?",
                params![id],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    /// Rename a torrent by updating its name, filename, and source_path fields.
    pub fn rename_torrent(
        &mut self,
        torrent_id: i64,
        new_name: &str,
        new_filename: &str,
        new_source_path: &str,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET name = ?, filename = ?, source_path = ? WHERE id = ?",
            params![new_name, new_filename, new_source_path, torrent_id],
        )?;

        // Ensure metadata directories exist for the new source_path
        if !new_source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(new_source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    new_source_path,
                    e
                );
            }
        }

        Ok(())
    }

    /// Get a torrent by its filename and source_path.
    pub fn get_torrent_by_filename_and_source_path(
        &self,
        filename: &str,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE filename = ? AND source_path = ?",
                params![filename, source_path],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }
}

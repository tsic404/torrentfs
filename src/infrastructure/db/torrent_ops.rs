use rusqlite::{params, OptionalExtension};

use super::database::Database;
use super::types::{
    DbError, FileEntry, InsertTorrentOutcome, InsertTorrentResult, MoveOverwriteResult, Torrent,
    TorrentStatus,
};

/// Shared SELECT for a source-location view of a torrent: one row per
/// `(source_path, filename)` entry joined against its shared content row.
const TORRENT_SELECT: &str = "SELECT s.id, s.torrent_id, s.source_path, t.name, s.filename, \
     t.total_size, t.info_hash, t.file_count, t.status, t.torrent_data, t.resume_data, \
     t.created_at FROM torrent_sources s JOIN torrents t ON t.id = s.torrent_id";

fn torrent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Torrent> {
    Ok(Torrent {
        id: row.get(0)?,
        torrent_id: row.get(1)?,
        source_path: row.get(2)?,
        name: row.get(3)?,
        filename: row.get(4)?,
        total_size: row.get(5)?,
        info_hash: row.get(6)?,
        file_count: row.get(7)?,
        status: row.get::<_, String>(8)?.into(),
        torrent_data: row.get(9)?,
        resume_data: row.get(10)?,
        created_at: row.get(11)?,
    })
}

/// Insert a torrent's file/directory tree under a content id inside `tx`.
/// Content-scoped: two source paths sharing one content row also share this
/// file list, so it must be built once per content.
fn insert_files_in_tx(
    tx: &rusqlite::Transaction<'_>,
    content_id: i64,
    files: &[FileEntry],
) -> Result<(), DbError> {
    let mut dir_cache: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

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
                    params![content_id, current_parent_id, part, &file_entry.path, file_entry.size],
                )?;
            } else {
                if let Some(&cached_id) = dir_cache.get(&current_path) {
                    current_parent_id = Some(cached_id);
                    continue;
                }

                let existing_id: Option<i64> = tx
                    .query_row(
                        "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                        params![content_id, current_parent_id, part],
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
                    params![content_id, current_parent_id, part],
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

    Ok(())
}

impl Database {
    /// Resolve a source-location id (`torrent_sources.id`) to its shared
    /// content id (`torrents.id`).
    pub(crate) fn resolve_content_id(&self, source_id: i64) -> Result<Option<i64>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT torrent_id FROM torrent_sources WHERE id = ?",
                params![source_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        Ok(result)
    }

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
        let tx = self.conn.transaction()?;

        let existing: Option<i64> = tx
            .query_row(
                "SELECT id FROM torrent_sources WHERE source_path = ? AND filename = ?",
                params![source_path, filename],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if let Some(id) = existing {
            return Ok(InsertTorrentResult::Duplicate(id));
        }

        // No raw bytes: dedupe content by the metadata identity that would be
        // identical for an identical `.torrent` file.  Keeps `insert_torrent`
        // (a public trait API) consistent with the byte-dedup data path.
        let content_id: Option<i64> = tx
            .query_row(
                "SELECT id FROM torrents WHERE info_hash = ? AND total_size = ? AND file_count = ?",
                params![info_hash, total_size, file_count],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        let content_id = match content_id {
            Some(cid) => cid,
            None => {
                tx.execute(
                    "INSERT INTO torrents (info_hash, name, total_size, file_count, status) VALUES (?, ?, ?, ?, 'pending')",
                    params![info_hash, name, total_size, file_count],
                )?;
                tx.last_insert_rowid()
            }
        };

        tx.execute(
            "INSERT INTO torrent_sources (torrent_id, source_path, filename) VALUES (?, ?, ?)",
            params![content_id, source_path, filename],
        )?;
        let source_id = tx.last_insert_rowid();

        tx.commit()?;

        if !source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    source_path,
                    e
                );
            }
        }

        Ok(InsertTorrentResult::Inserted(source_id))
    }

    pub fn set_torrent_data(&mut self, source_id: i64, data: &[u8]) -> Result<(), DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(());
        };
        let content_hash = super::content_hash_of(data);
        self.conn.execute(
            "UPDATE torrents SET torrent_data = ?, content_hash = ? WHERE id = ?",
            params![data, content_hash, content_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn set_resume_data(&mut self, source_id: i64, data: &[u8]) -> Result<(), DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(());
        };
        self.conn.execute(
            "UPDATE torrents SET resume_data = ? WHERE id = ?",
            params![data, content_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn set_torrent_status(
        &mut self,
        source_id: i64,
        status: &TorrentStatus,
    ) -> Result<(), DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(());
        };
        self.conn.execute(
            "UPDATE torrents SET status = ? WHERE id = ?",
            params![status.as_str(), content_id],
        )?;
        Ok(())
    }

    /// Insert torrent and its files atomically in a single transaction.
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
        let outcome = self.insert_torrent_with_files_inner(
            source_path,
            name,
            filename,
            total_size,
            info_hash,
            file_count,
            files,
            None,
        )?;
        Ok(if outcome.source_reused {
            InsertTorrentResult::Duplicate(outcome.source_id)
        } else {
            InsertTorrentResult::Inserted(outcome.source_id)
        })
    }

    /// Insert a torrent, its files, AND its raw `.torrent` bytes in one
    /// transaction, deduplicating content by exact bytes.  When an identical
    /// torrent already exists at a different source path, only a new source
    /// row is created (the content row and its file list are shared); when the
    /// same `(source_path, filename)` is overwritten, the source is repointed
    /// at the (possibly new) content and the orphaned old content is deleted.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_torrent_with_files_and_data(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
        files: &[FileEntry],
        data: &[u8],
    ) -> Result<InsertTorrentOutcome, DbError> {
        self.insert_torrent_with_files_inner(
            source_path,
            name,
            filename,
            total_size,
            info_hash,
            file_count,
            files,
            Some(data),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_torrent_with_files_inner(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
        files: &[FileEntry],
        data: Option<&[u8]>,
    ) -> Result<InsertTorrentOutcome, DbError> {
        let tx = self.conn.transaction()?;

        let existing_source: Option<i64> = tx
            .query_row(
                "SELECT id FROM torrent_sources WHERE source_path = ? AND filename = ?",
                params![source_path, filename],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        // Content dedup is by exact bytes; info_hash alone is insufficient
        // (two files may share an info_hash while differing in trackers).
        // `content_hash` narrows via an index; `torrent_data = ?` verifies the
        // exact bytes so a theoretical SHA-1 collision cannot merge files.
        let content_hash = data.map(super::content_hash_of);
        let existing_content: Option<i64> = match data {
            Some(bytes) => tx
                .query_row(
                    "SELECT id FROM torrents WHERE content_hash = ?1 AND torrent_data = ?2",
                    params![content_hash, bytes],
                    |row| row.get(0),
                )
                .optional()?
                .flatten(),
            None => tx
                .query_row(
                    "SELECT id FROM torrents WHERE info_hash = ? AND total_size = ? AND file_count = ?",
                    params![info_hash, total_size, file_count],
                    |row| row.get(0),
                )
                .optional()?
                .flatten(),
        };

        let (content_id, content_created) = match existing_content {
            Some(cid) => (cid, false),
            None => {
                tx.execute(
                    "INSERT INTO torrents (info_hash, name, total_size, file_count, status, torrent_data, content_hash) VALUES (?, ?, ?, ?, 'pending', ?, ?)",
                    params![
                        info_hash,
                        name,
                        total_size,
                        file_count,
                        data.map(|d| d.to_vec()),
                        content_hash.unwrap_or_default(),
                    ],
                )?;
                let cid = tx.last_insert_rowid();
                insert_files_in_tx(&tx, cid, files)?;
                (cid, true)
            }
        };

        let (source_id, source_reused, stale_info_hash) = match existing_source {
            Some(sid) => {
                let old: Option<(i64, String)> = tx
                    .query_row(
                        "SELECT s.torrent_id, t.info_hash FROM torrent_sources s JOIN torrents t ON t.id = s.torrent_id WHERE s.id = ?",
                        params![sid],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;

                tx.execute(
                    "UPDATE torrent_sources SET torrent_id = ? WHERE id = ?",
                    params![content_id, sid],
                )?;

                // The overwritten content may now be unreferenced: delete it
                // (and, via FK cascade, its files/directories) so a long-lived
                // daemon does not accumulate orphaned content rows.
                let mut stale = None;
                if let Some((old_content_id, old_info_hash)) = old {
                    if old_content_id != content_id {
                        let refs: i64 = tx.query_row(
                            "SELECT COUNT(*) FROM torrent_sources WHERE torrent_id = ?",
                            params![old_content_id],
                            |row| row.get(0),
                        )?;
                        if refs == 0 {
                            tx.execute(
                                "DELETE FROM torrents WHERE id = ?",
                                params![old_content_id],
                            )?;
                            // Release the old info_hash's handle/pieces only
                            // when NO source (across all contents) still
                            // references it — another content may share the
                            // same info_hash with different bytes.
                            let remaining: i64 = tx.query_row(
                                "SELECT COUNT(*) FROM torrent_sources s JOIN torrents t ON t.id = s.torrent_id WHERE t.info_hash = ?",
                                params![old_info_hash],
                                |row| row.get(0),
                            )?;
                            if remaining == 0 {
                                stale = Some(old_info_hash);
                            }
                        }
                    }
                }
                (sid, true, stale)
            }
            None => {
                tx.execute(
                    "INSERT INTO torrent_sources (torrent_id, source_path, filename) VALUES (?, ?, ?)",
                    params![content_id, source_path, filename],
                )?;
                (tx.last_insert_rowid(), false, None)
            }
        };

        tx.commit()?;

        if !source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    source_path,
                    e
                );
            }
        }

        Ok(InsertTorrentOutcome {
            source_id,
            content_id,
            content_created,
            source_reused,
            stale_info_hash,
        })
    }

    pub fn get_torrent_by_source_path(
        &self,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError> {
        let sql = format!("{TORRENT_SELECT} WHERE s.source_path = ? ORDER BY s.id LIMIT 1");
        let result = self
            .conn
            .query_row(&sql, params![source_path], torrent_from_row)
            .optional()?;
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn get_torrent_by_info_hash(&self, info_hash: &str) -> Result<Option<Torrent>, DbError> {
        let sql = format!("{TORRENT_SELECT} WHERE t.info_hash = ? ORDER BY s.id LIMIT 1");
        let result = self
            .conn
            .query_row(&sql, params![info_hash], torrent_from_row)
            .optional()?;
        Ok(result)
    }

    pub fn delete_torrent(&mut self, source_id: i64) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;

        let content_id: Option<i64> = tx
            .query_row(
                "SELECT torrent_id FROM torrent_sources WHERE id = ?",
                params![source_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        tx.execute(
            "DELETE FROM torrent_sources WHERE id = ?",
            params![source_id],
        )?;

        if let Some(content_id) = content_id {
            let refs: i64 = tx.query_row(
                "SELECT COUNT(*) FROM torrent_sources WHERE torrent_id = ?",
                params![content_id],
                |row| row.get(0),
            )?;
            if refs == 0 {
                // Cascade deletes files/directories/closure.
                tx.execute("DELETE FROM torrents WHERE id = ?", params![content_id])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn get_all_torrents(&self) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self
            .conn
            .prepare(&format!("{TORRENT_SELECT} ORDER BY s.id"))?;
        let torrents = stmt
            .query_map([], torrent_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(torrents)
    }

    #[allow(dead_code)]
    pub fn get_torrents_by_status(&self, status: &TorrentStatus) -> Result<Vec<Torrent>, DbError> {
        let sql = format!("{TORRENT_SELECT} WHERE t.status = ? ORDER BY s.id");
        let mut stmt = self.conn.prepare(&sql)?;
        let torrents = stmt
            .query_map(params![status.as_str()], torrent_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(torrents)
    }

    pub fn get_torrents_by_source_path(&self, source_path: &str) -> Result<Vec<Torrent>, DbError> {
        let sql = format!("{TORRENT_SELECT} WHERE s.source_path = ? ORDER BY s.id");
        let mut stmt = self.conn.prepare(&sql)?;
        let torrents = stmt
            .query_map(params![source_path], torrent_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(torrents)
    }

    /// Get all torrents whose source_path matches exactly or is a child of the
    /// given prefix.
    pub fn get_torrents_by_source_path_prefix(
        &self,
        source_path: &str,
    ) -> Result<Vec<Torrent>, DbError> {
        let escaped_path = super::escape_like_pattern(source_path);
        let pattern = if source_path.is_empty() {
            "%".to_string()
        } else {
            format!("{}/%", escaped_path)
        };
        let sql = format!(
            "{TORRENT_SELECT} WHERE s.source_path = ?1 OR s.source_path LIKE ?2 ESCAPE '\\' ORDER BY s.id"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let torrents = stmt
            .query_map(params![source_path, pattern], torrent_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(torrents)
    }

    /// Get counts of torrents grouped by status.  Counts source entries (one
    /// per `(source_path, filename)`), grouped by the shared content's status.
    pub fn get_torrent_counts_by_status(&self) -> Result<(i64, i64, i64, i64, i64), DbError> {
        let mut pending: i64 = 0;
        let mut downloading: i64 = 0;
        let mut seeding: i64 = 0;
        let mut error: i64 = 0;

        let mut stmt = self.conn.prepare(
            "SELECT t.status, COUNT(*) AS cnt FROM torrent_sources s JOIN torrents t ON t.id = s.torrent_id GROUP BY t.status",
        )?;
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

    /// Get all source entries that share a given info_hash (across all
    /// contents — two files with the same info_hash but different bytes both
    /// appear here).
    pub fn get_torrents_by_infohash(
        &self,
        info_hash: &str,
    ) -> Result<Vec<(i64, String, String, String)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT s.id, t.name, s.filename, s.source_path FROM torrent_sources s JOIN torrents t ON t.id = s.torrent_id WHERE t.info_hash = ? ORDER BY s.id",
        )?;
        let rows = stmt.query_map(params![info_hash], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
                "SELECT s.id FROM torrent_sources s JOIN torrents t ON t.id = s.torrent_id WHERE t.name = ? AND s.source_path = ?",
                params![name, source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        Ok(result)
    }

    pub fn get_torrent_by_id(&self, source_id: i64) -> Result<Option<Torrent>, DbError> {
        let sql = format!("{TORRENT_SELECT} WHERE s.id = ?");
        let result = self
            .conn
            .query_row(&sql, params![source_id], torrent_from_row)
            .optional()?;
        Ok(result)
    }

    /// Rename a source entry by updating its filename and source_path.  The
    /// content name is shared across source entries and therefore untouched.
    pub fn rename_torrent(
        &mut self,
        source_id: i64,
        new_filename: &str,
        new_source_path: &str,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrent_sources SET filename = ?, source_path = ? WHERE id = ?",
            params![new_filename, new_source_path, source_id],
        )?;

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

    /// Atomically move a torrent over an existing destination in a single
    /// transaction: look up the source row, delete the destination row (if
    /// any), then re-point the source at the destination. The
    /// `UNIQUE(source_path, filename)` constraint makes this indivisible —
    /// re-pointing first collides with the destination, while deleting first
    /// loses the destination if the re-point later fails. Returns
    /// `SourceAbsent` when the source row is missing (a failed pending add
    /// leaves no row); otherwise `Moved`, carrying the removed destination's
    /// `(id, info_hash)` (`None` when the destination had no persisted row).
    pub fn move_torrent_replacing(
        &mut self,
        source_filename: &str,
        source_path: &str,
        dest_filename: &str,
        dest_path: &str,
    ) -> Result<MoveOverwriteResult, DbError> {
        let tx = self.conn.transaction()?;

        let source_id: Option<i64> = tx
            .query_row(
                "SELECT id FROM torrent_sources WHERE filename = ? AND source_path = ?",
                params![source_filename, source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        let Some(source_id) = source_id else {
            return Ok(MoveOverwriteResult::SourceAbsent);
        };

        // Delete the destination source entry (and its orphaned content) in
        // the same transaction as the re-point, mirroring `delete_torrent`.
        let removed_target: Option<(i64, String)> = tx
            .query_row(
                "SELECT s.id, t.info_hash FROM torrent_sources s JOIN torrents t ON t.id = s.torrent_id WHERE s.filename = ? AND s.source_path = ?",
                params![dest_filename, dest_path],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .map(|(dest_source_id, info_hash)| {
                let content_id: Option<i64> = tx
                    .query_row(
                        "SELECT torrent_id FROM torrent_sources WHERE id = ?",
                        params![dest_source_id],
                        |row| row.get(0),
                    )
                    .optional()?
                    .flatten();

                tx.execute("DELETE FROM torrent_sources WHERE id = ?", params![dest_source_id])?;

                if let Some(content_id) = content_id {
                    let refs: i64 = tx.query_row(
                        "SELECT COUNT(*) FROM torrent_sources WHERE torrent_id = ?",
                        params![content_id],
                        |row| row.get(0),
                    )?;
                    if refs == 0 {
                        // Cascade deletes files/directories/closure.
                        tx.execute("DELETE FROM torrents WHERE id = ?", params![content_id])?;
                    }
                }

                Ok::<_, DbError>((dest_source_id, info_hash))
            })
            .transpose()?;

        // Preserve the source's content (bencode name); only its `filename`
        // and `source_path` change — matching `rename_torrent`'s contract.
        tx.execute(
            "UPDATE torrent_sources SET filename = ?, source_path = ? WHERE id = ?",
            params![dest_filename, dest_path, source_id],
        )?;

        tx.commit()?;

        // Ensure metadata directories exist for the destination path, outside
        // the transaction (best-effort, mirroring `rename_torrent`).
        if !dest_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(dest_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    dest_path,
                    e
                );
            }
        }

        Ok(MoveOverwriteResult::Moved { removed_target })
    }

    /// Get a torrent by its filename and source_path.
    pub fn get_torrent_by_filename_and_source_path(
        &self,
        filename: &str,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError> {
        let sql = format!(
            "{TORRENT_SELECT} WHERE s.filename = ? AND s.source_path = ? ORDER BY s.id LIMIT 1"
        );
        let result = self
            .conn
            .query_row(&sql, params![filename, source_path], torrent_from_row)
            .optional()?;
        Ok(result)
    }
}

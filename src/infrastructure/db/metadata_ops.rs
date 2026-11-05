use rusqlite::{params, Connection, OptionalExtension};

use super::database::Database;
use super::types::DbError;

impl Database {
    pub fn ensure_metadata_directories(&mut self, source_path: &str) -> Result<(), DbError> {
        Self::ensure_metadata_directories_on(&self.conn, source_path)
    }

    /// Create the directory chain for `source_path` on an open connection.
    ///
    /// Taking the connection explicitly lets the torrent-insert path run this
    /// inside its own transaction: the directory rows and the source row are
    /// then atomic (a failure leaves neither a source row nor half a directory
    /// tree), and every statement commits once instead of per path segment.
    pub(super) fn ensure_metadata_directories_on(
        conn: &Connection,
        source_path: &str,
    ) -> Result<(), DbError> {
        let parts: Vec<&str> = source_path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.is_empty() {
            return Ok(());
        }

        let mut current_path = String::new();
        let mut parent_id: Option<i64> = None;

        for part in parts {
            if current_path.is_empty() {
                current_path = part.to_string();
            } else {
                current_path = format!("{}/{}", current_path, part);
            }

            let existing_id: Option<i64> = conn
                .query_row(
                    "SELECT id FROM metadata_directories WHERE path = ?",
                    params![&current_path],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            if let Some(id) = existing_id {
                parent_id = Some(id);
                continue;
            }

            conn.execute(
                "INSERT INTO metadata_directories (parent_id, name, path) VALUES (?, ?, ?)",
                params![parent_id, part, &current_path],
            )?;
            let dir_id = conn.last_insert_rowid();

            conn.execute(
                "INSERT INTO metadata_directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
                params![dir_id, dir_id],
            )?;

            if let Some(pid) = parent_id {
                conn.execute(
                    "INSERT INTO metadata_directory_closure (ancestor_id, descendant_id, depth)
                     SELECT ancestor_id, ?, depth + 1 FROM metadata_directory_closure WHERE descendant_id = ?",
                    params![dir_id, pid],
                )?;
            }

            parent_id = Some(dir_id);
        }

        Ok(())
    }

    /// Delete a metadata directory by its path.
    pub fn delete_metadata_directory(&mut self, path: &str) -> Result<(), DbError> {
        self.conn.execute(
            "DELETE FROM metadata_directories WHERE path = ?",
            params![path],
        )?;
        Ok(())
    }

    /// Rename a metadata directory: its own name, path and parent link, the
    /// descendant paths, and the torrent source paths.
    pub fn rename_metadata_directory(
        &mut self,
        old_path: &str,
        new_name: &str,
        new_path: &str,
    ) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;

        // 1. Update the directory itself, including its parent link: `data/`
        // lists children by `parent_id`, so a stale link kept the moved
        // subtree under its old location however often it was re-read.
        let new_parent_id: Option<i64> = match new_path.rsplit_once('/') {
            Some((parent_path, _)) => tx
                .query_row(
                    "SELECT id FROM metadata_directories WHERE path = ?",
                    params![parent_path],
                    |row| row.get(0),
                )
                .optional()?,
            None => None,
        };
        tx.execute(
            "UPDATE metadata_directories SET name = ?, path = ?, parent_id = ? WHERE path = ?",
            params![new_name, new_path, new_parent_id, old_path],
        )?;

        // 2. Update all child directories' paths (replace the old_path prefix
        // with new_path).
        //
        // Both the cut position and the prefix test are done in SQLite's own
        // text semantics: `length()`/`substr()` count characters, so a
        // non-ASCII directory name keeps the suffix intact (`substr(path,
        // length(prefix) + 1)`), while a Rust `str::len()` byte count would
        // cut too far and corrupt every descendant path. `substr(...) = ...`
        // is a case-sensitive comparison, unlike `LIKE`, which is
        // case-insensitive for ASCII and would sweep `Movies/...` into a
        // rename of `movies`.
        let old_prefix = format!("{}/", old_path);
        let new_prefix = format!("{}/", new_path);
        tx.execute(
            "UPDATE metadata_directories SET path = ?1 || substr(path, length(?2) + 1)
             WHERE substr(path, 1, length(?2)) = ?2",
            params![new_prefix, old_prefix],
        )?;

        // 3. Update source entries whose source_path matches exactly
        tx.execute(
            "UPDATE torrent_sources SET source_path = ? WHERE source_path = ?",
            params![new_path, old_path],
        )?;

        // 4. Update source entries whose source_path starts with old_path/
        tx.execute(
            "UPDATE torrent_sources SET source_path = ?1 || substr(source_path, length(?2) + 1)
             WHERE substr(source_path, 1, length(?2)) = ?2",
            params![new_prefix, old_prefix],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Get all metadata directories with their id, parent_id, name, and path.
    #[allow(clippy::type_complexity)]
    pub fn get_all_metadata_directories(
        &self,
    ) -> Result<Vec<(i64, Option<i64>, String, String)>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, parent_id, name, path FROM metadata_directories ORDER BY path")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_source_path_prefixes(&self, prefix: &str) -> Result<Vec<String>, DbError> {
        let names: Vec<String> = if prefix.is_empty() {
            let mut stmt = self.conn.prepare(
                "SELECT name FROM metadata_directories WHERE parent_id IS NULL ORDER BY name",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let parent_id: Option<i64> = self
                .conn
                .query_row(
                    "SELECT id FROM metadata_directories WHERE path = ?",
                    params![prefix],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            match parent_id {
                Some(pid) => {
                    let mut stmt = self.conn.prepare(
                        "SELECT name FROM metadata_directories WHERE parent_id = ? ORDER BY name",
                    )?;
                    let rows = stmt.query_map(params![pid], |row| row.get(0))?;
                    rows.collect::<Result<Vec<_>, _>>()?
                }
                None => Vec::new(),
            }
        };

        Ok(names)
    }
}

use rusqlite::{params, OptionalExtension};

use super::database::Database;
use super::types::{DbError, FileEntry, TorrentDirectory, TorrentFile};

impl Database {
    /// Insert a torrent's file tree.  `source_id` is a source-location id;
    /// files/directories are stored under its shared content id so identical
    /// torrents share one file list.
    #[allow(dead_code)]
    pub fn insert_files(&mut self, source_id: i64, files: &[FileEntry]) -> Result<(), DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(());
        };

        let tx = self.conn.transaction()?;

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

        tx.commit()?;
        Ok(())
    }

    pub fn get_files_by_torrent_id(&self, source_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(Vec::new());
        };

        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
             FROM torrent_files WHERE torrent_id = ? ORDER BY id",
        )?;

        let files = stmt
            .query_map(params![content_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    #[allow(dead_code)]
    pub fn get_subdirectory_ids(&self, parent_id: i64) -> Result<Vec<i64>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id FROM torrent_directories WHERE parent_id = ?")?;

        let ids = stmt
            .query_map(params![parent_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ids)
    }

    pub fn get_files_in_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
             FROM torrent_files WHERE directory_id = ?",
        )?;

        let files = stmt
            .query_map(params![directory_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn get_root_files(&self, source_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(Vec::new());
        };

        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
             FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL",
        )?;

        let files = stmt
            .query_map(params![content_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn get_torrent_directory(
        &self,
        source_id: i64,
        parent_id: Option<i64>,
        name: &str,
    ) -> Result<Option<TorrentDirectory>, DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(None);
        };

        let result = self.conn
            .query_row(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                params![content_id, parent_id, name],
                |row| {
                    Ok(TorrentDirectory {
                        id: row.get(0)?,
                        torrent_id: row.get(1)?,
                        parent_id: row.get(2)?,
                        name: row.get(3)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_torrent_directory_by_id(
        &self,
        dir_id: i64,
    ) -> Result<Option<TorrentDirectory>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE id = ?",
                params![dir_id],
                |row| {
                    Ok(TorrentDirectory {
                        id: row.get(0)?,
                        torrent_id: row.get(1)?,
                        parent_id: row.get(2)?,
                        name: row.get(3)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_torrent_directories_by_parent(
        &self,
        parent_id: Option<i64>,
        source_id: i64,
    ) -> Result<Vec<TorrentDirectory>, DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(Vec::new());
        };

        let mut stmt = if parent_id.is_none() {
            self.conn.prepare(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS NULL",
            )?
        } else {
            self.conn.prepare(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id = ?",
            )?
        };

        let dirs = if parent_id.is_none() {
            stmt.query_map(params![content_id], |row| {
                Ok(TorrentDirectory {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    parent_id: row.get(2)?,
                    name: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![content_id, parent_id], |row| {
                Ok(TorrentDirectory {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    parent_id: row.get(2)?,
                    name: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        Ok(dirs)
    }

    #[allow(dead_code)]
    pub fn get_all_files_under_directory(
        &self,
        directory_id: i64,
    ) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT f.id, f.torrent_id, f.directory_id, f.name, f.path, f.size, f.first_piece, f.last_piece, f.piece_start, f.piece_end
             FROM torrent_files f
             JOIN directory_closure c ON f.directory_id = c.descendant_id
             WHERE c.ancestor_id = ?",
        )?;

        let files = stmt
            .query_map(params![directory_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    #[allow(dead_code)]
    pub fn get_file_by_path(
        &self,
        source_id: i64,
        path: &str,
    ) -> Result<Option<TorrentFile>, DbError> {
        let Some(content_id) = self.resolve_content_id(source_id)? else {
            return Ok(None);
        };

        let parts: Vec<&str> = path.split('/').collect();
        if parts.is_empty() {
            return Ok(None);
        }

        let file_name = parts.last().unwrap();
        let dir_path_parts: Vec<&str> = parts[..parts.len() - 1].to_vec();

        if dir_path_parts.is_empty() {
            let result = self.conn
                .query_row(
                    "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
                     FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL AND name = ?",
                    params![content_id, file_name],
                    |row| {
                        Ok(TorrentFile {
                            id: row.get(0)?,
                            torrent_id: row.get(1)?,
                            directory_id: row.get(2)?,
                            name: row.get(3)?,
                            path: row.get(4)?,
                            size: row.get(5)?,
                            first_piece: row.get(6)?,
                            last_piece: row.get(7)?,
                            piece_start: row.get(8)?,
                            piece_end: row.get(9)?,
                        })
                    },
                )
                .optional()?;

            return Ok(result);
        }

        let dir_id = self.resolve_directory_path(content_id, &dir_path_parts)?;

        match dir_id {
            Some(did) => {
                let result = self.conn
                    .query_row(
                        "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
                         FROM torrent_files WHERE torrent_id = ? AND directory_id = ? AND name = ?",
                        params![content_id, did, file_name],
                        |row| {
                            Ok(TorrentFile {
                                id: row.get(0)?,
                                torrent_id: row.get(1)?,
                                directory_id: row.get(2)?,
                                name: row.get(3)?,
                                path: row.get(4)?,
                                size: row.get(5)?,
                                first_piece: row.get(6)?,
                                last_piece: row.get(7)?,
                                piece_start: row.get(8)?,
                                piece_end: row.get(9)?,
                            })
                        },
                    )
                    .optional()?;
                Ok(result)
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    fn resolve_directory_path(
        &self,
        content_id: i64,
        parts: &[&str],
    ) -> Result<Option<i64>, DbError> {
        let mut current_parent: Option<i64> = None;

        for part in parts {
            let existing_id: Option<i64> = self.conn
                .query_row(
                    "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                    params![content_id, current_parent, part],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            match existing_id {
                Some(id) => current_parent = Some(id),
                None => return Ok(None),
            }
        }

        Ok(current_parent)
    }
}

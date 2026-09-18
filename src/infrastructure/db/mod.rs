//! Database module — SQLite-backed persistence layer for torrent metadata.
//!
//! Split from the original monolithic `db.rs` (2277 lines) into focused sub-modules:
//! - `types` — data types and error types
//! - `database` — Database struct, connection management, migrations
//! - `torrent_ops` — torrent CRUD operations
//! - `file_ops` — file and directory query operations
//! - `metadata_ops` — metadata directory management operations

/// Escape SQLite LIKE metacharacters (`%`, `_`) and the escape character (`\`)
/// in a literal string so it can be embedded into a LIKE pattern and matched
/// verbatim. Must be paired with `ESCAPE '\\'` in the SQL statement.
pub(super) fn escape_like_pattern(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// SHA-1 hex of a `.torrent` file's raw bytes, used as the indexed dedup key
/// on `torrents.content_hash`.  The index narrows the lookup to a hash
/// match; callers still compare `torrent_data` bytes exactly so a (theoretical)
/// SHA-1 collision can never merge two distinct files.
pub(super) fn content_hash_of(data: &[u8]) -> String {
    hex::encode(sha1_smol::Sha1::from(data).digest().bytes())
}

mod database;
mod file_ops;
mod metadata_ops;
#[cfg(test)]
mod tests;
mod torrent_ops;
mod types;

use crate::domain::repository::{FileRepository, TorrentRepository};

pub use database::Database;
pub use types::{
    DbError, FileEntry, InsertTorrentOutcome, InsertTorrentResult, MoveOverwriteResult, Torrent,
    TorrentDirectory, TorrentFile, TorrentStatus,
};

// ---- Repository trait implementations for Database ----

impl TorrentRepository for Database {
    fn insert_torrent(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
    ) -> Result<InsertTorrentResult, DbError> {
        self.insert_torrent(
            source_path,
            name,
            filename,
            total_size,
            info_hash,
            file_count,
        )
    }

    fn insert_torrent_with_files(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
        files: &[FileEntry],
    ) -> Result<InsertTorrentResult, DbError> {
        self.insert_torrent_with_files(
            source_path,
            name,
            filename,
            total_size,
            info_hash,
            file_count,
            files,
        )
    }

    fn set_torrent_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
        self.set_torrent_data(torrent_id, data)
    }

    fn set_resume_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
        self.set_resume_data(torrent_id, data)
    }

    fn set_torrent_status(
        &mut self,
        torrent_id: i64,
        status: &TorrentStatus,
    ) -> Result<(), DbError> {
        self.set_torrent_status(torrent_id, status)
    }

    fn get_torrent_by_id(&self, id: i64) -> Result<Option<Torrent>, DbError> {
        self.get_torrent_by_id(id)
    }

    fn get_torrent_by_source_path(&self, source_path: &str) -> Result<Option<Torrent>, DbError> {
        self.get_torrent_by_source_path(source_path)
    }

    fn get_torrent_by_info_hash(&self, info_hash: &str) -> Result<Option<Torrent>, DbError> {
        self.get_torrent_by_info_hash(info_hash)
    }

    fn get_torrent_by_filename_and_source_path(
        &self,
        filename: &str,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError> {
        self.get_torrent_by_filename_and_source_path(filename, source_path)
    }

    fn get_torrent_id_by_name_and_source_path(
        &self,
        name: &str,
        source_path: &str,
    ) -> Result<Option<i64>, DbError> {
        self.get_torrent_id_by_name_and_source_path(name, source_path)
    }

    fn get_all_torrents(&self) -> Result<Vec<Torrent>, DbError> {
        self.get_all_torrents()
    }

    fn get_torrents_by_status(&self, status: &TorrentStatus) -> Result<Vec<Torrent>, DbError> {
        self.get_torrents_by_status(status)
    }

    fn get_torrents_by_source_path(&self, source_path: &str) -> Result<Vec<Torrent>, DbError> {
        self.get_torrents_by_source_path(source_path)
    }

    fn get_torrent_counts_by_status(&self) -> Result<(i64, i64, i64, i64, i64), DbError> {
        self.get_torrent_counts_by_status()
    }

    fn get_torrents_by_infohash(
        &self,
        info_hash: &str,
    ) -> Result<Vec<(i64, String, String, String)>, DbError> {
        self.get_torrents_by_infohash(info_hash)
    }

    fn delete_torrent(&mut self, torrent_id: i64) -> Result<(), DbError> {
        self.delete_torrent(torrent_id)
    }

    fn rename_torrent(
        &mut self,
        source_id: i64,
        new_filename: &str,
        new_source_path: &str,
    ) -> Result<(), DbError> {
        self.rename_torrent(source_id, new_filename, new_source_path)
    }
}

impl FileRepository for Database {
    fn insert_files(&mut self, torrent_id: i64, files: &[FileEntry]) -> Result<(), DbError> {
        self.insert_files(torrent_id, files)
    }

    fn get_files_by_torrent_id(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        self.get_files_by_torrent_id(torrent_id)
    }

    fn get_root_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        self.get_root_files(torrent_id)
    }

    fn get_files_in_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        self.get_files_in_directory(directory_id)
    }

    fn get_all_files_under_directory(
        &self,
        directory_id: i64,
    ) -> Result<Vec<TorrentFile>, DbError> {
        self.get_all_files_under_directory(directory_id)
    }

    fn get_file_by_path(
        &self,
        torrent_id: i64,
        path: &str,
    ) -> Result<Option<TorrentFile>, DbError> {
        self.get_file_by_path(torrent_id, path)
    }

    fn get_subdirectory_ids(&self, parent_id: i64) -> Result<Vec<i64>, DbError> {
        self.get_subdirectory_ids(parent_id)
    }

    fn get_torrent_directory(
        &self,
        torrent_id: i64,
        parent_id: Option<i64>,
        name: &str,
    ) -> Result<Option<TorrentDirectory>, DbError> {
        self.get_torrent_directory(torrent_id, parent_id, name)
    }

    fn get_torrent_directory_by_id(
        &self,
        dir_id: i64,
    ) -> Result<Option<TorrentDirectory>, DbError> {
        self.get_torrent_directory_by_id(dir_id)
    }

    fn get_torrent_directories_by_parent(
        &self,
        parent_id: Option<i64>,
        torrent_id: i64,
    ) -> Result<Vec<TorrentDirectory>, DbError> {
        self.get_torrent_directories_by_parent(parent_id, torrent_id)
    }

    fn ensure_metadata_directories(&mut self, source_path: &str) -> Result<(), DbError> {
        self.ensure_metadata_directories(source_path)
    }

    fn delete_metadata_directory(&mut self, path: &str) -> Result<(), DbError> {
        self.delete_metadata_directory(path)
    }

    fn rename_metadata_directory(
        &mut self,
        old_path: &str,
        new_name: &str,
        new_path: &str,
    ) -> Result<(), DbError> {
        self.rename_metadata_directory(old_path, new_name, new_path)
    }

    fn get_all_metadata_directories(
        &self,
    ) -> Result<Vec<(i64, Option<i64>, String, String)>, DbError> {
        self.get_all_metadata_directories()
    }

    fn get_source_path_prefixes(&self, prefix: &str) -> Result<Vec<String>, DbError> {
        self.get_source_path_prefixes(prefix)
    }
}

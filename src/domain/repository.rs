//! Repository traits — abstract interfaces for persistence operations.
//!
//! These traits define the contract that any storage backend must fulfill.
//! The current implementation lives in `crate::db::Database`.

use super::types::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};

/// Repository trait for torrent CRUD operations.
pub trait TorrentRepository {
    fn insert_torrent(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
    ) -> Result<InsertTorrentResult, DbError>;

    fn insert_torrent_with_files(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
        files: &[FileEntry],
    ) -> Result<InsertTorrentResult, DbError>;

    fn set_torrent_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError>;

    fn set_resume_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError>;

    fn set_torrent_status(
        &mut self,
        torrent_id: i64,
        status: &TorrentStatus,
    ) -> Result<(), DbError>;

    fn get_torrent_by_id(&self, id: i64) -> Result<Option<Torrent>, DbError>;

    fn get_torrent_by_source_path(&self, source_path: &str) -> Result<Option<Torrent>, DbError>;

    fn get_torrent_by_info_hash(&self, info_hash: &str) -> Result<Option<Torrent>, DbError>;

    fn get_torrent_by_filename_and_source_path(
        &self,
        filename: &str,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError>;

    fn get_torrent_id_by_name_and_source_path(
        &self,
        name: &str,
        source_path: &str,
    ) -> Result<Option<i64>, DbError>;

    fn get_all_torrents(&self) -> Result<Vec<Torrent>, DbError>;

    fn get_torrents_by_status(&self, status: &TorrentStatus) -> Result<Vec<Torrent>, DbError>;

    fn get_torrents_by_source_path(&self, source_path: &str) -> Result<Vec<Torrent>, DbError>;

    fn get_torrent_counts_by_status(&self) -> Result<(i64, i64, i64, i64, i64), DbError>;

    fn get_torrents_by_infohash(
        &self,
        info_hash: &str,
    ) -> Result<Vec<(i64, String, String, String)>, DbError>;

    fn delete_torrent(&mut self, torrent_id: i64) -> Result<(), DbError>;

    fn rename_torrent(
        &mut self,
        source_id: i64,
        new_filename: &str,
        new_source_path: &str,
    ) -> Result<(), DbError>;
}

/// Repository trait for file, directory, and metadata-directory operations.
pub trait FileRepository {
    fn insert_files(&mut self, torrent_id: i64, files: &[FileEntry]) -> Result<(), DbError>;

    fn get_files_by_torrent_id(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError>;

    fn get_root_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError>;

    fn get_files_in_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError>;

    fn get_all_files_under_directory(&self, directory_id: i64)
        -> Result<Vec<TorrentFile>, DbError>;

    fn get_file_by_path(&self, torrent_id: i64, path: &str)
        -> Result<Option<TorrentFile>, DbError>;

    fn get_subdirectory_ids(&self, parent_id: i64) -> Result<Vec<i64>, DbError>;

    fn get_torrent_directory(
        &self,
        torrent_id: i64,
        parent_id: Option<i64>,
        name: &str,
    ) -> Result<Option<TorrentDirectory>, DbError>;

    fn get_torrent_directory_by_id(&self, dir_id: i64)
        -> Result<Option<TorrentDirectory>, DbError>;

    fn get_torrent_directories_by_parent(
        &self,
        parent_id: Option<i64>,
        torrent_id: i64,
    ) -> Result<Vec<TorrentDirectory>, DbError>;

    fn ensure_metadata_directories(&mut self, source_path: &str) -> Result<(), DbError>;

    fn delete_metadata_directory(&mut self, path: &str) -> Result<(), DbError>;

    fn rename_metadata_directory(
        &mut self,
        old_path: &str,
        new_name: &str,
        new_path: &str,
    ) -> Result<(), DbError>;

    fn get_all_metadata_directories(
        &self,
    ) -> Result<Vec<(i64, Option<i64>, String, String)>, DbError>;

    fn get_source_path_prefixes(&self, prefix: &str) -> Result<Vec<String>, DbError>;
}

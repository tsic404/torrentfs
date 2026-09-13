use super::*;
use rusqlite::Connection;
use tempfile::{NamedTempFile, TempDir};

#[test]
fn test_open_in_memory() {
    let db = Database::open_in_memory();
    assert!(db.is_ok());
}

#[test]
fn test_insert_and_get_torrent() {
    let mut db = Database::open_in_memory().unwrap();

    let result = db
        .insert_torrent(
            "test/path",
            "Test Torrent",
            "Test Torrent",
            1024,
            "abc123",
            5,
        )
        .unwrap();
    assert_eq!(result, InsertTorrentResult::Inserted(1));

    let torrent = db.get_torrent_by_source_path("test/path").unwrap().unwrap();
    assert_eq!(torrent.name, "Test Torrent");
    assert_eq!(torrent.total_size, 1024);
    assert_eq!(torrent.file_count, 5);
    assert_eq!(torrent.status, TorrentStatus::Pending);
}

#[test]
fn test_same_info_hash_different_source_path() {
    let mut db = Database::open_in_memory().unwrap();

    let result1 = db
        .insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
        .unwrap();
    assert_eq!(result1, InsertTorrentResult::Inserted(1));

    let result2 = db
        .insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash1", 2)
        .unwrap();
    assert_eq!(result2, InsertTorrentResult::Inserted(2));

    let torrent1 = db.get_torrent_by_source_path("path1").unwrap().unwrap();
    let torrent2 = db.get_torrent_by_source_path("path2").unwrap().unwrap();
    assert_eq!(torrent1.info_hash, torrent2.info_hash);
    assert_eq!(torrent1.id, 1);
    assert_eq!(torrent2.id, 2);
}

#[test]
fn test_duplicate_source_path_and_filename() {
    let mut db = Database::open_in_memory().unwrap();

    // Same source_path + same filename → Duplicate
    db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
        .unwrap();
    let result = db
        .insert_torrent("path1", "Torrent 1 again", "Torrent 1", 2048, "hash1", 2)
        .unwrap();
    assert_eq!(result, InsertTorrentResult::Duplicate(1));

    // Same source_path + different filename → Inserted (independent mirror)
    let result = db
        .insert_torrent("path1", "Torrent 2", "Torrent 2", 2048, "hash1", 2)
        .unwrap();
    assert!(matches!(result, InsertTorrentResult::Inserted(_)));
}

#[test]
fn test_torrent_status() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.status, TorrentStatus::Pending);

    db.set_torrent_status(torrent_id, &TorrentStatus::Downloading)
        .unwrap();
    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.status, TorrentStatus::Downloading);

    db.set_torrent_status(torrent_id, &TorrentStatus::Seeding)
        .unwrap();
    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.status, TorrentStatus::Seeding);

    db.set_torrent_status(torrent_id, &TorrentStatus::Error)
        .unwrap();
    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.status, TorrentStatus::Error);
}

#[test]
fn test_torrent_data() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert!(torrent.torrent_data.is_none());
    assert!(torrent.resume_data.is_none());

    let test_data = vec![1, 2, 3, 4, 5];
    db.set_torrent_data(torrent_id, &test_data).unwrap();
    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.torrent_data, Some(test_data));

    let resume_data = vec![10, 20, 30];
    db.set_resume_data(torrent_id, &resume_data).unwrap();
    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.resume_data, Some(resume_data));
}

#[test]
fn test_get_torrents_by_status() {
    let mut db = Database::open_in_memory().unwrap();

    let id1 = match db
        .insert_torrent("path1", "T1", "T1", 100, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let id2 = match db
        .insert_torrent("path2", "T2", "T2", 200, "hash2", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let _id3 = match db
        .insert_torrent("path3", "T3", "T3", 300, "hash3", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.set_torrent_status(id1, &TorrentStatus::Downloading)
        .unwrap();
    db.set_torrent_status(id2, &TorrentStatus::Seeding).unwrap();

    let pending = db.get_torrents_by_status(&TorrentStatus::Pending).unwrap();
    assert_eq!(pending.len(), 1);

    let downloading = db
        .get_torrents_by_status(&TorrentStatus::Downloading)
        .unwrap();
    assert_eq!(downloading.len(), 1);

    let seeding = db.get_torrents_by_status(&TorrentStatus::Seeding).unwrap();
    assert_eq!(seeding.len(), 1);
}

#[test]
fn test_insert_files() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 3)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![
        FileEntry {
            path: "dir1/file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "dir1/file2.txt".to_string(),
            size: 200,
        },
        FileEntry {
            path: "dir2/file3.txt".to_string(),
            size: 300,
        },
    ];

    db.insert_files(torrent_id, &files).unwrap();

    let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
    assert_eq!(all_files.len(), 3);
}

#[test]
fn test_file_path_field_populated() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test.torrent", 1024, "hash1", 3)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![
        FileEntry {
            path: "dir1/file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "file2.txt".to_string(),
            size: 200,
        },
        FileEntry {
            path: "a/b/c/deep.txt".to_string(),
            size: 300,
        },
    ];

    db.insert_files(torrent_id, &files).unwrap();

    let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
    assert_eq!(all_files.len(), 3);

    // Verify path field is correctly populated
    let file1 = all_files.iter().find(|f| f.name == "file1.txt").unwrap();
    assert_eq!(file1.path, "dir1/file1.txt");

    let file2 = all_files.iter().find(|f| f.name == "file2.txt").unwrap();
    assert_eq!(file2.path, "file2.txt");

    let deep = all_files.iter().find(|f| f.name == "deep.txt").unwrap();
    assert_eq!(deep.path, "a/b/c/deep.txt");
}

#[test]
fn test_get_subdirectory_ids() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 2)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![
        FileEntry {
            path: "dir1/file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "dir2/file2.txt".to_string(),
            size: 200,
        },
    ];

    db.insert_files(torrent_id, &files).unwrap();

    let root_dirs = db
        .get_torrent_directories_by_parent(None, torrent_id)
        .unwrap();
    assert_eq!(root_dirs.len(), 2);
}

#[test]
fn test_delete_torrent_cascade() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![FileEntry {
        path: "file.txt".to_string(),
        size: 100,
    }];
    db.insert_files(torrent_id, &files).unwrap();

    db.delete_torrent(torrent_id).unwrap();

    let torrent = db.get_torrent_by_source_path("path1").unwrap();
    assert!(torrent.is_none());

    let files = db.get_files_by_torrent_id(torrent_id).unwrap();
    assert!(files.is_empty());
}

#[test]
fn test_get_files_in_directory() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 3)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![
        FileEntry {
            path: "dir1/file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "dir1/file2.txt".to_string(),
            size: 200,
        },
        FileEntry {
            path: "file3.txt".to_string(),
            size: 300,
        },
    ];

    db.insert_files(torrent_id, &files).unwrap();

    let dirs = db
        .get_torrent_directories_by_parent(None, torrent_id)
        .unwrap();
    let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();

    let dir_files = db.get_files_in_directory(dir1.id).unwrap();
    assert_eq!(dir_files.len(), 2);
}

#[test]
fn test_get_all_files_under_directory() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 2)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![
        FileEntry {
            path: "dir1/subdir/file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "dir1/file2.txt".to_string(),
            size: 200,
        },
    ];

    db.insert_files(torrent_id, &files).unwrap();

    let dirs = db
        .get_torrent_directories_by_parent(None, torrent_id)
        .unwrap();
    let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();

    let all_files = db.get_all_files_under_directory(dir1.id).unwrap();
    assert_eq!(all_files.len(), 2);
}

#[test]
fn test_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    {
        let mut db = Database::open(path).unwrap();
        db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
            .unwrap();
    }

    {
        let db = Database::open(path).unwrap();
        let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
        assert_eq!(torrent.name, "Test");
        assert_eq!(torrent.status, TorrentStatus::Pending);
    }
}

#[test]
fn test_get_torrent_by_info_hash() {
    let mut db = Database::open_in_memory().unwrap();

    db.insert_torrent("path1", "Test", "Test", 1024, "abc123", 1)
        .unwrap();

    let torrent = db.get_torrent_by_info_hash("abc123").unwrap().unwrap();
    assert_eq!(torrent.source_path, "path1");
}

#[test]
fn test_get_all_torrents() {
    let mut db = Database::open_in_memory().unwrap();

    db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
        .unwrap();
    db.insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash2", 1)
        .unwrap();

    let torrents = db.get_all_torrents().unwrap();
    assert_eq!(torrents.len(), 2);
}

#[test]
fn test_nested_directory_structure() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![FileEntry {
        path: "a/b/c/file.txt".to_string(),
        size: 100,
    }];

    db.insert_files(torrent_id, &files).unwrap();

    let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
    assert_eq!(all_files.len(), 1);
}

#[test]
fn test_get_torrents_by_source_path() {
    let mut db = Database::open_in_memory().unwrap();

    db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
        .unwrap();
    db.insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash2", 1)
        .unwrap();
    db.insert_torrent("other", "Torrent 3", "Torrent 3", 3072, "hash3", 1)
        .unwrap();

    let torrents = db.get_torrents_by_source_path("path1").unwrap();
    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].name, "Torrent 1");

    let torrents = db.get_torrents_by_source_path("nonexistent").unwrap();
    assert_eq!(torrents.len(), 0);
}

#[test]
fn test_get_torrents_by_source_path_prefix() {
    let mut db = Database::open_in_memory().unwrap();

    // Insert torrents at various source_path hierarchy levels
    db.insert_torrent("os", "Ubuntu", "ubuntu.iso.torrent", 1024, "hash1", 1)
        .unwrap();
    db.insert_torrent("os/linux", "Debian", "debian.iso.torrent", 2048, "hash2", 1)
        .unwrap();
    db.insert_torrent("os/bsd", "FreeBSD", "freebsd.iso.torrent", 3072, "hash3", 1)
        .unwrap();
    db.insert_torrent("other", "Other", "other.torrent", 4096, "hash4", 1)
        .unwrap();

    // Prefix "os" should return torrents at "os", "os/linux", "os/bsd" (3 total)
    let torrents = db.get_torrents_by_source_path_prefix("os").unwrap();
    assert_eq!(torrents.len(), 3);
    let names: Vec<&str> = torrents.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"Ubuntu"));
    assert!(names.contains(&"Debian"));
    assert!(names.contains(&"FreeBSD"));

    // Prefix "os/linux" should return only the exact match and any deeper children
    let torrents = db.get_torrents_by_source_path_prefix("os/linux").unwrap();
    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].name, "Debian");

    // Prefix "other" should return only "other" (no deeper children)
    let torrents = db.get_torrents_by_source_path_prefix("other").unwrap();
    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].name, "Other");

    // Non-existent prefix returns empty
    let torrents = db
        .get_torrents_by_source_path_prefix("nonexistent")
        .unwrap();
    assert_eq!(torrents.len(), 0);

    // LIKE metacharacters in the prefix must be matched literally, not as
    // wildcards: "a_b" must NOT match "aXb" or "aYb".
    db.insert_torrent("aXb", "Should Not Match 1", "a1.torrent", 1024, "h_1", 1)
        .unwrap();
    db.insert_torrent("a_b", "Literal Underscore", "a2.torrent", 2048, "h_2", 1)
        .unwrap();
    db.insert_torrent("a_b/sub", "Literal Child", "a3.torrent", 4096, "h_3", 1)
        .unwrap();

    let torrents = db.get_torrents_by_source_path_prefix("a_b").unwrap();
    let names: Vec<&str> = torrents.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(torrents.len(), 2);
    assert!(names.contains(&"Literal Underscore"));
    assert!(names.contains(&"Literal Child"));
    assert!(!names.contains(&"Should Not Match 1"));
}

#[test]
fn test_get_source_path_prefixes() {
    let mut db = Database::open_in_memory().unwrap();

    db.insert_torrent("a/b", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
        .unwrap();
    db.insert_torrent("a/c", "Torrent 2", "Torrent 2", 2048, "hash2", 1)
        .unwrap();
    db.insert_torrent("d", "Torrent 3", "Torrent 3", 3072, "hash3", 1)
        .unwrap();

    let prefixes = db.get_source_path_prefixes("").unwrap();
    assert!(prefixes.contains(&"a".to_string()));
    assert!(prefixes.contains(&"d".to_string()));

    let prefixes = db.get_source_path_prefixes("a").unwrap();
    assert!(prefixes.contains(&"b".to_string()));
    assert!(prefixes.contains(&"c".to_string()));
}

#[test]
fn test_metadata_directory_structure_preserved() {
    let mut db = Database::open_in_memory().unwrap();

    db.insert_torrent(
        "anime/naruto/season1",
        "Naruto S1",
        "Naruto S1",
        1024,
        "hash1",
        1,
    )
    .unwrap();
    db.insert_torrent(
        "anime/naruto/season2",
        "Naruto S2",
        "Naruto S2",
        2048,
        "hash2",
        1,
    )
    .unwrap();
    db.insert_torrent("anime/onepiece", "One Piece", "One Piece", 3072, "hash3", 1)
        .unwrap();
    db.insert_torrent(
        "movies/scifi",
        "SciFi Movies",
        "SciFi Movies",
        4096,
        "hash4",
        1,
    )
    .unwrap();

    let root = db.get_source_path_prefixes("").unwrap();
    assert_eq!(root.len(), 2);
    assert!(root.contains(&"anime".to_string()));
    assert!(root.contains(&"movies".to_string()));

    let anime = db.get_source_path_prefixes("anime").unwrap();
    assert_eq!(anime.len(), 2);
    assert!(anime.contains(&"naruto".to_string()));
    assert!(anime.contains(&"onepiece".to_string()));

    let naruto = db.get_source_path_prefixes("anime/naruto").unwrap();
    assert_eq!(naruto.len(), 2);
    assert!(naruto.contains(&"season1".to_string()));
    assert!(naruto.contains(&"season2".to_string()));

    let onepiece = db.get_source_path_prefixes("anime/onepiece").unwrap();
    assert_eq!(onepiece.len(), 0);

    let movies = db.get_source_path_prefixes("movies").unwrap();
    assert_eq!(movies.len(), 1);
    assert!(movies.contains(&"scifi".to_string()));
}

#[test]
fn test_delete_metadata_directory() {
    let mut db = Database::open_in_memory().unwrap();

    // Create a nested directory structure
    db.insert_torrent(
        "anime/naruto/season1",
        "Naruto S1",
        "Naruto S1",
        1024,
        "hash1",
        1,
    )
    .unwrap();

    // Verify the directory structure exists
    let root = db.get_source_path_prefixes("").unwrap();
    assert!(root.contains(&"anime".to_string()));

    let anime = db.get_source_path_prefixes("anime").unwrap();
    assert!(anime.contains(&"naruto".to_string()));

    let naruto = db.get_source_path_prefixes("anime/naruto").unwrap();
    assert!(naruto.contains(&"season1".to_string()));

    // Delete a leaf directory
    db.delete_metadata_directory("anime/naruto/season1")
        .unwrap();

    // Verify the leaf directory is gone
    let naruto = db.get_source_path_prefixes("anime/naruto").unwrap();
    assert!(!naruto.contains(&"season1".to_string()));

    // Delete a parent directory - should cascade delete children
    db.delete_metadata_directory("anime").unwrap();

    // Verify parent and children are gone
    let root = db.get_source_path_prefixes("").unwrap();
    assert!(!root.contains(&"anime".to_string()));

    let anime = db.get_source_path_prefixes("anime").unwrap();
    assert!(anime.is_empty());
}

#[test]
fn test_get_root_files() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 3)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![
        FileEntry {
            path: "file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "file2.txt".to_string(),
            size: 200,
        },
        FileEntry {
            path: "dir/file3.txt".to_string(),
            size: 300,
        },
    ];

    db.insert_files(torrent_id, &files).unwrap();

    let root_files = db.get_root_files(torrent_id).unwrap();
    assert_eq!(root_files.len(), 2);
}

#[test]
fn test_get_torrent_directory() {
    let mut db = Database::open_in_memory().unwrap();

    let torrent_id = match db
        .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let files = vec![FileEntry {
        path: "dir1/file.txt".to_string(),
        size: 100,
    }];

    db.insert_files(torrent_id, &files).unwrap();

    let dir = db.get_torrent_directory(torrent_id, None, "dir1").unwrap();
    assert!(dir.is_some());
    assert_eq!(dir.unwrap().name, "dir1");
}

#[test]
fn test_insert_torrent_with_files_atomic() {
    let mut db = Database::open_in_memory().unwrap();

    let files = vec![
        FileEntry {
            path: "dir1/file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "dir1/file2.txt".to_string(),
            size: 200,
        },
        FileEntry {
            path: "dir2/file3.txt".to_string(),
            size: 300,
        },
    ];

    // Insert torrent with files atomically
    let result = db
        .insert_torrent_with_files(
            "path1",
            "Test Torrent",
            "Test Torrent.torrent",
            600,
            "hash1",
            3,
            &files,
        )
        .unwrap();

    let torrent_id = match result {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify torrent was inserted
    let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
    assert_eq!(torrent.name, "Test Torrent");
    assert_eq!(torrent.filename, "Test Torrent.torrent");
    assert_eq!(torrent.total_size, 600);

    // Verify files were inserted
    let db_files = db.get_files_by_torrent_id(torrent_id).unwrap();
    assert_eq!(db_files.len(), 3);

    // Verify directories were created
    let root_dirs = db
        .get_torrent_directories_by_parent(None, torrent_id)
        .unwrap();
    assert_eq!(root_dirs.len(), 2);
}

/// TSI-3114: `insert_torrent_with_files_and_data` writes `torrent_data` in the
/// SAME transaction as the row + file tree, so a failure cannot leave a
/// `torrent_data = NULL` row behind (the old `set_torrent_data` follow-up ran
/// outside the insert transaction and could be split by a disk-full).
#[test]
fn test_insert_torrent_with_files_and_data_is_atomic() {
    let files = vec![FileEntry {
        path: "a.txt".to_string(),
        size: 10,
    }];

    // Success: torrent_data is present immediately after commit.
    let mut db = Database::open_in_memory().unwrap();
    let result = db
        .insert_torrent_with_files_and_data(
            "path1",
            "Test",
            "Test.torrent",
            10,
            "hash1",
            1,
            &files,
            b"raw-torrent-bytes",
        )
        .unwrap();
    let id = match result {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let torrent = db.get_torrent_by_id(id).unwrap().unwrap();
    assert_eq!(
        torrent.torrent_data.as_deref(),
        Some(&b"raw-torrent-bytes"[..])
    );

    // Failure: a read-only connection makes the transaction fail and roll
    // back — no torrent row (and thus no NULL-torrent_data row) survives.
    let mut db2 = Database::open_in_memory().unwrap();
    db2.conn
        .execute_batch("PRAGMA query_only = ON;")
        .expect("set query_only");
    let result = db2.insert_torrent_with_files_and_data(
        "path2",
        "Test2",
        "Test2.torrent",
        10,
        "hash2",
        1,
        &files,
        b"raw-torrent-bytes",
    );
    assert!(result.is_err(), "read-only insert must fail");
    assert!(
        db2.get_torrents_by_source_path("path2").unwrap().is_empty(),
        "no partial row may survive the failed transaction"
    );
}

#[test]
fn test_insert_torrent_with_files_populates_path() {
    let mut db = Database::open_in_memory().unwrap();

    let files = vec![
        FileEntry {
            path: "dir1/file1.txt".to_string(),
            size: 100,
        },
        FileEntry {
            path: "file2.txt".to_string(),
            size: 200,
        },
        FileEntry {
            path: "a/b/c/deep.txt".to_string(),
            size: 300,
        },
    ];

    let torrent_id = match db
        .insert_torrent_with_files("path1", "Test", "Test.torrent", 600, "hash1", 3, &files)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
    assert_eq!(all_files.len(), 3);

    let file1 = all_files.iter().find(|f| f.name == "file1.txt").unwrap();
    assert_eq!(file1.path, "dir1/file1.txt");

    let file2 = all_files.iter().find(|f| f.name == "file2.txt").unwrap();
    assert_eq!(file2.path, "file2.txt");

    let deep = all_files.iter().find(|f| f.name == "deep.txt").unwrap();
    assert_eq!(deep.path, "a/b/c/deep.txt");

    // Nested file must sit under the `c` directory, not at root (directory_id != None).
    let dir_a = db
        .get_torrent_directory(torrent_id, None, "a")
        .unwrap()
        .expect("dir `a` exists");
    let dir_b = db
        .get_torrent_directory(torrent_id, Some(dir_a.id), "b")
        .unwrap()
        .expect("dir `b` exists");
    let dir_c = db
        .get_torrent_directory(torrent_id, Some(dir_b.id), "c")
        .unwrap()
        .expect("dir `c` exists");
    assert_eq!(deep.directory_id, Some(dir_c.id));
}

#[test]
fn test_insert_torrent_with_files_duplicate() {
    let mut db = Database::open_in_memory().unwrap();

    let files = vec![FileEntry {
        path: "file1.txt".to_string(),
        size: 100,
    }];

    // First insert
    let result = db
        .insert_torrent_with_files(
            "path1",
            "Test Torrent",
            "Test Torrent.torrent",
            100,
            "hash1",
            1,
            &files,
        )
        .unwrap();
    assert!(matches!(result, InsertTorrentResult::Inserted(_)));

    // Same source_path + different filename → Inserted (independent mirror)
    let result = db
        .insert_torrent_with_files(
            "path1",
            "Test Torrent 2",
            "Test Torrent 2.torrent",
            200,
            "hash1",
            1,
            &files,
        )
        .unwrap();
    assert!(matches!(result, InsertTorrentResult::Inserted(_)));

    // Same source_path + same filename → Duplicate
    let result = db
        .insert_torrent_with_files(
            "path1",
            "Test Torrent 2",
            "Test Torrent 2.torrent",
            200,
            "hash1",
            1,
            &files,
        )
        .unwrap();
    assert!(matches!(result, InsertTorrentResult::Duplicate(_)));
}

#[test]
fn test_rename_torrent() {
    let mut db = Database::open_in_memory().unwrap();

    // Insert a torrent
    let torrent_id = match db
        .insert_torrent("path1", "Old Name", "Old Name.torrent", 1024, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify initial state
    let torrent = db
        .get_torrent_by_filename_and_source_path("Old Name.torrent", "path1")
        .unwrap()
        .unwrap();
    assert_eq!(torrent.name, "Old Name");
    assert_eq!(torrent.filename, "Old Name.torrent");

    // Rename the torrent
    db.rename_torrent(torrent_id, "New Name", "New Name.torrent", "path1")
        .unwrap();

    // Verify the rename
    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.name, "New Name");
    assert_eq!(torrent.filename, "New Name.torrent");

    // Old filename lookup should return None
    let old_lookup = db
        .get_torrent_by_filename_and_source_path("Old Name.torrent", "path1")
        .unwrap();
    assert!(old_lookup.is_none());

    // New filename lookup should work
    let new_lookup = db
        .get_torrent_by_filename_and_source_path("New Name.torrent", "path1")
        .unwrap();
    assert!(new_lookup.is_some());
}

#[test]
fn test_rename_torrent_cross_directory() {
    let mut db = Database::open_in_memory().unwrap();

    // Insert a torrent in "path1"
    let torrent_id = match db
        .insert_torrent("path1", "MyTorrent", "MyTorrent.torrent", 1024, "hash1", 1)
        .unwrap()
    {
        InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify initial state
    let torrent = db
        .get_torrent_by_filename_and_source_path("MyTorrent.torrent", "path1")
        .unwrap()
        .unwrap();
    assert_eq!(torrent.source_path, "path1");

    // Rename and move to "path2" (cross-directory rename)
    db.rename_torrent(torrent_id, "Renamed", "Renamed.torrent", "path2")
        .unwrap();

    // Verify the rename and source_path update
    let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
    assert_eq!(torrent.name, "Renamed");
    assert_eq!(torrent.filename, "Renamed.torrent");
    assert_eq!(torrent.source_path, "path2");

    // Old location lookup should return None
    let old_lookup = db
        .get_torrent_by_filename_and_source_path("MyTorrent.torrent", "path1")
        .unwrap();
    assert!(old_lookup.is_none());

    // New location lookup should work
    let new_lookup = db
        .get_torrent_by_filename_and_source_path("Renamed.torrent", "path2")
        .unwrap();
    assert!(new_lookup.is_some());
}

#[test]
fn test_rename_metadata_directory() {
    let mut db = Database::open_in_memory().unwrap();

    // Create metadata directories: "movies" and "movies/action"
    db.ensure_metadata_directories("movies/action").unwrap();

    // Insert a torrent in "movies/action"
    db.insert_torrent(
        "movies/action",
        "DieHard",
        "DieHard.torrent",
        1024,
        "hash1",
        1,
    )
    .unwrap();

    // Rename "movies" -> "films"
    db.rename_metadata_directory("movies", "films", "films")
        .unwrap();

    // Verify directory was renamed
    let dirs = db.get_all_metadata_directories().unwrap();
    let dir_paths: Vec<&str> = dirs.iter().map(|(_, _, _, p)| p.as_str()).collect();
    assert!(dir_paths.contains(&"films"));
    assert!(dir_paths.contains(&"films/action"));
    assert!(!dir_paths.contains(&"movies"));
    assert!(!dir_paths.contains(&"movies/action"));

    // Verify torrent source_path was updated
    let torrent = db.get_torrent_by_id(1).unwrap().unwrap();
    assert_eq!(torrent.source_path, "films/action");
}

#[test]
fn test_rename_metadata_directory_simple() {
    let mut db = Database::open_in_memory().unwrap();

    // Create metadata directory "old_dir"
    db.ensure_metadata_directories("old_dir").unwrap();

    // Insert a torrent in "old_dir"
    db.insert_torrent("old_dir", "MyTorrent", "MyTorrent.torrent", 512, "hash2", 1)
        .unwrap();

    // Rename "old_dir" -> "new_dir"
    db.rename_metadata_directory("old_dir", "new_dir", "new_dir")
        .unwrap();

    // Verify directory renamed
    let dirs = db.get_all_metadata_directories().unwrap();
    let dir_paths: Vec<&str> = dirs.iter().map(|(_, _, _, p)| p.as_str()).collect();
    assert!(dir_paths.contains(&"new_dir"));
    assert!(!dir_paths.contains(&"old_dir"));

    // Verify torrent source_path updated
    let torrent = db.get_torrent_by_id(1).unwrap().unwrap();
    assert_eq!(torrent.source_path, "new_dir");
}

#[test]
fn test_rename_metadata_directory_persists() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Create, insert, rename, and close
    {
        let mut db = Database::open(&db_path).unwrap();
        db.ensure_metadata_directories("alpha").unwrap();
        db.insert_torrent("alpha", "Test", "Test.torrent", 100, "h1", 1)
            .unwrap();
        db.rename_metadata_directory("alpha", "beta", "beta")
            .unwrap();
    }

    // Reopen and verify persistence
    {
        let db = Database::open(&db_path).unwrap();
        let dirs = db.get_all_metadata_directories().unwrap();
        let dir_paths: Vec<&str> = dirs.iter().map(|(_, _, _, p)| p.as_str()).collect();
        assert!(dir_paths.contains(&"beta"));
        assert!(!dir_paths.contains(&"alpha"));

        let torrent = db.get_torrent_by_id(1).unwrap().unwrap();
        assert_eq!(torrent.source_path, "beta");
    }
}

#[test]
fn test_rename_metadata_directory_nested_torrents() {
    let mut db = Database::open_in_memory().unwrap();

    // Create "a/b" and "a/c"
    db.ensure_metadata_directories("a/b").unwrap();
    db.ensure_metadata_directories("a/c").unwrap();

    // Insert torrents in different subdirs
    db.insert_torrent("a/b", "T1", "T1.torrent", 100, "h1", 1)
        .unwrap();
    db.insert_torrent("a/c", "T2", "T2.torrent", 200, "h2", 1)
        .unwrap();

    // Rename "a" -> "z"
    db.rename_metadata_directory("a", "z", "z").unwrap();

    // Both torrents should have updated source_paths
    let t1 = db.get_torrent_by_id(1).unwrap().unwrap();
    assert_eq!(t1.source_path, "z/b");
    let t2 = db.get_torrent_by_id(2).unwrap().unwrap();
    assert_eq!(t2.source_path, "z/c");
}

#[test]
fn test_rename_metadata_directory_underscore_in_name() {
    let mut db = Database::open_in_memory().unwrap();

    // Create "a_b/c" and "aXb/d" — the underscore in "a_b" must be literal,
    // not a LIKE wildcard matching "aXb".
    db.ensure_metadata_directories("a_b/c").unwrap();
    db.ensure_metadata_directories("aXb/d").unwrap();
    db.insert_torrent("a_b/c", "T1", "T1.torrent", 100, "h1", 1)
        .unwrap();
    db.insert_torrent("aXb/d", "T2", "T2.torrent", 200, "h2", 1)
        .unwrap();

    // Rename "a_b" -> "a_b_new" must NOT touch "aXb/d".
    db.rename_metadata_directory("a_b", "a_b_new", "a_b_new")
        .unwrap();

    let t1 = db.get_torrent_by_id(1).unwrap().unwrap();
    assert_eq!(t1.source_path, "a_b_new/c");
    let t2 = db.get_torrent_by_id(2).unwrap().unwrap();
    assert_eq!(t2.source_path, "aXb/d");
}

#[test]
fn test_migrate_v5_preserves_child_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create a v4 database with pre-existing data (torrent + files + directories)
    let torrent_id = {
        let mut db = Database::open(&db_path).unwrap();

        let files = vec![
            FileEntry {
                path: "dir1/file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "file2.txt".to_string(),
                size: 200,
            },
        ];

        let result = db
            .insert_torrent_with_files("music", "Album", "Album.torrent", 300, "abc123", 2, &files)
            .unwrap();
        match result {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        }
    };

    // Re-open: triggers all migrations including v5
    {
        let db = Database::open(&db_path).unwrap();

        // Verify torrent survived migration
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.name, "Album");
        assert_eq!(torrent.source_path, "music");

        // Verify child data preserved (was NOT cascade-deleted by DROP TABLE)
        let files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert_eq!(files.len(), 2, "torrent_files should survive v5 migration");

        let dirs = db
            .get_torrent_directories_by_parent(None, torrent_id)
            .unwrap();
        assert_eq!(
            dirs.len(),
            1,
            "torrent_directories should survive v5 migration"
        );
    }
}

#[test]
fn test_migrate_v5_dedup_conflicting_rows() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Build a v4 database manually so the new UNIQUE(source_path, filename)
    // is NOT yet active. Then insert two rows with same (source_path, filename)
    // but different info_hash — legal under old UNIQUE(info_hash, source_path).
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        Database::migrate_v1(&conn).unwrap();
        // v1 already includes v2 columns (file_count, status, etc.)
        Database::migrate_v3(&conn).unwrap();
        Database::migrate_v4(&conn).unwrap();
        conn.pragma_update(None, "user_version", 4).unwrap();

        conn.execute(
                    "INSERT INTO torrents (info_hash, name, total_size, file_count, status, source_path, filename)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params!["hash1", "Torrent Old", 100i64, 1i64, "pending", "a", "T.torrent"],
                ).unwrap();

        conn.execute(
                    "INSERT INTO torrents (info_hash, name, total_size, file_count, status, source_path, filename)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params!["hash2", "Torrent New", 200i64, 1i64, "pending", "a", "T.torrent"],
                ).unwrap();
    }

    // Re-open via Database::open — triggers v5 migration, must dedup and succeed
    {
        let db = Database::open(&db_path).unwrap();

        let torrents = db.get_torrents_by_source_path("a").unwrap();
        assert_eq!(
            torrents.len(),
            1,
            "dedup should keep one row per (source_path, filename)"
        );
        assert_eq!(
            torrents[0].info_hash, "hash2",
            "should keep the later row (MAX id)"
        );
        assert_eq!(torrents[0].name, "Torrent New");
    }
}

#[test]
fn test_migrate_v5_cleans_orphaned_child_rows() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Build a v4 database manually with two rows sharing (source_path, filename)
    // but different info_hash (legal under old UNIQUE(info_hash, source_path)).
    // Attach child rows to BOTH torrents so the non-retained one becomes an orphan.
    let orphan_id;
    let survivor_id;
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        Database::migrate_v1(&conn).unwrap();
        Database::migrate_v3(&conn).unwrap();
        Database::migrate_v4(&conn).unwrap();
        conn.pragma_update(None, "user_version", 4).unwrap();

        conn.execute(
            "INSERT INTO torrents (info_hash, name, total_size, file_count, status, source_path, filename)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params!["hash1", "Old", 100i64, 1i64, "pending", "a", "T.torrent"],
        ).unwrap();
        orphan_id = conn.last_insert_rowid();

        conn.execute(
            "INSERT INTO torrents (info_hash, name, total_size, file_count, status, source_path, filename)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params!["hash2", "New", 200i64, 1i64, "pending", "a", "T.torrent"],
        ).unwrap();
        survivor_id = conn.last_insert_rowid();

        // Child rows for the orphaned torrent
        conn.execute(
            "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?1, NULL, 'dir_old')",
            rusqlite::params![orphan_id],
        ).unwrap();
        let old_dir_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO torrent_files (torrent_id, directory_id, name, size) VALUES (?1, ?2, 'f_old', 10)",
            rusqlite::params![orphan_id, old_dir_id],
        ).unwrap();
        conn.execute(
            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?1, ?1, 0)",
            rusqlite::params![old_dir_id],
        )
        .unwrap();

        // Child rows for the surviving torrent
        conn.execute(
            "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?1, NULL, 'dir_new')",
            rusqlite::params![survivor_id],
        ).unwrap();
        let new_dir_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO torrent_files (torrent_id, directory_id, name, size) VALUES (?1, ?2, 'f_new', 20)",
            rusqlite::params![survivor_id, new_dir_id],
        ).unwrap();
        conn.execute(
            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?1, ?1, 0)",
            rusqlite::params![new_dir_id],
        )
        .unwrap();
    }

    // Re-open: v5 migration must delete the orphan's child rows.
    {
        let db = Database::open(&db_path).unwrap();

        // Survivor's child rows intact
        let files = db.get_files_by_torrent_id(survivor_id).unwrap();
        assert_eq!(files.len(), 1, "survivor files preserved");
        assert_eq!(files[0].name, "f_new");

        let dirs = db
            .get_torrent_directories_by_parent(None, survivor_id)
            .unwrap();
        assert_eq!(dirs.len(), 1, "survivor directories preserved");

        // Orphan's child rows gone (no dangling torrent_id references)
        let orphan_files = db.get_files_by_torrent_id(orphan_id).unwrap();
        assert_eq!(orphan_files.len(), 0, "orphan files cleaned up");

        let orphan_dirs = db
            .get_torrent_directories_by_parent(None, orphan_id)
            .unwrap();
        assert_eq!(orphan_dirs.len(), 0, "orphan directories cleaned up");

        // directory_closure: no rows referencing the orphan's directory id
        let conn = &db.conn;
        let dangling: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM directory_closure WHERE ancestor_id IN (
                    SELECT id FROM torrent_directories WHERE torrent_id = ?1
                ) OR descendant_id IN (
                    SELECT id FROM torrent_directories WHERE torrent_id = ?1
                )",
                rusqlite::params![orphan_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dangling, 0, "orphan directory_closure rows cleaned up");
    }
}

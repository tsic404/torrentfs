use tempfile::TempDir;
use torrentfs::db::Database;

/// Test that renaming a torrent file in metadata/ updates the database correctly
/// The key behavior: torrent.name (internal name) is preserved, only filename changes
#[test]
fn test_rename_updates_database() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let mut db = Database::open(&db_path).expect("Failed to open database");

    // Insert a torrent with internal name "ubuntu-25.10-desktop-amd64.iso"
    // and filename "ubuntu-25.10-desktop-amd64.iso.torrent"
    let torrent_id = match db
        .insert_torrent(
            "",
            "ubuntu-25.10-desktop-amd64.iso",
            "ubuntu-25.10-desktop-amd64.iso.torrent",
            1024,
            "hash123",
            1,
        )
        .expect("Failed to insert torrent")
    {
        torrentfs::db::InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify initial state
    let torrent = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10-desktop-amd64.iso.torrent", "")
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-25.10-desktop-amd64.iso");
    assert_eq!(torrent.filename, "ubuntu-25.10-desktop-amd64.iso.torrent");

    // Rename the torrent file to "ubuntu-25.10.torrent"
    // The internal name should stay the same, only filename changes
    db.rename_torrent(torrent_id, "ubuntu-25.10.torrent", "")
        .expect("Failed to rename torrent");

    // Verify the rename - internal name preserved, filename updated
    let torrent = db
        .get_torrent_by_id(torrent_id)
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-25.10-desktop-amd64.iso"); // Internal name preserved
    assert_eq!(torrent.filename, "ubuntu-25.10.torrent"); // Filename updated

    // Old filename lookup should return None
    let old_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10-desktop-amd64.iso.torrent", "")
        .expect("Failed to lookup");
    assert!(old_lookup.is_none());

    // New filename lookup should work
    let new_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10.torrent", "")
        .expect("Failed to lookup");
    assert!(new_lookup.is_some());
}

/// Test that renaming a torrent file with source_path updates correctly
/// The internal name is preserved, only filename and source_path change
#[test]
fn test_rename_with_source_path() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let mut db = Database::open(&db_path).expect("Failed to open database");

    // Insert a torrent with source_path
    let torrent_id = match db
        .insert_torrent(
            "os/linux",
            "ubuntu-25.10",
            "ubuntu-25.10.torrent",
            1024,
            "hash456",
            1,
        )
        .expect("Failed to insert torrent")
    {
        torrentfs::db::InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify initial state
    let torrent = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10.torrent", "os/linux")
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-25.10");
    assert_eq!(torrent.filename, "ubuntu-25.10.torrent");

    // Rename the torrent - internal name preserved, filename and source_path updated
    db.rename_torrent(torrent_id, "ubuntu-26.04.torrent", "os/linux")
        .expect("Failed to rename torrent");

    // Verify the rename - internal name preserved
    let torrent = db
        .get_torrent_by_id(torrent_id)
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-25.10"); // Internal name preserved
    assert_eq!(torrent.filename, "ubuntu-26.04.torrent"); // Filename updated

    // Old filename lookup should return None
    let old_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10.torrent", "os/linux")
        .expect("Failed to lookup");
    assert!(old_lookup.is_none());

    // New filename lookup should work
    let new_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-26.04.torrent", "os/linux")
        .expect("Failed to lookup");
    assert!(new_lookup.is_some());
}

/// Test that renaming persists across database reopen
#[test]
fn test_rename_persists() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Insert and rename
    {
        let mut db = Database::open(&db_path).expect("Failed to open database");

        let torrent_id = match db
            .insert_torrent("", "old-name", "old-name.torrent", 1024, "hash789", 1)
            .expect("Failed to insert torrent")
        {
            torrentfs::db::InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        // Rename - internal name preserved
        db.rename_torrent(torrent_id, "new-name.torrent", "")
            .expect("Failed to rename torrent");
    }

    // Reopen and verify
    {
        let db = Database::open(&db_path).expect("Failed to open database");

        let torrent = db
            .get_torrent_by_filename_and_source_path("new-name.torrent", "")
            .expect("Failed to get torrent")
            .expect("Torrent not found");
        assert_eq!(torrent.name, "old-name"); // Internal name preserved
        assert_eq!(torrent.filename, "new-name.torrent"); // Filename updated

        let old_lookup = db
            .get_torrent_by_filename_and_source_path("old-name.torrent", "")
            .expect("Failed to lookup");
        assert!(old_lookup.is_none());
    }
}

/// Test that hash-named files maintain their filename across database operations
/// This is the core bug fix: hash-named files should not be renamed to internal name
#[test]
fn test_hash_filename_preserved() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    {
        let mut db = Database::open(&db_path).expect("Failed to open database");

        // Insert a torrent with hash filename but different internal name
        // This simulates the bug scenario: hash filename "3a4816f6...torrent"
        // but internal name "How.NOT.to.Summon.a.Demon.Lord.S02"
        let torrent_id = match db
            .insert_torrent(
                "",
                "How.NOT.to.Summon.a.Demon.Lord.S02", // Internal name
                "3a4816f6.torrent",                   // Hash filename
                1024,
                "hash999",
                1,
            )
            .expect("Failed to insert torrent")
        {
            torrentfs::db::InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        // Verify initial state
        let torrent = db
            .get_torrent_by_id(torrent_id)
            .expect("Failed to get torrent")
            .expect("Torrent not found");
        assert_eq!(torrent.name, "How.NOT.to.Summon.a.Demon.Lord.S02"); // Internal name
        assert_eq!(torrent.filename, "3a4816f6.torrent"); // Hash filename

        // Rename to another hash filename
        db.rename_torrent(torrent_id, "newhash123.torrent", "")
            .expect("Failed to rename torrent");

        // Verify internal name is preserved
        let torrent = db
            .get_torrent_by_id(torrent_id)
            .expect("Failed to get torrent")
            .expect("Torrent not found");
        assert_eq!(torrent.name, "How.NOT.to.Summon.a.Demon.Lord.S02"); // Still preserved
        assert_eq!(torrent.filename, "newhash123.torrent"); // New filename
    }

    // Reopen and verify the hash filename is still preserved
    {
        let db = Database::open(&db_path).expect("Failed to open database");

        let torrent = db
            .get_torrent_by_filename_and_source_path("newhash123.torrent", "")
            .expect("Failed to get torrent")
            .expect("Torrent not found");
        // Internal name should still be the original, not the hash filename
        assert_eq!(torrent.name, "How.NOT.to.Summon.a.Demon.Lord.S02");
        assert_eq!(torrent.filename, "newhash123.torrent");
    }
}

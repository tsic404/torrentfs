//! Multi-file (BEP-3 `files` list) torrent construction shared by the QA
//! seeder examples: `torrentfs-mffs-seeder` (`ci/mffs_seeder.rs`) seeds a
//! multi-file payload on its own, and `torrentfs-selfseed-env`
//! (`ci/selfseed_env.rs`) seeds one alongside its single-file payload so the
//! self-seed QA environment can serve both layouts from one swarm.  The
//! tracker, bencoding helpers, and keep-alive loop live in [`seeder_common`]
//! (`ci/seeder_common.rs`); this module holds only the multi-file half that
//! both examples would otherwise duplicate.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::seeder_common::{bencode_bytes, bencode_int, PIECE_LEN};

/// Recursively collect every regular file under `root`, returning
/// `(relative path, size)` sorted by relative path so both the piece hashing
/// and the bencoded `files` list observe the same deterministic order.
pub fn collect_files(root: &Path) -> Vec<(PathBuf, u64)> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .expect("failed to read payload dir")
            .filter_map(|e| e.ok())
            .collect();
        // Deterministic traversal order before the final sort (read_dir order
        // is filesystem-dependent).
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            // `file_type()` does not follow symlinks: a circular symlink
            // directory would otherwise push the same tree onto the stack
            // forever, and a symlinked file must not leak outside the payload.
            let file_type = entry.file_type().expect("failed to stat payload entry");
            if file_type.is_symlink() {
                continue;
            }
            let path = entry.path();
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                let meta = entry.metadata().expect("failed to stat payload entry");
                files.push((path.strip_prefix(root).unwrap().to_path_buf(), meta.len()));
            }
        }
    }
    files.sort();
    files
}

/// Split a relative path into its byte-string components for the bencoded
/// `path` list (BEP-3 multi-file layout).  Unix `OsStr` bytes are used
/// verbatim so non-UTF-8 names round-trip.
fn path_components(rel: &Path) -> Vec<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt;
    rel.components()
        .map(|c| c.as_os_str().as_bytes().to_vec())
        .collect()
}

/// Stream every file under `root` into the seed tree while hashing the
/// concatenated byte stream piece-by-piece (pieces span file boundaries).
///
/// Returns the concatenated SHA-1 digests (one 20-byte digest per piece, in
/// order) and the total payload length.  Files are written to
/// `seed_root/<name>/<relative path>`, matching libtorrent's multi-file
/// save-path layout, and memory stays bounded by one piece buffer plus the
/// digest list.
pub fn hash_and_seed_files(
    root: &Path,
    files: &[(PathBuf, u64)],
    seed_root: &Path,
    name: &str,
) -> (Vec<u8>, u64) {
    use sha1_smol::Sha1;

    let mut pieces = Vec::new();
    let mut pending: Vec<u8> = Vec::with_capacity(PIECE_LEN);
    let mut total: u64 = 0;

    for (rel, _size) in files {
        let src = root.join(rel);
        let dst = seed_root.join(name).join(rel);
        std::fs::create_dir_all(dst.parent().expect("file has a parent")).expect("mkdir seed dir");
        let mut input = std::fs::File::open(&src).expect("failed to read payload file");
        let mut output = std::fs::File::create(&dst).expect("failed to write seed file");

        let mut buf = vec![0u8; PIECE_LEN];
        loop {
            let n = match input.read(&mut buf) {
                Ok(0) => break, // EOF for this file
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("failed to read {}: {e}", src.display()),
            };
            total += n as u64;
            output
                .write_all(&buf[..n])
                .expect("failed to write seed file");
            pending.extend_from_slice(&buf[..n]);
            // Drain every complete piece; the remainder carries across the
            // file boundary so pieces spanning two files hash correctly.
            while pending.len() >= PIECE_LEN {
                let mut h = Sha1::new();
                h.update(&pending[..PIECE_LEN]);
                pieces.extend_from_slice(&h.digest().bytes());
                pending.drain(..PIECE_LEN);
            }
        }
    }

    // Trailing partial piece (a multi-file torrent's last piece is usually
    // shorter than PIECE_LEN).
    if !pending.is_empty() {
        let mut h = Sha1::new();
        h.update(&pending);
        pieces.extend_from_slice(&h.digest().bytes());
    }
    (pieces, total)
}

/// Bencode a multi-file torrent: top-level `announce` + `info` dict with a
/// `files` list, `name`, `piece length`, and `pieces`.
pub fn bencode_multifile_torrent(
    announce_url: &str,
    name: &str,
    files: &[(PathBuf, u64)],
    pieces: &[u8],
) -> Vec<u8> {
    let mut d = vec![b'd'];
    d.extend_from_slice(b"8:announce");
    d.extend_from_slice(&bencode_bytes(announce_url.as_bytes()));
    d.extend_from_slice(b"4:infod");
    d.extend_from_slice(b"5:filesl");
    for (rel, size) in files {
        d.push(b'd');
        d.extend_from_slice(b"6:length");
        d.extend_from_slice(&bencode_int(*size as i64));
        d.extend_from_slice(b"4:pathl");
        for comp in path_components(rel) {
            d.extend_from_slice(&bencode_bytes(&comp));
        }
        d.push(b'e'); // close the path list
        d.push(b'e'); // close the file dict
    }
    d.push(b'e'); // close the files list
    d.extend_from_slice(b"4:name");
    d.extend_from_slice(&bencode_bytes(name.as_bytes()));
    d.extend_from_slice(b"12:piece length");
    d.extend_from_slice(&bencode_int(PIECE_LEN as i64));
    d.extend_from_slice(b"6:pieces");
    d.extend_from_slice(&bencode_bytes(pieces));
    d.extend_from_slice(b"ee"); // close info + top-level dicts
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Removes a temp directory on drop so a failing assertion does not leak
    /// it in `/tmp`.
    struct TempDirGuard(std::path::PathBuf);
    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_file(dir: &Path, rel: &str, data: &[u8]) {
        let path = dir.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }

    /// `hash_and_seed_files` must hash the concatenated file stream (pieces
    /// spanning file boundaries) and place each file at
    /// `seed_root/<name>/<relative path>`.  Files are deliberately sized so a
    /// piece boundary falls inside the middle file.
    #[test]
    fn hash_and_seed_files_streams_across_boundaries() {
        let dir = std::env::temp_dir().join(format!("mffs-seeder-hash-{}", std::process::id()));
        let _guard = TempDirGuard(dir.clone());
        let payload_dir = dir.join("payload");
        let seed_dir = dir.join("seed_data");
        std::fs::create_dir_all(&payload_dir).unwrap();

        // a.txt: half a piece, b.bin: one full piece + a tail, dir/c.txt: a
        // trailing partial piece — so the concatenated stream spans boundaries.
        let a = vec![b'a'; PIECE_LEN / 2];
        let b = vec![b'b'; PIECE_LEN + 123];
        let c = vec![b'c'; 45];
        write_file(&payload_dir, "a.txt", &a);
        write_file(&payload_dir, "b.bin", &b);
        write_file(&payload_dir, "dir/c.txt", &c);

        let files = collect_files(&payload_dir);
        // Sorted: a.txt, b.bin, dir/c.txt.
        assert_eq!(files.len(), 3);

        let (pieces, total) = hash_and_seed_files(&payload_dir, &files, &seed_dir, "mffs");

        // Reference: concatenate in the same order and chunk by PIECE_LEN.
        let mut concat = Vec::new();
        concat.extend_from_slice(&a);
        concat.extend_from_slice(&b);
        concat.extend_from_slice(&c);
        assert_eq!(total, concat.len() as u64);
        let expected: Vec<u8> = concat
            .chunks(PIECE_LEN)
            .flat_map(|chunk| {
                let mut h = sha1_smol::Sha1::new();
                h.update(chunk);
                h.digest().bytes()
            })
            .collect();
        assert_eq!(pieces, expected);

        // Seed files land under seed_data/mffs/<relative path>.
        assert_eq!(std::fs::read(seed_dir.join("mffs/a.txt")).unwrap(), a);
        assert_eq!(std::fs::read(seed_dir.join("mffs/b.bin")).unwrap(), b);
        assert_eq!(std::fs::read(seed_dir.join("mffs/dir/c.txt")).unwrap(), c);
    }

    /// The bencoded multi-file torrent must parse through `TorrentInfo` with
    /// the expected name, file count, total size, and file paths (libtorrent
    /// prepends the torrent name to each multi-file path).
    #[test]
    fn bencoded_multifile_torrent_parses() {
        let dir = std::env::temp_dir().join(format!("mffs-seeder-bencode-{}", std::process::id()));
        let _guard = TempDirGuard(dir.clone());
        let payload_dir = dir.join("payload");
        std::fs::create_dir_all(&payload_dir).unwrap();

        let a = vec![1u8; 100];
        let b = vec![2u8; 200];
        write_file(&payload_dir, "readme.md", &a);
        write_file(&payload_dir, "sub/data.bin", &b);

        let files = collect_files(&payload_dir);
        let (pieces, total) = hash_and_seed_files(&payload_dir, &files, &dir.join("seed"), "mffs");
        let dict =
            bencode_multifile_torrent("http://example.com/announce", "mffs", &files, &pieces);

        let info = torrentfs::TorrentInfo::from_bytes(dict).expect("parse generated torrent");
        assert_eq!(info.name(), "mffs");
        assert_eq!(info.num_files(), 2);
        assert_eq!(info.total_size(), total);

        let mut got: Vec<(String, u64)> = info
            .files()
            .expect("files")
            .into_iter()
            .map(|f| (f.path, f.size))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("mffs/readme.md".to_string(), 100),
                ("mffs/sub/data.bin".to_string(), 200),
            ]
        );
    }

    /// `collect_files` must skip symlinks: a symlink to a directory (including
    /// a self-referential loop) would otherwise recurse forever, and a
    /// symlinked file must not leak content from outside the payload.
    #[test]
    fn collect_files_skips_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = std::env::temp_dir().join(format!("mffs-seeder-symlink-{}", std::process::id()));
        let _guard = TempDirGuard(dir.clone());
        let payload_dir = dir.join("payload");
        std::fs::create_dir_all(&payload_dir).unwrap();

        let real = vec![7u8; 16];
        write_file(&payload_dir, "real.txt", &real);

        // A self-referential directory symlink: without the symlink skip,
        // collect_files would push the same tree forever.
        symlink(&payload_dir, payload_dir.join("loop")).unwrap();
        // A file symlink pointing outside the payload.
        symlink(dir.join("outside.txt"), payload_dir.join("outside-link")).unwrap();
        std::fs::write(dir.join("outside.txt"), b"outside").unwrap();

        let files = collect_files(&payload_dir);
        assert_eq!(files.len(), 1, "symlinks must be excluded");
        assert_eq!(files[0].0, PathBuf::from("real.txt"));
    }
}

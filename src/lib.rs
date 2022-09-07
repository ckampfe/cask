#![forbid(unsafe_code)]

//! TODO
//! - bincode config nonsense: is this fixed in bincode 2.0?
//! - crc verify on initial entries load
//! - figure out what to do with these "file already exists" timing errors
//! - pluggable trait-based value compression (lz4, flate2, zstd)
//! - property tests
//! - use xxhash instead of crc32?
//! - benchmarks?

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};
use thiserror::Error;

/// 256 MiB
const DEFAULT_MAX_FILE_LEN_TARGET_BYTES: usize = 2usize.pow(28);

const HEADER_LENGTH: usize = std::mem::size_of::<Crc>()
    + std::mem::size_of::<TimestampMillis>()
    + std::mem::size_of::<BytesLen>() // key length
    + std::mem::size_of::<BytesLen>(); // value length

/// u32
const CRC_BYTES_RANGE: Range<usize> = 0..4;
/// u128 millis since epoch
const TIMESTAMP_BYTES_RANGE: Range<usize> = 4..20;
/// u64
const KEY_LEN_BYTES_RANGE: Range<usize> = 20..28;
/// u64
const VALUE_LEN_BYTES_RANGE: Range<usize> = 28..HEADER_LENGTH;

type EntryRefs<K> = HashMap<K, EntryRef>;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
struct LogFileId(u128);

type Crc = u32;

#[derive(Debug, PartialEq, PartialOrd)]
struct TimestampMillis(u128);

struct BytesLen(u64);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("serde")]
    Serde(#[from] bincode::Error),
    #[error("io")]
    Io(#[from] std::io::Error),
    #[error(
        "CRC32 for data at offset `{entry_offset}` in file `{file_id}` did not match expected"
    )]
    Corrupt { file_id: u128, entry_offset: u64 },
}

/// Distinguishes between a value and deletion on disk
#[derive(Serialize, Deserialize)]
enum ValueOrDeletion<V> {
    /// A actual value of type `V`
    Value(V),
    /// A tombstone, meaning that that the associated key has been deleted
    /// and no longer has a value
    Tombstone,
}

#[derive(Clone, Debug)]
pub struct Options {
    /// Where log files will live.
    /// Defaults to the current directory.
    pub data_directory: String,
    /// How to sync/persist writes to disk.
    /// Defaults to `SyncStrategy::EveryWrite`.
    pub sync_strategy: SyncStrategy,
    /// Whether or not to sync writes to disk when `Cask` is dropped.
    /// Defaults to true.
    pub sync_on_drop: bool,
    /// Whether to verify the CRC32 of an entry when it is read,
    /// in order to detect data corruption.
    /// Defaults to true.
    pub verify_crc_on_read: bool,
    /// The target maximum file size for log files, in bytes.
    /// A new log file will be created and swapped in when the current log file exceeds this size.
    pub max_file_len_target_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            data_directory: ".".to_string(),
            sync_strategy: SyncStrategy::default(),
            sync_on_drop: true,
            verify_crc_on_read: true,
            max_file_len_target_bytes: DEFAULT_MAX_FILE_LEN_TARGET_BYTES,
        }
    }
}

/// `SyncStrategy` determines when to [fsync](https://linux.die.net/man/2/fsync)
/// (or platform equivalent, via Rust's [File::sync_all])
/// writes to disk. As writes are buffered internally for performance,
/// the choice of `SyncStrategy` has important implications for both
/// performance and safety. For example, if `write` is called but `sync`
/// has not yet been called and the device loses power (or similar),
/// it is possible for that write to not have been committed to disk.
/// The default `SyncStrategy` is `EveryWrite`, which calls `sync`
/// after every single write. This is the safest option, but it leaves
/// quite a bit of throughput on the table, so it is possible to pick
/// more relaxed strategies with higher write throughput if you are able
/// to tolerate some data loss.
#[derive(Clone, Debug, Default)]
pub enum SyncStrategy {
    /// Data is synced to disk after every write.
    /// This is the default, as it is the safest,
    /// but it is also very expensive.
    /// It has higher system call overhead compared to
    /// buffering and flushing multiple writes together.
    #[default]
    EveryWrite,
    /// During sustained writes, data is synced to disk at most every `Duration`.
    ///
    /// *WARNING*: with this strategy, syncing only happens if a
    /// caller calls `write` within an interval!
    /// `cask` has no runtime and as such there is no internal timer,
    /// so data can remain unsynced if there is no `write` in a given interval!
    Interval(Duration),
    /// Syncing writes to disk happens at the discretion of the OS,
    /// or when `self.sync()` is explicitly called
    Manual,
}

pub struct Cask<
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
> {
    /// Log file we are currently writing to
    current_log_file: BufWriter<File>,
    /// Id of the log file we are currently writing to
    current_log_file_id: LogFileId,
    /// Size of the current log file, in bytes
    current_log_file_size_bytes: usize,
    /// Map of Key -> EntryRef, so we can retrieve values for a given key
    entry_refs: EntryRefs<K>,
    /// Current byte offset into the current log file
    offset_bytes: u64,
    /// Options for configuration
    options: Options,
    /// bincode configuration because bincode has a weird API
    /// where the free functions have different configuration
    /// than the global configuration, for some reason?
    /// This allows us to serialize enum variants (`ValueOrDeletion`)
    /// with only a single byte for the tag,
    /// rather than a 4-byte u32 for the tag.
    bincode_options: bincode::DefaultOptions,
    /// When writes were last synced to the current log file
    last_sync: std::time::Instant,
    /// Zero-sized placeholder for V, because V only matters for
    /// insertion and retrieval and is not present on this type itself
    _v: PhantomData<V>,
}

/// Information about where to find an entry in the log files
#[derive(Debug)]
struct EntryRef {
    /// File containing this entry
    file_id: LogFileId,
    /// Size of the entry on disk, in bytes
    entry_len: u64,
    /// Offset in the file where this entry is located, in bytes
    entry_offset: u64,
    /// Millis since epoch
    timestamp: TimestampMillis,
}

impl EntryRef {
    fn entry_log_file(&self, data_directory: &str) -> std::io::Result<File> {
        let file_id = self.file_id;
        let mut path = PathBuf::new();
        path.push(data_directory);
        path.push(file_id.0.to_string());
        path.set_extension("log");
        std::fs::File::open(path)
    }
}

impl<K, V> Cask<K, V>
where
    K: Eq + Hash + serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    /// open a bitcask store
    pub fn open(options: Options) -> Result<Self> {
        std::fs::create_dir_all(&options.data_directory)?;

        let entry_refs = load_entries(&options.data_directory)?;

        let (current_log_file_id, current_log_file) = create_log_file(&options.data_directory)?;

        Ok(Cask {
            current_log_file,
            current_log_file_id,
            current_log_file_size_bytes: 0,
            entry_refs,
            offset_bytes: 0,
            last_sync: std::time::Instant::now(),
            options,
            bincode_options: bincode::DefaultOptions::new(),
            _v: PhantomData,
        })
    }

    /// write a value to a key
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.write(key, ValueOrDeletion::Value(value))
    }

    /// read a key's value
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some(entry_ref) = self.entry_refs.get(key) {
            //
            // open file
            //
            let mut log_file_for_entry = entry_ref.entry_log_file(&self.options.data_directory)?;

            //
            // read entry at position
            //
            log_file_for_entry.seek(SeekFrom::Start(entry_ref.entry_offset))?;
            let mut buf = vec![0; entry_ref.entry_len as usize];
            log_file_for_entry.read_exact(&mut buf)?;

            //
            // get header fields
            //
            let expected_crc = Crc::from_le_bytes(buf[CRC_BYTES_RANGE].try_into().unwrap());
            let timestamp_bytes = &buf[TIMESTAMP_BYTES_RANGE];
            let key_len_bytes = &buf[KEY_LEN_BYTES_RANGE];
            let value_len_bytes = &buf[VALUE_LEN_BYTES_RANGE];

            //
            // get kv
            //
            let key_len = BytesLen(u64::from_le_bytes(key_len_bytes.try_into().unwrap()));
            let value_len = BytesLen(u64::from_le_bytes(value_len_bytes.try_into().unwrap()));
            let key_bytes = &buf[HEADER_LENGTH..(HEADER_LENGTH + key_len.0 as usize)];
            let value_bytes = &buf[(HEADER_LENGTH + key_len.0 as usize)
                ..(HEADER_LENGTH + key_len.0 as usize + value_len.0 as usize)];

            //
            // conditionally verify the read crc against the expected crc
            //
            if self.options.verify_crc_on_read {
                let calculated_crc = {
                    let mut crc_hasher = crc32fast::Hasher::new();
                    crc_hasher.update(timestamp_bytes);
                    crc_hasher.update(key_len_bytes);
                    crc_hasher.update(value_len_bytes);
                    crc_hasher.update(key_bytes);
                    crc_hasher.update(value_bytes);
                    crc_hasher.finalize()
                };

                if calculated_crc != expected_crc {
                    Err(Error::Corrupt {
                        file_id: entry_ref.file_id.0,
                        entry_offset: entry_ref.entry_offset,
                    })?
                }
            }

            //
            // deserialize the value
            //
            let value: ValueOrDeletion<V> =
                bincode::Options::deserialize(self.bincode_options, value_bytes)?;

            //
            // if it's a value, return it, otherwise None
            //
            match value {
                ValueOrDeletion::Value(value) => Ok(Some(value)),
                ValueOrDeletion::Tombstone => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: K) -> Result<()> {
        self.write(key, ValueOrDeletion::Tombstone)
    }

    /// get all keys
    pub fn keys(&self) -> Result<Vec<&K>> {
        Ok(self.entry_refs.keys().collect())
    }

    /// merge logfiles to their most compact representation
    /// invariant: user-visible database should be the same after merging as
    /// it was before merging
    pub fn merge(&mut self) -> Result<()> {
        //
        // sync any pending writes to the existing current_log_file.
        //
        self.sync()?;

        //
        // find those files that we will delete
        //
        let log_files_to_delete: Vec<Result<(LogFileId, PathBuf)>> =
            all_log_files(&self.options.data_directory)?.collect();

        //
        // create a new log file
        //
        self.rotate_log_file()?;

        //
        // copy the entries for all keys to the new log file,
        // creating more log files if they spill over the size limit
        //
        self.copy_all_entries_to_current_log_file()?;

        //
        // remove the leftover log files
        //
        for log_file_result in log_files_to_delete {
            let (_, log_file) = log_file_result?;
            std::fs::remove_file(log_file)?;
        }

        Ok(())
    }

    // the implementation of actually writing an entry to disk
    fn write(&mut self, key: K, value: ValueOrDeletion<V>) -> Result<()> {
        let key_bytes = bincode::Options::serialize(self.bincode_options, &key)
            .expect("could not serialize key");
        let value_bytes = bincode::Options::serialize(self.bincode_options, &value)
            .expect("could not serialize value");

        let timestamp = TimestampMillis(
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_millis(),
        );

        let key_len = BytesLen(key_bytes.len() as u64);
        let value_len = BytesLen(value_bytes.len() as u64);

        let crc = {
            let mut crc = crc32fast::Hasher::new();
            crc.update(&timestamp.0.to_le_bytes());
            crc.update(&key_len.0.to_le_bytes());
            crc.update(&value_len.0.to_le_bytes());
            crc.update(&key_bytes);
            crc.update(&value_bytes);
            crc.finalize()
        };

        self.current_log_file.write_all(&crc.to_le_bytes())?;
        self.current_log_file
            .write_all(&timestamp.0.to_le_bytes())?;
        self.current_log_file.write_all(&key_len.0.to_le_bytes())?;
        self.current_log_file
            .write_all(&value_len.0.to_le_bytes())?;
        self.current_log_file.write_all(&key_bytes)?;
        self.current_log_file.write_all(&value_bytes)?;

        match self.options.sync_strategy {
            SyncStrategy::EveryWrite => self.sync()?,
            SyncStrategy::Interval(sync_interval) => {
                if std::time::Instant::now().duration_since(self.last_sync) >= sync_interval {
                    self.sync()?
                }
            }
            SyncStrategy::Manual => (),
        }

        let entry_len = (HEADER_LENGTH + key_bytes.len() + value_bytes.len()) as u64;

        match value {
            ValueOrDeletion::Value(_) => {
                let entry_ref = EntryRef {
                    file_id: self.current_log_file_id,
                    entry_len,
                    entry_offset: self.offset_bytes,
                    timestamp,
                };

                self.entry_refs.insert(key, entry_ref);
            }
            ValueOrDeletion::Tombstone => {
                self.entry_refs.remove(&key);
            }
        }

        self.offset_bytes += entry_len;
        self.current_log_file_size_bytes += entry_len as usize;

        self.maybe_rotate_large_log_file()?;

        Ok(())
    }

    /// create a new log file iff it is the same size or larger
    /// than the configured `max_file_len_target_bytes`
    fn maybe_rotate_large_log_file(&mut self) -> Result<()> {
        if self.current_log_file_size_bytes >= self.options.max_file_len_target_bytes {
            self.rotate_log_file()?;
        }

        Ok(())
    }

    fn rotate_log_file(&mut self) -> Result<()> {
        // force a sync to flush any pending writes to the current log file,
        // as data in the internal write buffer could represent partial writes,
        // and we do not want to stripe an entry across 2 different log files
        self.sync()?;

        let (new_log_file_id, new_log_file) = create_log_file(&self.options.data_directory)?;

        self.current_log_file_id = new_log_file_id;
        self.current_log_file = new_log_file;

        self.current_log_file_size_bytes = 0;
        self.offset_bytes = 0;

        Ok(())
    }

    fn copy_all_entries_to_current_log_file(&mut self) -> Result<()> {
        // this is a hack
        // so we can mutable iterate over entry_refs,
        // while mutably borrowing self in the loop.
        // if we don't move entry_refs out,
        // we can't call `self.copy_raw_entry_to_current_log_file`
        // in the loop
        let mut entry_refs = std::mem::take(&mut self.entry_refs);

        for entry_ref in entry_refs.values_mut() {
            self.copy_raw_entry_to_current_log_file(entry_ref)?;
        }

        // and reset it here
        self.entry_refs = entry_refs;

        self.sync()?;

        Ok(())
    }

    fn copy_raw_entry_to_current_log_file(&mut self, entry_ref: &mut EntryRef) -> Result<()> {
        self.maybe_rotate_large_log_file()?;

        let mut log_file_for_entry = entry_ref.entry_log_file(&self.options.data_directory)?;

        log_file_for_entry.seek(SeekFrom::Start(entry_ref.entry_offset))?;

        let mut take = log_file_for_entry.take(entry_ref.entry_len);

        let copied_len = std::io::copy(&mut take, &mut self.current_log_file)?;

        // lol
        // TODO
        // this should probably be a real error that we surface to the caller,
        // notifying them that data is corrupt
        assert_eq!(copied_len, entry_ref.entry_len);

        entry_ref.file_id = self.current_log_file_id;

        Ok(())
    }
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Cask<K, V> {
    /// Ensure that all pending insertions and deletes are persisted to disk.
    /// Calls fsync or its platform-specific equivalent.
    pub fn sync(&mut self) -> Result<()> {
        // flush the internal BufWriter
        self.current_log_file.flush()?;
        // force the underlying File to fsync
        self.current_log_file.get_ref().sync_all()?;
        self.last_sync = std::time::Instant::now();
        Ok(())
    }
}

impl<K, V> Drop for Cask<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        if self.options.sync_on_drop {
            self.sync().expect("could not flush writes on drop")
        }
    }
}

/// stream all log files and load their entries into EntryRefs
fn load_entries<K>(data_directory: &str) -> Result<EntryRefs<K>>
where
    K: Hash + Eq + serde::de::DeserializeOwned,
{
    let mut entry_refs = EntryRefs::new();

    for log_result in nonempty_log_files(data_directory)? {
        let (log_file_id, log_file_path) = log_result?;
        for offset_entry in stream_entry_refs(log_file_id, &log_file_path)? {
            let (key, entry_ref) = offset_entry?;

            // insert the entry_ref if its timestamp is later than
            // the existing timestamp, or insert it
            // if the key does not exist
            if let Some(e) = entry_refs.get_mut(&key) {
                if entry_ref.timestamp > e.timestamp {
                    *e = entry_ref;
                }
            } else {
                entry_refs.insert(key, entry_ref);
            }
        }
    }

    Ok(entry_refs)
}

/// return an iterator of all log file (id, path)
fn nonempty_log_files(
    data_directory: &str,
) -> Result<impl Iterator<Item = Result<(LogFileId, PathBuf)>> + '_> {
    Ok(
        all_log_files(data_directory)?.filter(|log_result| match log_result {
            Ok((_, log_file_path)) => log_file_path
                .metadata()
                .map_or(true, |metadata| metadata.len() > 0),
            Err(_e) => true,
        }),
    )
}

/// return an iterator of all log file (id, path)
/// NOTE: includes empty log files (those files where .len() == 0)
fn all_log_files(
    data_directory: &str,
) -> Result<impl Iterator<Item = Result<(LogFileId, PathBuf)>> + '_> {
    Ok(std::fs::read_dir(data_directory)
        .map_err(Error::Io)?
        // only log files or errors,
        // so we can propagate them
        .filter(|dir_entry_result| match dir_entry_result {
            Ok(dir_entry) => {
                dir_entry.path().is_file()
                    && dir_entry.path().extension().is_some()
                    && dir_entry.path().extension().unwrap() == "log"
            }
            Err(_) => true,
        })
        .map(move |dir_entry| match dir_entry {
            Ok(dir_entry) => {
                let current_log_file_id: LogFileId = LogFileId(
                    dir_entry
                        .path()
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<u128>()
                        .expect("could not parse as LogFileId"),
                );

                Ok((current_log_file_id, dir_entry.path()))
            }
            Err(e) => Err(e.into()),
        }))
}

fn stream_entry_refs<K>(
    log_file_id: LogFileId,
    log_file_path: &Path,
) -> std::io::Result<EntryRefOffsetIter<K>> {
    Ok(EntryRefOffsetIter {
        offset: 0,
        header_bytes: [0u8; HEADER_LENGTH],
        log_file_id,
        log_file: std::fs::File::open(log_file_path)?,
        bincode_options: bincode::DefaultOptions::new(),
        _k: PhantomData,
    })
}

struct EntryRefOffsetIter<K> {
    /// Offset into the file, in bytes
    offset: u64,
    /// Id of the log file
    log_file_id: LogFileId,
    /// The log file
    log_file: std::fs::File,
    /// Reusable buffer for loading each entry's header
    header_bytes: [u8; HEADER_LENGTH],
    bincode_options: bincode::DefaultOptions,
    _k: PhantomData<K>,
}

impl<K> Iterator for EntryRefOffsetIter<K>
where
    K: serde::de::DeserializeOwned,
{
    type Item = Result<(K, EntryRef)>;

    fn next(&mut self) -> Option<Self::Item> {
        let header = self.log_file.read_exact(&mut self.header_bytes);

        match header {
            Ok(_) => {
                let key_len_bytes = &self.header_bytes[KEY_LEN_BYTES_RANGE];
                let value_len_bytes = &self.header_bytes[VALUE_LEN_BYTES_RANGE];
                let key_len = BytesLen(u64::from_le_bytes(key_len_bytes.try_into().unwrap()));
                let value_len = BytesLen(u64::from_le_bytes(value_len_bytes.try_into().unwrap()));

                let mut key = vec![0; key_len.0 as usize];

                match self.log_file.read_exact(&mut key) {
                    Ok(_) => (),
                    Err(e) => return Some(Err(Error::Io(e))),
                };

                let key: K = match bincode::Options::deserialize(self.bincode_options, &key) {
                    Ok(k) => k,
                    Err(e) => return Some(Err(Error::Serde(e))),
                };

                let entry_len = HEADER_LENGTH as u64 + key_len.0 + value_len.0;

                let item = Ok((
                    key,
                    EntryRef {
                        file_id: self.log_file_id,
                        entry_len,
                        entry_offset: self.offset,
                        timestamp: TimestampMillis(u128::from_le_bytes(
                            self.header_bytes[TIMESTAMP_BYTES_RANGE].try_into().unwrap(),
                        )),
                    },
                ));

                self.offset += entry_len;

                Some(item)
            }
            Err(_e) => None,
        }
    }
}

/// note: assumes the data directory already exists!
fn create_log_file<P>(data_directory: P) -> std::io::Result<(LogFileId, BufWriter<File>)>
where
    P: AsRef<Path>,
{
    let current_log_file_id = LogFileId(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis(),
    );

    let mut current_log_file_path = PathBuf::new();
    current_log_file_path.push(&data_directory);
    current_log_file_path.push(current_log_file_id.0.to_string());
    current_log_file_path.set_extension("log");

    let current_log_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(current_log_file_path)?;

    Ok((current_log_file_id, BufWriter::new(current_log_file)))
}

#[cfg(test)]
mod tests {
    use crate::{Cask, Options};
    use std::collections::HashSet;
    use std::path::Path;
    use std::thread::sleep;

    struct RmDir<P: AsRef<Path>>(P);

    impl<P: AsRef<Path>> Drop for RmDir<P> {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn roundtrips_a_value() {
        let options = Options {
            data_directory: "test_data/round_trips_a_value".to_string(),
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        let mut db = Cask::open(options).unwrap();

        db.insert("a".to_string(), 0).unwrap();

        let read = db.get(&"a".to_string()).unwrap().unwrap();

        assert_eq!(read, 0);
    }

    #[test]
    fn inserts_overwrite() {
        let options = Options {
            data_directory: "test_data/inserts_overwrite".to_string(),
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        let mut db = Cask::open(options).unwrap();

        db.insert("a".to_string(), 0).unwrap();

        let read = db.get(&"a".to_string()).unwrap().unwrap();

        assert_eq!(read, 0);

        db.insert("a".to_string(), 1).unwrap();

        let read = db.get(&"a".to_string()).unwrap().unwrap();

        assert_eq!(read, 1);
    }

    #[test]
    fn delete_deletes() {
        let options = Options {
            data_directory: "test_data/delete_deletes".to_string(),
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        let mut db = Cask::open(options).unwrap();

        db.insert("a".to_string(), 0).unwrap();

        let read = db.get(&"a".to_string()).unwrap();

        assert_eq!(read, Some(0));

        db.delete("a".to_string()).unwrap();

        let read = db.get(&"a".to_string()).unwrap();

        assert_eq!(read, None);

        db.insert("a".to_string(), 2).unwrap();

        let read = db.get(&"a".to_string()).unwrap();

        assert_eq!(read, Some(2));
    }

    #[test]
    fn shows_keys() {
        let options = Options {
            data_directory: "test_data/shows_keys".to_string(),
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        let mut db = Cask::open(options).unwrap();

        db.insert("a".to_string(), 0).unwrap();

        let keys = db.keys().unwrap();

        assert_eq!(keys, vec![&"a"]);
    }

    #[test]
    fn delete_deletes_keys() {
        let options = Options {
            data_directory: "test_data/delete_deletes_keys".to_string(),
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        let mut db = Cask::open(options).unwrap();

        db.insert("a".to_string(), 0).unwrap();

        let keys = db.keys().unwrap();

        assert_eq!(keys, vec![&"a"]);

        db.delete("a".to_string()).unwrap();

        let keys = db.keys().unwrap();

        let keys_challenge: Vec<&&str> = vec![];

        assert_eq!(keys, keys_challenge);
    }

    #[test]
    fn loads_entries_on_start() {
        let options = Options {
            data_directory: "test_data/loads_entries_on_start".to_string(),
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        {
            let mut db1 = Cask::open(options.clone()).unwrap();

            db1.insert("a".to_string(), 99).unwrap();

            let keys = db1.keys().unwrap();

            assert_eq!(keys, vec![&"a"]);
        }

        sleep(std::time::Duration::from_millis(5));

        {
            let mut db2 = Cask::open(options.clone()).unwrap();

            db2.insert("b".to_string(), 14).unwrap();

            let keys: HashSet<&String> = db2.keys().unwrap().into_iter().collect();

            assert_eq!(keys, HashSet::from([&"a".to_string(), &"b".to_string()]));
        }

        sleep(std::time::Duration::from_millis(5));

        {
            let db3: Cask<String, i32> = Cask::open(options.clone()).unwrap();

            let loaded_keys: HashSet<&String> = db3.keys().unwrap().into_iter().collect();

            assert_eq!(
                loaded_keys,
                HashSet::from([&"a".to_string(), &"b".to_string()])
            );

            let a = db3.get(&"a".to_string()).unwrap();

            assert_eq!(a, Some(99));

            let b = db3.get(&"b".to_string()).unwrap();

            assert_eq!(b, Some(14));
        }
    }

    #[test]
    fn rotates_logs() {
        let options = Options {
            data_directory: "test_data/rotates_logs".to_string(),
            max_file_len_target_bytes: 10,
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        let mut db: Cask<String, String> = Cask::open(options).unwrap();

        let initial_log_file_id = db.current_log_file_id;

        // 5 bytes + 6 bytes = 11 bytes,
        // which is greater than 10 bytes,
        // and forces a rotation after the insertion
        db.insert("hello".to_string(), "there!".to_string())
            .unwrap();

        // the log file is rotated after the write
        assert!(db.current_log_file_id > initial_log_file_id);
    }

    #[test]
    fn merges_log_files() {
        let options = Options {
            data_directory: "test_data/merges".to_string(),
            ..Default::default()
        };

        let _rm = RmDir(options.data_directory.clone());

        for i in 0..5 {
            let mut db: Cask<String, usize> = Cask::open(options.clone()).unwrap();
            db.insert("a".to_string(), i).unwrap();
            sleep(std::time::Duration::from_millis(15));
        }

        assert_eq!(
            std::fs::read_dir(&options.data_directory)
                .unwrap()
                .collect::<Vec<_>>()
                .len(),
            5
        );

        sleep(std::time::Duration::from_millis(15));

        let mut db: Cask<String, usize> = Cask::open(options.clone()).unwrap();

        assert_eq!(
            std::fs::read_dir(&options.data_directory)
                .unwrap()
                .collect::<Vec<_>>()
                .len(),
            6
        );

        assert_eq!(db.get(&"a".to_string()).unwrap().unwrap(), 4);

        db.merge().unwrap();

        assert_eq!(db.get(&"a".to_string()).unwrap().unwrap(), 4);

        assert_eq!(
            std::fs::read_dir(&options.data_directory)
                .unwrap()
                .collect::<Vec<_>>()
                .len(),
            1
        );
    }
}

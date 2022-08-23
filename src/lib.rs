use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use thiserror::Error;

const MAX_FILE_SIZE_BYTES: usize = 2usize.pow(28);
const HEADER_LENGTH: usize = 36;

const CRC_BYTES_RANGE: Range<usize> = 0..4;
const TIMESTAMP_BYTES_RANGE: Range<usize> = 4..20;
const KEY_LEN_BYTES_RANGE: Range<usize> = 20..28;
const VALUE_LEN_BYTES_RANGE: Range<usize> = 28..HEADER_LENGTH;

type Entries<K> = HashMap<K, EntryRef>;
type FileId = u128;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("serde")]
    Serde(#[from] bincode::Error),
    #[error("io")]
    Io(#[from] std::io::Error),
}

#[derive(Serialize, Deserialize)]
enum ValueOrDeletion<V> {
    Value(V),
    Tombstone,
}

#[derive(Debug)]
pub struct Options {
    pub data_directory: String,
    pub sync_every_write: bool,
    pub sync_on_drop: bool,
    pub verify_crc_on_read: bool,
    pub max_file_size_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            data_directory: ".".to_string(),
            sync_every_write: true,
            sync_on_drop: true,
            verify_crc_on_read: true,
            max_file_size_bytes: MAX_FILE_SIZE_BYTES,
        }
    }
}

#[derive(Debug)]
pub struct Cask<
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
> {
    current_log_file: BufWriter<File>,
    current_log_file_id: FileId,
    entries: Entries<K>,
    offset_bytes: u64,
    options: Options,
    _v: PhantomData<V>,
}

/// Information about where to find an entry in the log files
#[derive(Debug)]
struct EntryRef {
    /// file containing this entry
    file_id: FileId,
    /// size of the entry on disk, in bytes
    entry_len: u64,
    /// the offset in the file where this entry is located, in bytes
    entry_offset: u64,
    /// millis since epoch
    timestamp: u128,
}

// struct Entry<K, V> {
//     crc: u32,
//     timestamp: u128,
//     key_size: u64,
//     value_size: u64,
//     key: K,
//     value: V,
// }

// impl<K, V> Entry<K, V>
// where
//     K: serde::de::DeserializeOwned,
//     V: serde::de::DeserializeOwned,
// {
//     fn key(&self) -> K {
//         self.key
//     }

//     fn value(&self) -> V {
//         self.value
//     }

//     fn len(&self) -> usize {
//         HEADER_LENGTH + self.key_size as usize + self.value_size as usize
//     }
// }

impl<K, V> Cask<K, V>
where
    K: Eq + Hash + serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    /// open a bitcask store
    pub fn open(options: Options) -> Result<Self> {
        std::fs::create_dir_all(&options.data_directory)?;

        let keys: HashMap<K, EntryRef> = load_entries(&options.data_directory)?;

        let (current_log_file_id, current_log_file) = create_log_file(&options.data_directory)?;

        Ok(Cask {
            current_log_file,
            current_log_file_id,
            entries: keys,
            offset_bytes: 0,
            options,
            _v: PhantomData,
        })
    }

    /// write a value to a key
    pub fn write(&mut self, key: K, value: V) -> Result<()> {
        self.write_internal(key, ValueOrDeletion::Value(value))
    }

    /// read a key's value
    pub fn read(&self, key: &K) -> Result<Option<V>> {
        if let Some(entry_ref) = self.entries.get(key) {
            //
            // open file
            //
            let file_id = entry_ref.file_id;
            let mut path = PathBuf::new();
            path.push(&self.options.data_directory);
            path.push(file_id.to_string());
            path.set_extension("log");
            let mut file = std::fs::File::open(path)?;

            //
            // read entry at position
            //
            file.seek(SeekFrom::Start(entry_ref.entry_offset))?;
            let mut buf = vec![0; entry_ref.entry_len as usize];
            file.read_exact(&mut buf)?;

            //
            // get header fields
            //
            let expected_crc = u32::from_le_bytes(buf[CRC_BYTES_RANGE].try_into().unwrap());
            let timestamp_bytes = &buf[TIMESTAMP_BYTES_RANGE];
            let key_len_bytes = &buf[KEY_LEN_BYTES_RANGE];
            let value_len_bytes = &buf[VALUE_LEN_BYTES_RANGE];

            //
            // get kv
            //
            let key_len: u64 = u64::from_le_bytes(key_len_bytes.try_into().unwrap());
            let value_len = u64::from_le_bytes(value_len_bytes.try_into().unwrap());
            let key_bytes = &buf[HEADER_LENGTH..(HEADER_LENGTH + key_len as usize)];
            let value_bytes = &buf[(HEADER_LENGTH + key_len as usize)
                ..(HEADER_LENGTH + key_len as usize + value_len as usize)];

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
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "crc32's do not match; data is invalid",
                    ))?;
                }
            }

            //
            // deserialize the value
            //
            let value: ValueOrDeletion<V> = bincode::deserialize(value_bytes)?;

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
        self.write_internal(key, ValueOrDeletion::Tombstone)
    }

    /// get all keys
    pub fn keys(&self) -> Result<Vec<&K>> {
        Ok(self.entries.keys().collect())
    }

    /// merge logfiles to their most compact representation
    /// invariant: user-visible database should be the same after merging as
    /// it was before merging
    /// TODO should this return the number of scrubbed records?
    pub fn merge(&mut self) -> Result<()> {
        todo!()
    }

    fn write_internal(&mut self, key: K, value: ValueOrDeletion<V>) -> Result<()> {
        let key_bytes = bincode::serialize(&key).expect("could not serialize key");
        let value_bytes = bincode::serialize(&value).expect("could not serialize value");

        let timestamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis();

        let key_len = key_bytes.len() as u64;
        let value_len = value_bytes.len() as u64;

        let crc = {
            let mut crc = crc32fast::Hasher::new();
            crc.update(&timestamp.to_le_bytes());
            crc.update(&key_len.to_le_bytes());
            crc.update(&value_len.to_le_bytes());
            crc.update(&key_bytes);
            crc.update(&value_bytes);
            crc.finalize()
        };

        self.current_log_file.write_all(&crc.to_le_bytes())?;
        self.current_log_file.write_all(&timestamp.to_le_bytes())?;
        self.current_log_file.write_all(&key_len.to_le_bytes())?;
        self.current_log_file.write_all(&value_len.to_le_bytes())?;
        self.current_log_file.write_all(&key_bytes)?;
        self.current_log_file.write_all(&value_bytes)?;

        if self.options.sync_every_write {
            self.sync()?;
        }

        let entry_len = (crc.to_le_bytes().len()
            + timestamp.to_le_bytes().len()
            + key_len.to_le_bytes().len()
            + value_len.to_le_bytes().len()
            + key_bytes.len()
            + value_bytes.len()) as u64;

        match value {
            ValueOrDeletion::Value(_) => {
                let entry_ref = EntryRef {
                    file_id: self.current_log_file_id,
                    entry_len,
                    entry_offset: self.offset_bytes,
                    timestamp,
                };

                self.offset_bytes += entry_len;

                self.entries.insert(key, entry_ref);
            }
            ValueOrDeletion::Tombstone => {
                self.offset_bytes += entry_len;

                self.entries.remove(&key);
            }
        }

        Ok(())
    }
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Cask<K, V> {
    /// ensure that all pending insertions and deletes are persisted to disk
    pub fn sync(&mut self) -> Result<()> {
        self.current_log_file.flush()?;
        Ok(())
    }
}

impl<K, V> Drop for Cask<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        self.sync().expect("could not flush writes on drop")
    }
}

fn load_entries<K>(data_directory: &str) -> Result<HashMap<K, EntryRef>>
where
    K: Hash + Eq + serde::de::DeserializeOwned,
{
    let mut entries = Entries::new();

    for dir_entry in std::fs::read_dir(data_directory)? {
        let dir_entry = dir_entry?;

        if dir_entry.path().is_file()
            && dir_entry.path().extension().is_some()
            && dir_entry.path().extension().unwrap() == "log"
        {
            let current_log_file_id = dir_entry
                .path()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u128>()
                .unwrap();

            let mut current_log_file_path = PathBuf::new();
            current_log_file_path.push(&data_directory);
            current_log_file_path.push(current_log_file_id.to_string());
            current_log_file_path.set_extension("log");

            if current_log_file_path.metadata()?.len() > 0 {
                for offset_entry in stream_entry_refs(current_log_file_id, &current_log_file_path)?
                {
                    let (key, entry_ref) = offset_entry?;

                    // insert the entry_ref if its timestamp is later than
                    // the existing timestamp, or insert it
                    // if the key does not exist
                    if let Some(e) = entries.get_mut(&key) {
                        if entry_ref.timestamp > e.timestamp {
                            *e = entry_ref;
                        }
                    } else {
                        entries.insert(key, entry_ref);
                    }
                }
            }
        }
    }

    Ok(entries)
}

fn stream_entry_refs<K>(
    log_file_id: u128,
    log_file_path: &Path,
) -> std::io::Result<EntryRefOffsetIter<K>> {
    Ok(EntryRefOffsetIter {
        offset: 0,
        header_bytes: [0u8; HEADER_LENGTH],
        log_file_id,
        log_file: std::fs::File::open(log_file_path)?,
        _k: PhantomData,
    })
}

struct EntryRefOffsetIter<K> {
    offset: u64,
    log_file_id: u128,
    log_file: std::fs::File,
    header_bytes: [u8; HEADER_LENGTH],
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
                let key_len = u64::from_le_bytes(key_len_bytes.try_into().unwrap());
                let value_len = u64::from_le_bytes(value_len_bytes.try_into().unwrap());

                let mut key = vec![0; key_len as usize];

                match self.log_file.read_exact(&mut key) {
                    Ok(_) => (),
                    Err(e) => return Some(Err(Error::Io(e))),
                };

                let key: K = match bincode::deserialize(&key) {
                    Ok(k) => k,
                    Err(e) => return Some(Err(Error::Serde(e))),
                };

                let entry_len = HEADER_LENGTH as u64 + key_len + value_len;

                let item = Ok((
                    key,
                    EntryRef {
                        file_id: self.log_file_id,
                        entry_len,
                        entry_offset: self.offset,
                        timestamp: u128::from_le_bytes(
                            self.header_bytes[TIMESTAMP_BYTES_RANGE].try_into().unwrap(),
                        ),
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
fn create_log_file<P>(data_directory: P) -> std::io::Result<(u128, BufWriter<File>)>
where
    P: AsRef<Path>,
{
    let current_log_file_id = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis();

    let mut current_log_file_path = PathBuf::new();
    current_log_file_path.push(&data_directory);
    current_log_file_path.push(current_log_file_id.to_string());
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
    use std::{collections::HashSet, thread::sleep};

    use crate::{Cask, Options};

    struct RmDir(&'static str);

    impl Drop for RmDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(self.0).unwrap();
        }
    }

    #[test]
    fn roundtrips_a_value() {
        let _rm = RmDir("test_data/round_trips_a_value");

        let options = Options {
            data_directory: "test_data/round_trips_a_value".to_string(),
            ..Default::default()
        };

        let mut db = Cask::open(options).unwrap();

        db.write("a".to_string(), 0).unwrap();

        let read = db.read(&"a".to_string()).unwrap().unwrap();

        assert_eq!(read, 0);
    }

    #[test]
    fn writes_overwrite() {
        let _rm = RmDir("test_data/writes_overwrite");

        let options = Options {
            data_directory: "test_data/writes_overwrite".to_string(),
            ..Default::default()
        };

        let mut db = Cask::open(options).unwrap();

        db.write("a".to_string(), 0).unwrap();

        let read = db.read(&"a".to_string()).unwrap().unwrap();

        assert_eq!(read, 0);

        db.write("a".to_string(), 1).unwrap();

        let read = db.read(&"a".to_string()).unwrap().unwrap();

        assert_eq!(read, 1);
    }

    #[test]
    fn delete_deletes() {
        let _rm = RmDir("test_data/delete_deletes");

        let options = Options {
            data_directory: "test_data/delete_deletes".to_string(),
            ..Default::default()
        };

        let mut db = Cask::open(options).unwrap();

        db.write("a".to_string(), 0).unwrap();

        let read = db.read(&"a".to_string()).unwrap();

        assert_eq!(read, Some(0));

        db.delete("a".to_string()).unwrap();

        let read = db.read(&"a".to_string()).unwrap();

        assert_eq!(read, None);

        db.write("a".to_string(), 2).unwrap();

        let read = db.read(&"a".to_string()).unwrap();

        assert_eq!(read, Some(2));
    }

    #[test]
    fn shows_keys() {
        let _rm = RmDir("test_data/shows_keys");

        let options = Options {
            data_directory: "test_data/shows_keys".to_string(),
            ..Default::default()
        };

        let mut db = Cask::open(options).unwrap();

        db.write("a".to_string(), 0).unwrap();

        let keys = db.keys().unwrap();

        assert_eq!(keys, vec![&"a"]);
    }

    #[test]
    fn delete_deletes_keys() {
        let _rm = RmDir("test_data/delete_deletes_keys");

        let options = Options {
            data_directory: "test_data/delete_deletes_keys".to_string(),
            ..Default::default()
        };

        let mut db = Cask::open(options).unwrap();

        db.write("a".to_string(), 0).unwrap();

        let keys = db.keys().unwrap();

        assert_eq!(keys, vec![&"a"]);

        db.delete("a".to_string()).unwrap();

        let keys = db.keys().unwrap();

        let keys_challenge: Vec<&&str> = vec![];

        assert_eq!(keys, keys_challenge);
    }

    #[test]
    fn loads_entries_on_start() {
        let _rm = RmDir("test_data/loads_entries_on_start");

        {
            let options = Options {
                data_directory: "test_data/loads_entries_on_start".to_string(),
                ..Default::default()
            };

            let mut db1 = Cask::open(options).unwrap();

            db1.write("a".to_string(), 99).unwrap();

            let keys = db1.keys().unwrap();

            assert_eq!(keys, vec![&"a"]);
        }

        sleep(std::time::Duration::from_millis(5));

        {
            let options = Options {
                data_directory: "test_data/loads_entries_on_start".to_string(),
                ..Default::default()
            };

            let mut db2 = Cask::open(options).unwrap();

            db2.write("b".to_string(), 14).unwrap();

            let keys: HashSet<&String> = db2.keys().unwrap().into_iter().collect();

            assert_eq!(keys, HashSet::from([&"a".to_string(), &"b".to_string()]));
        }

        sleep(std::time::Duration::from_millis(5));

        {
            let options = Options {
                data_directory: "test_data/loads_entries_on_start".to_string(),
                ..Default::default()
            };

            let db3: Cask<String, i32> = Cask::open(options).unwrap();

            let loaded_keys: HashSet<&String> = db3.keys().unwrap().into_iter().collect();

            assert_eq!(
                loaded_keys,
                HashSet::from([&"a".to_string(), &"b".to_string()])
            );

            let a = db3.read(&"a".to_string()).unwrap();

            assert_eq!(a, Some(99));

            let b = db3.read(&"b".to_string()).unwrap();

            assert_eq!(b, Some(14));
        }
    }
}

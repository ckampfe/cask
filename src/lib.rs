use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const MAX_FILE_SIZE_BYTES: usize = 2usize.pow(28);

#[derive(Debug, Serialize, Deserialize)]
enum ValueOrDeletion<V> {
    Value(V),
    Tombstone,
}

#[derive(Debug)]
pub struct Options {
    pub data_directory: String,
    pub sync_after_every_write: bool,
    pub check_crc_on_every_read: bool,
    pub max_file_size_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            data_directory: ".".to_string(),
            sync_after_every_write: true,
            check_crc_on_every_read: true,
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
    keys: HashMap<K, EntryRef>,
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
    entry_size: u64,
    /// the offset in the file where this entry is located, in bytes
    entry_offset: u64,
    /// millis since epoch
    timestamp: u128,
}

// struct Entry {
//     crc: u32,
//     timestamp: u128,
//     key_size: u64,
//     value_size: u64,
//     key: Vec<u8>,
//     value: Vec<u8>,
// }

type FileId = u128;

impl<K, V> Cask<K, V>
where
    K: std::fmt::Debug + Eq + Hash + serde::Serialize + serde::de::DeserializeOwned,
    V: std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
{
    /// open a bitcask store
    pub fn open(options: Options) -> std::io::Result<Self> {
        let (current_log_file_id, current_log_file) = create_log_file(&options.data_directory)?;

        Ok(Cask {
            current_log_file,
            current_log_file_id,
            keys: HashMap::new(),
            offset_bytes: 0,
            options,
            _v: PhantomData,
        })
    }

    /// write a value to a key
    pub fn write(&mut self, key: K, value: V) -> std::io::Result<()> {
        self.write_internal(key, ValueOrDeletion::Value(value))
    }

    fn write_internal(&mut self, key: K, value: ValueOrDeletion<V>) -> std::io::Result<()> {
        let key_bytes: Vec<u8> = bincode::serialize(&key).expect("could not serialize key");
        let value_bytes = bincode::serialize(&value).expect("could not serialize value");

        let mut crc = crc32fast::Hasher::new();

        let timestamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis();

        let key_size = key_bytes.len() as u64;
        let value_size = value_bytes.len() as u64;

        crc.update(&timestamp.to_be_bytes());
        crc.update(&key_size.to_be_bytes());
        crc.update(&value_size.to_be_bytes());
        crc.update(&key_bytes);
        crc.update(&value_bytes);

        let crc = crc.finalize();

        self.current_log_file.write_all(&crc.to_be_bytes())?;
        self.current_log_file.write_all(&timestamp.to_be_bytes())?;
        self.current_log_file.write_all(&key_size.to_be_bytes())?;
        self.current_log_file.write_all(&value_size.to_be_bytes())?;
        self.current_log_file.write_all(&key_bytes)?;
        self.current_log_file.write_all(&value_bytes)?;

        if self.options.sync_after_every_write {
            self.sync()?;
        }

        let entry_size = (crc.to_be_bytes().len()
            + timestamp.to_be_bytes().len()
            + key_size.to_be_bytes().len()
            + value_size.to_be_bytes().len()
            + key_bytes.len()
            + value_bytes.len()) as u64;

        match value {
            ValueOrDeletion::Value(_) => {
                let entry = EntryRef {
                    file_id: self.current_log_file_id,
                    entry_size,
                    entry_offset: self.offset_bytes,
                    timestamp,
                };

                self.offset_bytes += entry_size;

                self.keys.insert(key, entry);
            }
            ValueOrDeletion::Tombstone => {
                self.offset_bytes += entry_size;

                self.keys.remove(&key);
            }
        }

        Ok(())
    }

    /// read a key's value
    pub fn read(&self, key: &K) -> std::io::Result<Option<V>> {
        if let Some(entry) = self.keys.get(key) {
            let file_id = entry.file_id;
            let mut path = PathBuf::new();
            path.push(&self.options.data_directory);
            path.push(file_id.to_string());
            path.set_extension("log");
            let mut file = std::fs::File::open(path)?;

            file.seek(SeekFrom::Start(entry.entry_offset))?;
            let mut buf = vec![0; entry.entry_size as usize];
            file.read_exact(&mut buf)?;

            let crc_challenge = u32::from_be_bytes(buf[0..4].try_into().unwrap());
            let timestamp_bytes = &buf[4..20];
            let key_size_bytes = &buf[20..28];
            let value_size_bytes = &buf[28..36];

            let key_size: u64 = u64::from_be_bytes(buf[20..28].try_into().unwrap());
            let value_size = u64::from_be_bytes(buf[28..36].try_into().unwrap());

            let key_bytes = &buf[36..(36 + key_size as usize)];
            let value_bytes =
                &buf[(36 + key_size as usize)..(36 + key_size as usize + value_size as usize)];

            if self.options.check_crc_on_every_read {
                let mut crc_hasher = crc32fast::Hasher::new();
                crc_hasher.update(timestamp_bytes);
                crc_hasher.update(key_size_bytes);
                crc_hasher.update(value_size_bytes);
                crc_hasher.update(key_bytes);
                crc_hasher.update(value_bytes);
                let calculated_crc = crc_hasher.finalize();

                if calculated_crc != crc_challenge {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "crc32's do not match; data is invalid",
                    ));
                }
            }

            let value: ValueOrDeletion<V> = bincode::deserialize(value_bytes).unwrap();

            match value {
                ValueOrDeletion::Value(value) => Ok(Some(value)),
                ValueOrDeletion::Tombstone => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: K) -> std::io::Result<()> {
        self.write_internal(key, ValueOrDeletion::Tombstone)
    }

    /// get all keys
    pub fn keys(&self) -> std::io::Result<Vec<&K>> {
        Ok(self.keys.keys().collect())
    }

    /// merge logfiles to their most compact representation
    /// invariant: user-visible database should be the same after merging as
    /// it was before merging
    /// TODO should this return the number of scrubbed records?
    pub fn merge(&mut self) -> std::io::Result<()> {
        todo!()
    }

    /// ensure that all pending writes (and deletes) are persisted to disk
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.current_log_file.flush()
    }
}

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

    std::fs::create_dir_all(&data_directory)?;

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

    #[test]
    fn roundtrips_a_value() {
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
}

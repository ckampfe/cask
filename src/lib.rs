use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
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
pub struct Cask<K: Into<Vec<u8>>, V: serde::Serialize + for<'de> serde::Deserialize<'de>> {
    current_log_file: BufWriter<File>,
    current_log_file_id: FileId,
    file_ids: Vec<FileId>,
    keys: HashMap<K, Entry>,
    offset_bytes: u64,
    options: Options,
    _v: PhantomData<V>,
}

#[derive(Debug)]
struct Entry {
    /// file containing this entry
    file_id: FileId,
    /// size of the entry on disk, in bytes
    entry_size: u64,
    /// the offset in the file where this entry is located, in bytes
    entry_offset: u64,
    /// millis since epoch
    timestamp: u128,
}

type FileId = u128;

impl<K, V> Cask<K, V>
where
    K: Clone + Eq + Hash + Into<Vec<u8>>,
    V: std::fmt::Debug + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// open a bitcask store
    pub fn open(options: Options) -> std::io::Result<Self> {
        let current_log_file_id = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis();

        let mut current_log_file_path = PathBuf::new();
        current_log_file_path.push(&options.data_directory);
        current_log_file_path.push(current_log_file_id.to_string());
        current_log_file_path.set_extension("log");

        std::fs::create_dir_all(&options.data_directory)?;

        let current_log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(current_log_file_path)?;

        let current_log_file = BufWriter::new(current_log_file);

        let mut file_ids = vec![];

        for path in std::fs::read_dir(&options.data_directory)? {
            let path = path?.path();
            if path.is_file() {
                if let Some(extension) = path.extension() {
                    if extension == "log" {
                        file_ids.push(
                            path.file_stem()
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .parse::<u128>()
                                .unwrap(),
                        );
                    }
                }
            }
        }

        Ok(Cask {
            current_log_file,
            current_log_file_id,
            file_ids,
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
        let key_bytes: Vec<u8> = key.clone().into();
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
                let entry = Entry {
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
    pub fn read(&mut self, key: K) -> std::io::Result<Option<V>> {
        if let Some(entry) = self.keys.get(&key) {
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

            let value: ValueOrDeletion<V> = bincode::deserialize(&value_bytes).unwrap();
            dbg!(&value);

            match value {
                ValueOrDeletion::Value(value) => Ok(Some(value)),
                ValueOrDeletion::Tombstone => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// delete a key by inserting a tombstone value
    pub fn delete(&mut self, key: K) -> std::io::Result<()> {
        self.write_internal(key, ValueOrDeletion::Tombstone)
    }

    /// get all keys
    pub fn keys(&self) -> std::io::Result<Vec<&K>> {
        Ok(self.keys.keys().collect())
    }

    /// merge logfiles to their most compact representation
    pub fn merge(&self) -> std::io::Result<()> {
        todo!()
    }

    /// ensure that all pending writes (and deletes) are persisted to disk
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.current_log_file.flush()
    }
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

        let mut db: Cask<&str, usize> = Cask::open(options).unwrap();

        db.write("a", 0).unwrap();

        let read = db.read("a").unwrap().unwrap();

        assert_eq!(read, 0);
    }

    #[test]
    fn writes_overwrite() {
        let options = Options {
            data_directory: "test_data/writes_overwrite".to_string(),
            ..Default::default()
        };

        let mut db: Cask<&str, usize> = Cask::open(options).unwrap();

        db.write("a", 0).unwrap();

        let read = db.read("a").unwrap().unwrap();

        assert_eq!(read, 0);

        db.write("a", 1).unwrap();

        let read = db.read("a").unwrap().unwrap();

        assert_eq!(read, 1);
    }

    #[test]
    fn delete_deletes() {
        let options = Options {
            data_directory: "test_data/delete_deletes".to_string(),
            ..Default::default()
        };

        let mut db: Cask<&str, usize> = Cask::open(options).unwrap();

        db.write("a", 0).unwrap();

        let read = db.read("a").unwrap();

        assert_eq!(read, Some(0));

        db.delete("a").unwrap();

        let read = db.read("a").unwrap();

        assert_eq!(read, None);

        db.write("a", 2).unwrap();

        let read = db.read("a").unwrap();

        assert_eq!(read, Some(2));
    }

    #[test]
    fn shows_keys() {
        let options = Options {
            data_directory: "test_data/shows_keys".to_string(),
            ..Default::default()
        };

        let mut db: Cask<&str, usize> = Cask::open(options).unwrap();

        db.write("a", 0).unwrap();

        let keys = db.keys().unwrap();

        assert_eq!(keys, vec![&"a"]);
    }

    #[test]
    fn delete_deletes_keys() {
        let options = Options {
            data_directory: "test_data/delete_deletes_keys".to_string(),
            ..Default::default()
        };

        let mut db: Cask<&str, usize> = Cask::open(options).unwrap();

        db.write("a", 0).unwrap();

        let keys = db.keys().unwrap();

        assert_eq!(keys, vec![&"a"]);

        db.delete("a").unwrap();

        let keys = db.keys().unwrap();

        let keys_challenge: Vec<&&str> = vec![];

        assert_eq!(keys, keys_challenge);
    }
}

// We use skiplist as container.
// No hard deletion.
// Insertion with same key is update.
//
//
//
//
use std::path::Path;
// use std::ops::{Deref, DerefMut};

use crate::log::LogReader;

use skiplist::SkipMap;
use anyhow::Result;
use bincode::{config, Decode, Encode};

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum ValueUpdate {
    Tombstone,
    Value(Vec<u8>),
}



#[derive(PartialEq, Eq)]
pub struct MemTable {
    container: SkipMap<Vec<u8>, ValueUpdate>,
    approx_size: u64,
}

// impl Deref for MemTable {
    // type Target = SkipMap<Vec<u8>, ValueUpdate>;

    // fn deref(&self) -> &Self::Target {
        // &self.container
    // }
// }

// impl DerefMut for MemTable {
    // fn deref_mut(&mut self) -> &mut Self::Target {
        // &mut self.container
    // }
// }

impl MemTable {
    // Generate memtable from log or just new one.
    // Use a new empty table if anything unexpected happens.
    // Though it can lead to data missing.
    pub fn recover_from_log(dir_path: &Path) -> Result<MemTable> {
        let config = bincode::config::standard();
        let reader = LogReader::new(dir_path)?;
        let mut table = Self::new();
        for entry in reader.iter() {
            let (key, update): (Vec<u8>, ValueUpdate) = bincode::decode_from_slice(&entry[..], config)?.0;
            table.insert(key, update);
        }
        Ok(table)
    }

    pub fn new() -> MemTable {
        MemTable {
            container: SkipMap::new(),
            approx_size: 0,
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, update: ValueUpdate) -> Option<ValueUpdate> {
        match &update {
            ValueUpdate::Value(v) => self.approx_size += key.len() as u64 + v.len() as u64 + 4,
            ValueUpdate::Tombstone => self.approx_size += key.len() as u64 + 4,
        }
        let old_value = self.container.insert(key.clone(), update);
        if let Some(old) = old_value.clone() {
            match old {
                ValueUpdate::Tombstone => self.approx_size -= key.len() as u64 + 4,
                ValueUpdate::Value(v) => self.approx_size -= key.len() as u64 + 4 + v.len() as u64,
            }
        }
        old_value
    }

    pub fn approx_size(&self) -> u64 {
        self.approx_size
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&ValueUpdate> {
        self.container.get(key)
    }

    pub fn front(&self) -> Option<(&Vec<u8>, &ValueUpdate)> {
        self.container.front()
    }

    pub fn back(&self) -> Option<(&Vec<u8>, &ValueUpdate)> {
        self.container.back()
    }

    pub fn iter(&self) -> skiplist::skipmap::Iter<Vec<u8>, ValueUpdate> {
        self.container.iter()
    }

    pub fn clear(&mut self) {
        self.container.clear();
        self.approx_size = 0;
    }

    pub fn len(&self) -> usize {
        self.container.len()
    }

    pub fn should_flush(&self) -> bool {
        self.approx_size() >= u64::pow(2, 20) // 1MB
    }

    // pub fn from_log() {}

    // pub fn insert(&mut self, key: Vec<u8>, update: ValueUpdate) -> Option<ValueUpdate> {
        // (*self).insert(key, update)
    // }

    // pub fn get(&self, key: &[u8]) -> Option<&ValueUpdate> {
        // (*self).container.get(key)
    // }
}

#[cfg(test)]
mod tests {
    

    use crate::test_util::*;
    use crate::log::{LogWriter, LOG_FILE_MAX_SIZE};
    use crate::memtable::{ValueUpdate, MemTable};
    

    use rand::Rng;
    use anyhow::{Result, bail, ensure};

    #[test]
    fn recover_from_log() -> Result<()> {
        // Set up log_writer and memtable.
        // Write to both.
        // Generate a new memtable from the log.
        // Compare two memtables.
        let test_dir_path = create_test_dir()?;
        let mut log_writer = LogWriter::new(&test_dir_path)?;
        let mut table = MemTable::recover_from_log(&test_dir_path)?;
        let config = bincode::config::standard();

        for _ in 0..1024 {
            let key = get_random_bytes(1, 10);
            let update = if rand::thread_rng().gen::<f64>() > 0.5 {
                ValueUpdate::Tombstone
            } else {
                ValueUpdate::Value(get_random_bytes(1, usize::pow(2, 10)))
            };
            let payload = bincode::encode_to_vec(&(&key, &update), config)?;
            log_writer.write(&payload)?;
            table.insert(key, update);
        }
        ensure!(log_writer.len()? <= LOG_FILE_MAX_SIZE, "Log writer wrote too much into a single log file");

        let recovered_table = MemTable::recover_from_log(&test_dir_path)?;
        if recovered_table == table {
            Ok(())
        } else {
            bail!("Recovered memtable is not valid");
        }
    }
}

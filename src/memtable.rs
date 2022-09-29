// We use skiplist as container.
// No hard deletion.
// Insertion with same key is update.
//
//
//
//
use std::path::Path;
use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::collections::VecDeque;

use crate::log::LogReader;

use skiplist::SkipMap;
use anyhow::Result;
use bincode::{config, Decode, Encode};


pub const MEMTABLE_LOG_FILENAME: &str = "MEMTABLE_LOG";

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum ValueUpdate {
    Tombstone,
    Value(Vec<u8>),
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum MemTableAction {
    Commit,
    Insert((Vec<u8>, ValueUpdate,)),
}


pub struct MemTableKeeper {
    memtable: MemTable,
    batch: Vec<MemTableAction>,
    log: File,
}

impl PartialEq for MemTableKeeper {
    fn eq(&self, other: &Self) -> bool {
        self.memtable == other.memtable
    }
}

impl Eq for MemTableKeeper {}

impl MemTableKeeper {
    pub fn new(store_dir: &Path) -> Result<MemTableKeeper> {
        Ok(MemTableKeeper {
            memtable: MemTable::new(),
            batch: Vec::new(),
            log: File::options().create(true).write(true).open(store_dir.join(MEMTABLE_LOG_FILENAME))?,
        })
    }

    pub fn recover(store_dir: &Path) -> Result<MemTableKeeper> {
        let mut log = File::options().read(true).write(true).open(store_dir.join(MEMTABLE_LOG_FILENAME))?;
        let mut buf = Vec::new();
        log.read_to_end(&mut buf)?;

        let mut memtable = MemTable::new();
        let mut batch = VecDeque::new();

        let mut cur = 0;
        while cur < buf.len() {
            if let Ok((action, size,)) = bincode::decode_from_slice(&buf[cur..], bincode::config::standard()) {
                cur += size;
                match action {
                    MemTableAction::Commit => {
                        while let Some(action) = batch.pop_front() {
                            memtable.execute_action(action);
                        }
                    },
                    _ => {
                        batch.push_back(action);
                    },
                };
            }
        }
        todo!()
    }

    pub fn add_action(&mut self, action: MemTableAction) {
        self.batch.push(action);
    }

    pub fn commit(&mut self) -> Result<()> {
        todo!()
    }

    pub fn insert(&mut self, key: Vec<u8>, update: ValueUpdate) {
        self.batch.push(MemTableAction::Insert((key, update,)));
    }

    pub fn approx_size(&self) -> u64 {
        self.memtable.approx_size()
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&ValueUpdate> {
        self.memtable.get(key)
    }

    pub fn front(&self) -> Option<(&Vec<u8>, &ValueUpdate)> {
        self.memtable.front()
    }

    pub fn back(&self) -> Option<(&Vec<u8>, &ValueUpdate)> {
        self.memtable.back()
    }

    pub fn iter(&self) -> skiplist::skipmap::Iter<Vec<u8>, ValueUpdate> {
        self.memtable.iter()
    }

    pub fn clear(&mut self) {
        self.memtable.clear();
    }

    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    pub fn should_flush(&self) -> bool {
        self.memtable.should_flush()
    }
}



#[derive(PartialEq, Eq)]
pub struct MemTable {
    container: SkipMap<Vec<u8>, ValueUpdate>,
    approx_size: u64,
}


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

    pub fn execute_action(&mut self, action: MemTableAction) {
        match action {
            MemTableAction::Insert((key, update,)) => { self.insert(key.clone(), update.clone()); },
            _ => {},
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, update: ValueUpdate) -> Option<ValueUpdate> {
        let key_len = key.len(); 
        match &update {
            ValueUpdate::Value(v) => self.approx_size += key_len as u64 + v.len() as u64 + 20, // two varstring + enum tag. let length of varstring be u64.
            ValueUpdate::Tombstone => self.approx_size += key_len as u64 + 12,
        }
        let old_value = self.container.insert(key, update);
        if let Some(old) = old_value.clone() {
            match old {
                ValueUpdate::Tombstone => self.approx_size -= key_len as u64 + 12,
                ValueUpdate::Value(v) => self.approx_size -= key_len as u64 + v.len() as u64 + 20,
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
}

#[cfg(test)]
mod tests {
    
    use std::sync::mpsc;
    use std::thread;
    use std::thread::JoinHandle; 

    use crate::test_util::*;
    use crate::log::{LogWriter, LOG_FILE_MAX_SIZE};
    use crate::memtable::*;
    

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

    #[test]
    fn test_recovery() -> Result<()> {
        // Write some data. Used for verification.
        let test_dir0 = create_test_dir()?;
        let (tx, rx) = mpsc::channel(); 
        {
            let test_dir = test_dir0.clone();
            thread::spawn(move || -> Result<()> {
                let mut keeper = MemTableKeeper::new(&test_dir)?;
                for (action, killed) in rx {
                    if killed {
                        break;
                    }
                    if action == MemTableAction::Commit {
                        keeper.commit()?;
                    } else {
                        keeper.add_action(action);
                    }
                }
                Ok(())
            });

        }

        let test_dir = create_test_dir()?;
        let mut keeper = MemTableKeeper::new(&test_dir)?;
        for i in 0..1024 {
            let key = get_random_bytes(1, 10);
            let update = if rand::thread_rng().gen::<f64>() > 0.8 {
                ValueUpdate::Tombstone
            } else {
                ValueUpdate::Value(get_random_bytes(1, usize::pow(2, 10)))
            };
            keeper.insert(key.clone(), update.clone());
            tx.send((MemTableAction::Insert((key, update,)), false,))?;
            if i % 16 == 0 {
                keeper.commit()?;
                tx.send((MemTableAction::Commit, false,))?;
            }
            if i == 1000 {
                tx.send((MemTableAction::Commit, true,))?;
                break;
            }
        }

        let recovered_keeper = MemTableKeeper::recover(&test_dir0)?;
        ensure!(keeper.len() != 0, "Memtable shouldn't be empty");
        ensure!(keeper == recovered_keeper, "Recovered memtable has inconsistent data");
        Ok(())
    }
}

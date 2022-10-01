// We use skiplist as container.
// No hard deletion.
// Insertion with same key is update.
//
//
//
//
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::Path;


use anyhow::Result;
use bincode::{config, Decode, Encode};
use skiplist::SkipMap;

pub const MEMTABLE_LOG_FILENAME: &str = "MEMTABLE_LOG";

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum ValueUpdate {
    Tombstone,
    Value(Vec<u8>),
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum MemTableAction {
    Commit,
    Insert((Vec<u8>, ValueUpdate)),
}

pub struct MemTableKeeper {
    memtable: MemTable,
    batch: VecDeque<MemTableAction>,
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
            batch: VecDeque::new(),
            log: File::options()
                .create(true)
                .write(true)
                .open(store_dir.join(MEMTABLE_LOG_FILENAME))?,
        })
    }

    pub fn recover(store_dir: &Path) -> Result<MemTableKeeper> {
        let mut log = File::options()
            .read(true)
            .write(true)
            .open(store_dir.join(MEMTABLE_LOG_FILENAME))?;
        let mut buf = Vec::new();
        log.read_to_end(&mut buf)?;

        let mut memtable = MemTable::new();
        let mut batch = VecDeque::new();

        let mut cur = 0;
        while cur < buf.len() {
            if let Ok((action, size)) =
                bincode::decode_from_slice(&buf[cur..], bincode::config::standard())
            {
                cur += size;
                match action {
                    MemTableAction::Commit => {
                        while let Some(action) = batch.pop_front() {
                            memtable.execute_action(action);
                        }
                    }
                    _ => {
                        batch.push_back(action);
                    }
                };
            } else {
                // Meets half written batch.
                // Rollback by delete them.
                log.set_len(cur as u64)?;
                break;
            }
        }
        Ok(MemTableKeeper {
            memtable,
            batch: VecDeque::new(),
            log,
        })
    }

    pub fn add_action(&mut self, action: MemTableAction) {
        self.batch.push_back(action);
    }

    pub fn commit(&mut self) -> Result<()> {
        // Write them in a single call. (Better with O_DIRECT | O_SYNC, but that's unix-specific)
        let mut buf = Vec::new();
        for action in &self.batch {
            buf.extend(bincode::encode_to_vec(action, bincode::config::standard())?);
        }
        // Confirm that operations are completed by an Commit action.
        buf.extend(bincode::encode_to_vec(MemTableAction::Commit, bincode::config::standard())?);
        self.log.write_all(&buf)?;
        self.log.sync_all()?;

        // Apply changes to in-memory manifest.
        while let Some(action) = self.batch.pop_front() {
            self.memtable.execute_action(action);
        }
        Ok(())
    }

    pub fn insert(&mut self, key: Vec<u8>, update: ValueUpdate) {
        self.batch.push_back(MemTableAction::Insert((key, update)));
    }

    pub fn container(&self) -> &MemTable {
        &self.memtable
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

    pub fn reset(&mut self) -> Result<()> {
        self.memtable.clear();
        self.batch.clear();
        self.log.set_len(0)?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {

    pub fn new() -> MemTable {
        MemTable {
            container: SkipMap::new(),
            approx_size: 0,
        }
    }

    pub fn execute_action(&mut self, action: MemTableAction) {
        if let MemTableAction::Insert((key, update)) = action {
            self.insert(key, update);
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    use crate::memtable::*;
    use crate::test_util::*;

    use anyhow::{bail, ensure, Result};
    use rand::Rng;


    #[test]
    fn test_recovery() -> Result<()> {
        // Write some data. Used for verification.
        let test_dir0 = create_test_dir()?;
        let (tx, rx) = mpsc::channel();
        let thread_handle;
        {
            let test_dir = test_dir0.clone();
            thread_handle = thread::spawn(move || -> Result<()> {
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
            tx.send((MemTableAction::Insert((key, update)), false))?;
            if i % 16 == 0 {
                keeper.commit()?;
                tx.send((MemTableAction::Commit, false))?;
            }
            if i == 1000 {
                tx.send((MemTableAction::Commit, true))?;
                break;
            }
        }

        thread_handle.join().unwrap()?;

        let recovered_keeper = MemTableKeeper::recover(&test_dir0)?;
        ensure!(!keeper.is_empty(), "Memtable shouldn't be empty");
        ensure!(
            keeper == recovered_keeper,
            "Recovered memtable has inconsistent data"
        );
        Ok(())
    }
}

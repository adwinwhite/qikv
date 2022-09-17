// MANIFEST_CURRENT format :=
//  snapshot_filename
//  \n
//  log_filename
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::ops::{Deref, DerefMut};
use std::fs;
use std::fs::File;
use std::io::{Read, Write};

use crate::sstable::*;
// use crate::memtable::MemTable;

use anyhow::Result;
use bincode::{Decode, Encode};
//
const MANIFEST_CURRENT: &str = "MANIFEST_CURRENT";
const MANIFEST_SNAPSHOT_PREFIX: &str = "MANIFEST_SNAPSHOT";
const MANIFEST_LOG_PREFIX: &str = "MANIFEST_LOG";

pub struct ManifestKeeper {
    manifest: Manifest,
    log: File,
}

impl Deref for ManifestKeeper {
    type Target = Manifest;

    fn deref(&self) -> &Self::Target {
        &self.manifest
    }
}

impl DerefMut for ManifestKeeper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.manifest
    }
}

#[derive(Encode, Decode, Debug)]
enum ManifestAction {
    Add,
    Remove,
    NewId,
    NextCompact,
}


impl ManifestKeeper {
    pub fn new(store_dir: &Path) -> Result<ManifestKeeper> {
        let mut current = File::options().write(true).create_new(true).open(store_dir.join(MANIFEST_CURRENT))?;
        current.write((MANIFEST_SNAPSHOT_PREFIX.to_owned() + "_0" 
            + "\n" 
            + MANIFEST_LOG_PREFIX + "_0").as_bytes())?;
        drop(current);
        let snapshot_file = File::options().write(true).create(true).open(store_dir.join(MANIFEST_SNAPSHOT_PREFIX.to_owned() + "_0"))?;
        snapshot_file.sync_all()?;
        drop(snapshot_file);
        let log_file = File::options().append(true).create(true).open(store_dir.join(MANIFEST_LOG_PREFIX.to_owned() + "_0"))?;
        let mut keeper = ManifestKeeper {
            manifest: Manifest::new(),
            log: log_file,
        };
        keeper.snapshot(store_dir)?;
        Ok(keeper)
    }

    pub fn snapshot(&mut self, store_dir: &Path) -> Result<()> {
        // Create a new file to store snapshot.
        // Create a new empty log file.
        // Point to new snapshot and log file.
        // Delete obsolete snapshot and log.
        let mut current = File::options().read(true).write(true).open(store_dir.join(MANIFEST_CURRENT))?;
        let mut content = String::new();
        current.read_to_string(&mut content)?;
        let names: Vec<_> = content.split_whitespace().collect();
        let snapshot_num = names[0][MANIFEST_SNAPSHOT_PREFIX.len() + 1 ..].parse::<u64>()?;
        let log_num = names[1][MANIFEST_LOG_PREFIX.len() + 1 ..].parse::<u64>()?;
        let mut snapshot_file = File::options().write(true).create(true).truncate(true).open(store_dir.join(MANIFEST_SNAPSHOT_PREFIX.to_owned() + "_" + &(snapshot_num + 1).to_string()))?;
        let log_file = File::options().write(true).create(true).truncate(true).open(store_dir.join(MANIFEST_LOG_PREFIX.to_owned() + "_" + &(log_num + 1).to_string()))?;

        bincode::encode_into_std_write(&self.manifest, &mut snapshot_file, bincode::config::standard())?;
        snapshot_file.sync_all()?;
        log_file.sync_all()?;

        current.set_len(0)?;
        current.write((MANIFEST_SNAPSHOT_PREFIX.to_owned() + "_" + &snapshot_num.to_string() 
            + "\n" 
            + MANIFEST_LOG_PREFIX + "_" + &log_num.to_string()).as_bytes())?;
        current.sync_all()?;

        self.log = log_file;

        fs::remove_file(store_dir.join(names[0]))?;
        fs::remove_file(store_dir.join(names[1]))?;

        Ok(())
    }

    pub fn recover(store_dir: &Path) -> Result<ManifestKeeper> {
        // Load snapshot and then replay log.
        let mut current = File::options().read(true).write(true).open(store_dir.join(MANIFEST_CURRENT))?;
        let mut content = String::new();
        current.read_to_string(&mut content)?;
        let names: Vec<_> = content.split_whitespace().collect();
        let mut snapshot_file = File::open(store_dir.join(names[0]))?;
        let mut manifest: Manifest = bincode::decode_from_std_read(&mut snapshot_file, bincode::config::standard())?;
        let mut log_file = File::options().read(true).write(true).open(store_dir.join(names[1]))?;

        let mut buf = Vec::new();
        log_file.read_to_end(&mut buf)?; // Now seek to end.

        let mut cur = 0;
        while cur < buf.len() {
            let (action, size): (ManifestAction, usize) = bincode::decode_from_slice(&buf[cur..], bincode::config::standard())?;
            cur += size;
            match action {
                ManifestAction::NextCompact => {
                    let ((level, ), size): ((u64, ), usize) = bincode::decode_from_slice(&buf[cur..], bincode::config::standard())?;
                    cur += size;
                    manifest.next_compact_sst(level);
                },
                ManifestAction::Add => {
                    let ((sst_id, first_key, last_key,), size): ((SstId, Vec<u8>, Vec<u8>,), usize) = bincode::decode_from_slice(&buf[cur..], bincode::config::standard())?;
                    cur += size;
                    manifest.add_sst(sst_id, &first_key, &last_key);
                },
                ManifestAction::Remove => {
                    let ((sst_id, ), size): ((SstId, ), usize) = bincode::decode_from_slice(&buf[cur..], bincode::config::standard())?;
                    cur += size;
                    manifest.remove_sst(&sst_id);
                },
                ManifestAction::NewId => {
                    let ((level, ), size): ((u64, ), usize) = bincode::decode_from_slice(&buf[cur..], bincode::config::standard())?;
                    cur += size;
                    manifest.new_sst_id(level);
                }
            }
        }

        Ok(ManifestKeeper {
            manifest,
            log: log_file,
        })
    }

    pub fn next_compact(&mut self, level: u64) -> Result<Option<SstId>> {
        bincode::encode_into_std_write(ManifestAction::NextCompact, &mut self.log, bincode::config::standard())?;
        bincode::encode_into_std_write((level,), &mut self.log, bincode::config::standard())?;
        Ok(self.next_compact_sst(level))
    }

    pub fn add(&mut self, sst_id: SstId, first_key: &[u8], last_key: &[u8]) -> Result<()> {
        bincode::encode_into_std_write(ManifestAction::Add, &mut self.log, bincode::config::standard())?;
        bincode::encode_into_std_write((sst_id, first_key, last_key,), &mut self.log, bincode::config::standard())?;
        Ok(self.add_sst(sst_id, first_key, last_key))
    }

    pub fn remove(&mut self, sst_id: &SstId) -> Result<()> {
        bincode::encode_into_std_write(ManifestAction::Remove, &mut self.log, bincode::config::standard())?;
        bincode::encode_into_std_write((sst_id,), &mut self.log, bincode::config::standard())?;
        Ok(self.remove_sst(sst_id))
    }

    pub fn new_id(&mut self, level: u64) -> Result<SstId> {
        bincode::encode_into_std_write(ManifestAction::NewId, &mut self.log, bincode::config::standard())?;
        bincode::encode_into_std_write((level,), &mut self.log, bincode::config::standard())?;
        Ok(self.new_sst_id(level))
    }

}


#[derive(Encode, Decode)]
pub struct Manifest {
    new_ids: BTreeMap<u64, u64>, // largest ids for each level.
    compact_keys: BTreeMap<u64, Vec<u8>>, // next compact key in each level.
    active_ssts: BTreeMap<u64, BTreeSet<u64>>,
    sst_ranges: BTreeMap<SstId, (Vec<u8>, Vec<u8>)>,
}

impl Manifest {
    pub fn new() -> Manifest {
        Manifest {
            new_ids: BTreeMap::new(),
            compact_keys: BTreeMap::new(),
            active_ssts: BTreeMap::new(),
            sst_ranges: BTreeMap::new(),
        }
    }

    pub fn max_level(&self) -> u64 { 
        if let Some((&level, _)) = self.active_ssts.last_key_value() {
            level
        } else {
            0
        }
    }

    // level is always <= max_level.
    fn get_sst_by_key_start(&self, level: u64, key: &Vec<u8>) -> SstId {
        // Get all ssts in the level.
        // Sort them by key_start.
        // Find the one with key_start <= key < next key_start.
        let mut ids = self.get_sst_by_level(level);
        ids.sort_unstable_by(|a, b| self.sst_ranges.get(a).unwrap().0.cmp(&self.sst_ranges.get(b).unwrap().0));
        let mut iter = ids.iter().peekable();
        while let Some(id) = iter.next() {
            if let Some(next_id) = iter.peek() {
                if &self.sst_ranges.get(id).unwrap().0 <= key &&
                    &self.sst_ranges.get(next_id).unwrap().0 > key {
                    return *id;
                }
            } else {
                return *id;
            }
        }
        unreachable!()
    }

    // level is always <= max_level.
    fn next_sst_start(&self, level: u64, key: &Vec<u8>) -> Vec<u8> {
        let mut ids = self.get_sst_by_level(level);
        ids.sort_unstable_by(|a, b| self.sst_ranges.get(a).unwrap().0.cmp(&self.sst_ranges.get(b).unwrap().0));
        let mut iter = ids.iter().peekable();
        while let Some(id) = iter.next() {
            if let Some(next_id) = iter.peek() {
                if &self.sst_ranges.get(id).unwrap().0 <= key &&
                    &self.sst_ranges.get(next_id).unwrap().0 > key {
                    return self.sst_ranges.get(next_id).unwrap().0.clone();
                }
            } else {
                return self.sst_ranges.get(&ids[0]).unwrap().0.clone();
            }
        }
        unreachable!()

    }

    pub fn next_compact_sst(&mut self, level: u64) -> Option<SstId> {
        if level > self.max_level() {
            return None;
        }
        let next_start = match self.compact_keys.get(&level) {
            Some(key) => {
                self.next_sst_start(level, key)
            },
            None => {
                self.sst_ranges.first_key_value().unwrap().1.0.clone()
            }
        };
        self.compact_keys.insert(level, next_start.clone());
        Some(self.get_sst_by_key_start(level, &next_start))


        // let compact_key = self.compact_keys.entry(level)
            // .and_modify(|key| *key = self.next_sst_start(level, key))
            // .or_insert(self.sst_ranges.first_key_value().unwrap().1.0.clone());
        // Some(self.get_sst_by_key_start(level, compact_key)) // Partitioned by key starts of ssts.
    }


    pub fn get_sst_by_level(&self, level: u64) -> Vec<SstId> { 
        match self.active_ssts.get(&level) {
            Some(ids) => ids.iter().map(|id| SstId { level, id: *id, }).collect(), 
            None      => Vec::new(),
        }
    }


    pub fn add_sst(&mut self, sst_id: SstId, first_key: &[u8], last_key: &[u8]) {
        self.active_ssts.entry(sst_id.level).or_insert(BTreeSet::new()).insert(sst_id.id);
        self.sst_ranges.insert(sst_id, (first_key.to_vec(), last_key.to_vec()));
    }

    pub fn remove_sst(&mut self, sst_id: &SstId) {
        self.active_ssts.entry(sst_id.level).or_insert(BTreeSet::new()).remove(&sst_id.id);
        self.sst_ranges.remove(&sst_id);
    }

    pub fn active_sst_ids(&self) -> Vec<SstId> {
        let mut ids = Vec::new();
        for (k, v) in &self.active_ssts {
            ids.append(&mut v.iter().map(|id| SstId { level: *k, id: *id, }).collect::<Vec<SstId>>());
        }
        ids
    }

    pub fn get_sst_by_key(&self, key: &[u8]) -> Vec<SstId> {
        let mut ssts = Vec::new();
        for id in self.active_sst_ids() {
            let (start, end) = self.sst_ranges.get(&id).unwrap();
            if key >= start && key <= end {
                ssts.push(id);
            }
        }
        ssts
    }

    pub fn level_byte_size(&self, level: u64, db_dir: &Path) -> Result<u64> {
        if let Some(ids) = self.active_ssts.get(&level) {
            ids.iter().map(|id| Ok(db_dir.join(SSTABLE_DIR).join(level.to_string()).join(id.to_string()).metadata()?.len())).sum::<Result<u64, _>>()
        } else {
            Ok(0)
        }
    }

    // Get ssts in the next level that overlap with `id`.
    pub fn get_overlappings(&self, id: &SstId) -> Vec<SstId> {
        let level = id.level + 1;
        let mut overlappings = Vec::new();
        if let Some((start, end)) = self.sst_ranges.get(id) {
            if let Some(ids) = self.active_ssts.get(&level) {
                for id in ids {
                    let sst_id = SstId { level, id: *id, };
                    let (s1, e1) = self.sst_ranges.get(&sst_id).expect("The range should exist");
                    if end >= s1 && start <= e1 {
                        overlappings.push(sst_id);
                    }
                }
            }
        }
        overlappings
    }

    pub fn new_sst_id(&mut self, level: u64) -> SstId {
        let id = self.new_ids.entry(level).and_modify(|i| { *i += 1 }).or_insert(0);
        SstId {
            level,
            id: *id,
        }
    }
}

#[cfg(test)]
mod tests {
}

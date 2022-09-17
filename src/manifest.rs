use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
// use std::fs;
// use std::fs::File;
// use std::io::{Read, Write};

use crate::sstable::*;
// use crate::memtable::MemTable;

use anyhow::Result;
// use bincode::config;


#[derive(Clone)]
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

    // pub fn get_overlappings(&self, level: u64, key_start: &[u8], key_end: &[u8]) -> Vec<SstId> {
        // let mut overlappings = Vec::new();
        // if let Some(ids) = self.active_ssts.get(&level) {
            // for id in ids {
                // let sst_id = SstId { level, id: *id, };
                // let (s1, e1) = self.sst_ranges.get(&sst_id).expect("The range should exist");
                // if key_end >= s1 && key_start <= e1 {
                    // overlappings.push(sst_id);
                // }
            // }
        // }
        // overlappings
    // }

    // pub fn new_from_log() -> Manifest {}

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

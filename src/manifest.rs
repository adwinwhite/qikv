use std::collections::{BTreeMap, BTreeSet};
// use std::path::Path;
// use std::fs;
// use std::fs::File;
// use std::io::{Read, Write};

use crate::sstable::*;
// use crate::memtable::MemTable;

// use anyhow::Result;
// use bincode::config;


#[derive(Clone)]
pub struct Manifest {
    new_ids: BTreeMap<u64, u64>, // largest ids for each level.
    active_ssts: BTreeMap<u64, BTreeSet<u64>>,
    sst_ranges: BTreeMap<SstId, (Vec<u8>, Vec<u8>)>,
}

impl Manifest {
    pub fn new() -> Manifest {
        Manifest {
            new_ids: BTreeMap::new(),
            active_ssts: BTreeMap::new(),
            sst_ranges: BTreeMap::new(),
        }
    }
    pub fn max_level(&self) -> u64 { 0 }


    // Ordered by key range.
    // pub fn get_sst_by_level() -> &[SstId] { }

    // // Ordered by level.
    // pub fn get_sst_by_key(key: &[u8]) -> &[SstId] { }

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

    pub fn get_sst_by_key(&self, key: &[u8]) -> Option<SstId> {
        for id in self.active_sst_ids() {
            let (start, end) = self.sst_ranges.get(&id).unwrap();
            if key >= start && key <= end {
                return Some(id);
            }
        }
        None
    }

    pub fn get_overlappings(&self, level: u64, key_start: &[u8], key_end: &[u8]) -> Vec<SstId> {
        let mut overlappings = Vec::new();
        if let Some(ids) = self.active_ssts.get(&level) {
            for id in ids {
                let sst_id = SstId { level, id: *id, };
                let (s1, e1) = self.sst_ranges.get(&sst_id).expect("The range should exist");
                if key_end >= s1 && key_start <= e1 {
                    overlappings.push(sst_id);
                }
            }
        }
        overlappings
    }

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

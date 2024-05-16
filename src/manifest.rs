// MANIFEST_CURRENT format :=
//  snapshot_filename
//  \n
//  log_filename
//
// MANIFEST_LOG format :=
//  latest_valid_offset: u64
//  action * n
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

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
    batch: VecDeque<ManifestAction>,
    store_dir: PathBuf,
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

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub enum ManifestAction {
    Commit,
    Add((SstId, Vec<u8>, Vec<u8>)),
    Remove((SstId,)),
    NewId((u64,)),
    NextCompact((u64,)),
}

impl ManifestKeeper {
    pub fn new(store_dir: &Path) -> Result<ManifestKeeper> {
        let init_current =
            MANIFEST_SNAPSHOT_PREFIX.to_owned() + "_0" + "\n" + MANIFEST_LOG_PREFIX + "_0";
        fs::write(store_dir.join(MANIFEST_CURRENT), init_current)?;
        let snapshot_file = File::options()
            .write(true)
            .create(true)
            .open(store_dir.join(MANIFEST_SNAPSHOT_PREFIX.to_owned() + "_0"))?;
        snapshot_file.sync_all()?;
        drop(snapshot_file);
        let log_file = File::options()
            .append(true)
            .create(true)
            .open(store_dir.join(MANIFEST_LOG_PREFIX.to_owned() + "_0"))?;
        let mut keeper = ManifestKeeper {
            manifest: Manifest::new(),
            log: log_file,
            batch: VecDeque::new(),
            store_dir: store_dir.to_path_buf(),
        };
        keeper.snapshot(store_dir)?;
        Ok(keeper)
    }

    pub fn snapshot(&mut self, store_dir: &Path) -> Result<()> {
        // Create a new file to store snapshot.
        // Create a new empty log file.
        // Point to new snapshot and log file.
        // Delete obsolete snapshot and log.
        let mut current = File::options()
            .read(true)
            .write(true)
            .open(store_dir.join(MANIFEST_CURRENT))?;
        let mut content = String::new();
        current.read_to_string(&mut content)?;
        let names: Vec<_> = content.split_whitespace().collect();
        let snapshot_num = names[0][MANIFEST_SNAPSHOT_PREFIX.len() + 1..].parse::<u64>()? + 1;
        let log_num = names[1][MANIFEST_LOG_PREFIX.len() + 1..].parse::<u64>()? + 1;
        let mut snapshot_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(
                store_dir
                    .join(MANIFEST_SNAPSHOT_PREFIX.to_owned() + "_" + &snapshot_num.to_string()),
            )?;
        let log_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(store_dir.join(MANIFEST_LOG_PREFIX.to_owned() + "_" + &log_num.to_string()))?;

        bincode::encode_into_std_write(
            &self.manifest,
            &mut snapshot_file,
            bincode::config::standard(),
        )?;
        snapshot_file.sync_all()?;
        log_file.sync_all()?;

        current.set_len(0)?;
        current.write_all(
            (MANIFEST_SNAPSHOT_PREFIX.to_owned()
                + "_"
                + &snapshot_num.to_string()
                + "\n"
                + MANIFEST_LOG_PREFIX
                + "_"
                + &log_num.to_string())
                .as_bytes(),
        )?;
        current.sync_all()?;

        self.log = log_file;

        fs::remove_file(store_dir.join(names[0]))?;
        fs::remove_file(store_dir.join(names[1]))?;

        Ok(())
    }

    pub fn recover(store_dir: &Path) -> Result<ManifestKeeper> {
        // Load snapshot and then replay log.
        // String read has leading \0 bytes and I don't know why.
        // Just trim it now.
        let current = fs::read_to_string(store_dir.join(MANIFEST_CURRENT))?;
        let names: Vec<_> = current.trim_matches('\0').split_whitespace().collect();
        let mut snapshot_file = File::open(store_dir.join(names[0]))?;
        let mut manifest: Manifest =
            bincode::decode_from_std_read(&mut snapshot_file, bincode::config::standard())?;
        let mut log_file = File::options()
            .read(true)
            .write(true)
            .open(store_dir.join(names[1]))?;

        let mut buf = Vec::new();
        log_file.read_to_end(&mut buf)?; // Now seek to end.

        let mut cur = 0;
        let mut batch = VecDeque::new();
        while cur < buf.len() {
            if let Ok((action, size)) =
                bincode::decode_from_slice(&buf[cur..], bincode::config::standard())
            {
                cur += size;
                match action {
                    ManifestAction::Commit => {
                        while let Some(action) = batch.pop_front() {
                            manifest.execute_action(action);
                        }
                    }
                    _ => batch.push_back(action),
                }
            } else {
                // Half written batch.
                // Abandon them.
                log_file.set_len(cur as u64)?;
                break;
            }
        }

        // Now we have a consistent manifest.
        // Clean up obsolete SST files.
        // Ignore non-utf8 path and non-numeric path.
        fs::create_dir_all(store_dir.join(SSTABLE_DIR))?;
        for entry in fs::read_dir(store_dir.join(SSTABLE_DIR))? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // Waiting for stablization of let chains.
                if let Some(path_str) = path.to_str() {
                    if let Ok(level) = path_str.parse::<u64>() {
                        match manifest.active_ssts.get(&level) {
                            Some(ids) => {
                                for sst in fs::read_dir(
                                    store_dir.join(SSTABLE_DIR).join(level.to_string()),
                                )? {
                                    let sst = sst?;
                                    let sst_path = sst.path();
                                    if sst_path.is_file() {
                                        if let Some(sst_path_str) = sst_path.to_str() {
                                            if let Ok(id) = sst_path_str.parse::<u64>() {
                                                if !ids.contains(&id) {
                                                    fs::remove_file(sst_path)?;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            None => {
                                fs::remove_dir_all(path)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(ManifestKeeper {
            manifest,
            log: log_file,
            batch: VecDeque::new(),
            store_dir: store_dir.to_path_buf(),
        })
    }

    pub fn next_compact(&mut self, level: u64) {
        self.batch.push_back(ManifestAction::NextCompact((level,)));
    }

    pub fn add(&mut self, sst_id: SstId, first_key: &[u8], last_key: &[u8]) {
        self.batch.push_back(ManifestAction::Add((
            sst_id,
            first_key.to_vec(),
            last_key.to_vec(),
        )));
    }

    pub fn remove(&mut self, sst_id: &SstId) {
        self.batch.push_back(ManifestAction::Remove((*sst_id,)));
    }

    pub fn new_id(&mut self, level: u64) {
        self.batch.push_back(ManifestAction::NewId((level,)));
    }

    pub fn batch_start(&mut self) {
        self.batch.clear();
    }

    pub fn add_action(&mut self, action: ManifestAction) {
        self.batch.push_back(action);
    }

    pub fn commit(&mut self) -> Result<()> {
        // Write them in a single call. (Better with O_DIRECT | O_SYNC, but that's unix-specific)
        let mut buf = Vec::new();
        for action in &self.batch {
            buf.extend(bincode::encode_to_vec(action, bincode::config::standard())?);
        }
        // Confirm that operations are completed by an Commit action.
        buf.extend(bincode::encode_to_vec(ManifestAction::Commit, bincode::config::standard())?);
        self.log.write_all(&buf)?;
        self.log.sync_all()?;

        // Apply changes to in-memory manifest.
        while let Some(action) = self.batch.pop_front() {
            if let ManifestAction::Remove((sst_id,)) = action {
                match SSTable::remove(&self.store_dir, &sst_id) {
                    Ok(()) => {},
                    Err(err) => { eprintln!("Failed to remove SST file {sst_id:#?}: {err}"); },
                }
            }
            self.manifest.execute_action(action);
        }
        Ok(())
    }
}

#[derive(Encode, Decode, PartialEq, Eq)]
pub struct Manifest {
    new_ids: BTreeMap<u64, u64>,          // largest ids for each level.
    compact_keys: BTreeMap<u64, Vec<u8>>, // next compact key in each level.
    active_ssts: BTreeMap<u64, BTreeSet<u64>>,
    sst_ranges: BTreeMap<SstId, (Vec<u8>, Vec<u8>)>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
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
        ids.sort_unstable_by(|a, b| {
            self.sst_ranges
                .get(a)
                .unwrap()
                .0
                .cmp(&self.sst_ranges.get(b).unwrap().0)
        });
        let mut iter = ids.iter().peekable();
        while let Some(id) = iter.next() {
            if let Some(next_id) = iter.peek() {
                if &self.sst_ranges.get(id).unwrap().0 <= key
                    && &self.sst_ranges.get(next_id).unwrap().0 > key
                {
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
        ids.sort_unstable_by(|a, b| {
            self.sst_ranges
                .get(a)
                .unwrap()
                .0
                .cmp(&self.sst_ranges.get(b).unwrap().0)
        });
        let mut iter = ids.iter().peekable();
        while let Some(id) = iter.next() {
            if let Some(next_id) = iter.peek() {
                if &self.sst_ranges.get(id).unwrap().0 <= key
                    && &self.sst_ranges.get(next_id).unwrap().0 > key
                {
                    return self.sst_ranges.get(next_id).unwrap().0.clone();
                }
            } else {
                return self.sst_ranges.get(&ids[0]).unwrap().0.clone();
            }
        }
        unreachable!()
    }

    pub fn latest_compact_sst(&self, level: u64) -> SstId {
        assert!(level <= self.max_level());
        let latest_start = match self.compact_keys.get(&level) {
            Some(key) => key,
            None => &self.sst_ranges.first_key_value().unwrap().1 .0,
        };
        self.get_sst_by_key_start(level, latest_start)
    }

    pub fn next_compact_sst(&mut self, level: u64) -> SstId {
        assert!(level <= self.max_level());
        let next_start = match self.compact_keys.get(&level) {
            Some(key) => self.next_sst_start(level, key),
            None => self.sst_ranges.first_key_value().unwrap().1 .0.clone(),
        };
        self.compact_keys.insert(level, next_start.clone());
        self.get_sst_by_key_start(level, &next_start)

        // let compact_key = self.compact_keys.entry(level)
        // .and_modify(|key| *key = self.next_sst_start(level, key))
        // .or_insert(self.sst_ranges.first_key_value().unwrap().1.0.clone());
        // Some(self.get_sst_by_key_start(level, compact_key)) // Partitioned by key starts of ssts.
    }

    pub fn get_sst_by_level(&self, level: u64) -> Vec<SstId> {
        match self.active_ssts.get(&level) {
            Some(ids) => ids.iter().map(|id| SstId { level, id: *id }).collect(),
            None => Vec::new(),
        }
    }

    pub fn add_sst(&mut self, sst_id: SstId, first_key: &[u8], last_key: &[u8]) {
        assert!(first_key <= last_key);
        self.active_ssts
            .entry(sst_id.level)
            .or_default()
            .insert(sst_id.id);
        self.sst_ranges
            .insert(sst_id, (first_key.to_vec(), last_key.to_vec()));
    }

    pub fn remove_sst(&mut self, sst_id: &SstId) {
        self.active_ssts
            .entry(sst_id.level)
            .or_default()
            .remove(&sst_id.id);
        self.sst_ranges.remove(sst_id);
    }

    pub fn active_sst_ids(&self) -> Vec<SstId> {
        let mut ids = Vec::new();
        for (k, v) in &self.active_ssts {
            ids.append(
                &mut v
                    .iter()
                    .map(|id| SstId { level: *k, id: *id })
                    .collect::<Vec<SstId>>(),
            );
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
            ids.iter()
                .map(|id| {
                    Ok(db_dir
                        .join(SSTABLE_DIR)
                        .join(level.to_string())
                        .join(id.to_string())
                        .metadata()?
                        .len())
                })
                .sum::<Result<u64, _>>()
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
                    let sst_id = SstId { level, id: *id };
                    let (s1, e1) = self
                        .sst_ranges
                        .get(&sst_id)
                        .expect("The range should exist");
                    if end >= s1 && start <= e1 {
                        overlappings.push(sst_id);
                    }
                }
            }
        }
        overlappings
    }

    pub fn latest_sst_id(&self, level: u64) -> SstId {
        match self.new_ids.get(&level) {
            Some(&id) => SstId { level, id },
            None => SstId { level, id: 0 },
        }
    }

    pub fn new_sst_id(&mut self, level: u64) -> SstId {
        let id = self
            .new_ids
            .entry(level)
            .and_modify(|i| *i += 1)
            .or_insert(1);
        SstId { level, id: *id }
    }

    pub fn execute_action(&mut self, action: ManifestAction) {
        match action {
            ManifestAction::Commit => {}
            ManifestAction::NextCompact((level,)) => {
                self.next_compact_sst(level);
            }
            ManifestAction::Add((sst_id, first_key, last_key)) => {
                self.add_sst(sst_id, &first_key, &last_key);
            }
            ManifestAction::Remove((sst_id,)) => {
                self.remove_sst(&sst_id);
            }
            ManifestAction::NewId((level,)) => {
                self.new_sst_id(level);
            }
        }
    }

    pub fn sort(&self, sst_ids: &[SstId]) -> Vec<SstId> {
        let mut metas: Vec<_> = sst_ids
            .iter()
            .map(|sst_id| SSTMetadata {
                level: sst_id.level,
                id: sst_id.id,
                first_key: &self.sst_ranges.get(sst_id).unwrap().0,
                last_key: &self.sst_ranges.get(sst_id).unwrap().1,
            })
            .collect();
        metas.sort();
        metas
            .iter()
            .map(|m| SstId {
                level: m.level,
                id: m.id,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::*;
    use crate::test_util::*;
    use anyhow::{ensure, Result};
    use std::sync::mpsc;
    use std::thread;

    // #[test]
    // fn test_fresh_start() -> Result<()> {
    // todo!()
    // }

    #[test]
    fn test_normal_exit() -> Result<()> {
        // 1. no snapshot, just from log.
        // 2. from snapshot and log.
        //
        // How to do it?
        // Compare two keepers: one crashed, one not.
        //
        // Imitate common cases.
        let mut actions = Vec::new();
        // Add 4 level 0 sst.
        actions.push(ManifestAction::NewId((0,)));
        let key_range = get_random_key_range(16, 17);
        actions.push(ManifestAction::Add((
            SstId { level: 0, id: 0 },
            key_range.0,
            key_range.1,
        )));

        actions.push(ManifestAction::NewId((0,)));
        let key_range = get_random_key_range(16, 17);
        actions.push(ManifestAction::Add((
            SstId { level: 0, id: 1 },
            key_range.0,
            key_range.1,
        )));

        actions.push(ManifestAction::NewId((0,)));
        let key_range = get_random_key_range(16, 17);
        actions.push(ManifestAction::Add((
            SstId { level: 0, id: 2 },
            key_range.0,
            key_range.1,
        )));

        actions.push(ManifestAction::NewId((0,)));
        let key_range = get_random_key_range(16, 17);
        actions.push(ManifestAction::Add((
            SstId { level: 0, id: 3 },
            key_range.0,
            key_range.1,
        )));
        // Compact to level 1
        actions.push(ManifestAction::NextCompact((1,)));
        actions.push(ManifestAction::NewId((1,)));
        let key_range = get_random_key_range(16, 17);
        actions.push(ManifestAction::Add((
            SstId { level: 1, id: 0 },
            key_range.0,
            key_range.1,
        )));
        for i in 0..4 {
            actions.push(ManifestAction::Remove((SstId { level: 0, id: i },)));
        }

        let test_dir0 = create_test_dir()?;
        let (tx, rx) = mpsc::channel();
        {
            let test_dir = test_dir0.clone();
            thread::spawn(move || {
                let mut keeper = ManifestKeeper::new(&test_dir)?;
                for action in rx {
                    if let ManifestAction::Commit = action {
                        keeper.commit()?;
                    } else {
                        keeper.add_action(action.clone());
                    }
                }
                // Normal exit
                Result::<(), anyhow::Error>::Ok(())
            });
        }
        let test_dir1 = create_test_dir()?;
        let mut keeper = ManifestKeeper::new(&test_dir1)?;
        for action in &actions {
            tx.send(action.clone())?;
            if let ManifestAction::Commit = action {
                keeper.commit()?;
            } else {
                keeper.add_action(action.clone());
            }
        }

        // Rebuild keeper.
        let keeper0 = ManifestKeeper::recover(&test_dir0)?;
        ensure!(
            keeper0.eq(&keeper),
            "(Normal Exit) Recovered manifest is corrupted"
        );

        Ok(())
    }

    #[test]
    fn test_new_id() -> Result<()> {
        let test_dir = create_test_dir()?;
        let mut keeper = ManifestKeeper::new(&test_dir)?;

        // First compaction.
        for j in 0..4 {
            for i in 0..4 {
                keeper.batch_start();
                ensure!(
                    keeper.latest_sst_id(j) == SstId { level: j, id: i },
                    "Assigned sst id is wrong. it should be level={j}, id={i}"
                );
                keeper.new_id(j);
                keeper.commit()?;
            }
        }
        Ok(())
    }

    // #[test]
    // fn test_flush_random_kill() -> Result<()> {
        // // Kill randomly and test data integrity.
        // todo!();
        // // Ok(())
    // }

    // #[test]
    // fn test_compact_random_kill() -> Result<()> {
        // // Kill randomly and test data integrity.
        // todo!();
    // }

    // #[test]
    // fn test_cleanup() -> Result<()> {
    // todo!()
    // }
}

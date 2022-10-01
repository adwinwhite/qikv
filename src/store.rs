// For simplcity, we flush memtable if it contains more than certain number of items.
use crate::manifest::*;
use crate::memtable::*;
use crate::sstable::*;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use growable_bloom_filter::GrowableBloom;
use skiplist::skipmap;

pub struct Store {
    memtable: MemTableKeeper,
    manifest: ManifestKeeper,
    bloom: GrowableBloom,
    dir: PathBuf,
}

impl Store {
    pub fn new(store_dir: &Path) -> Result<Store> {
        fs::create_dir_all(store_dir)?;
        Ok(Store {
            memtable: MemTableKeeper::new(store_dir)?,
            manifest: ManifestKeeper::new(store_dir)?,
            bloom: GrowableBloom::new(0.05, 4096),
            dir: store_dir.to_path_buf(),
        })
    }

    pub fn recover(store_dir: &Path) -> Result<Store> {
        todo!()
    }

    pub fn workdir(&self) -> PathBuf {
        self.dir.clone()
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.bloom.insert(&key);
        self.memtable.insert(key, ValueUpdate::Value(value));
        self.memtable.commit()?;
        self.checked_flush()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if !self.bloom.contains(key) {
            return None;
        }

        // The key possibly exists.
        // Check memtable and then sstables.
        match self.memtable.get(&key.to_vec()) {
            Some(update) => match update {
                ValueUpdate::Value(v) => Some(v.clone()),
                ValueUpdate::Tombstone => None,
            },
            None => {
                let group = SSTGroup::new(&self.manifest.get_sst_by_key(key), &self.dir)
                    .expect("Failed to load SSTable");
                match group.get(key) {
                    Some(ValueUpdate::Tombstone) | None => None,
                    Some(ValueUpdate::Value(v)) => Some(v),
                }
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.memtable.insert(key.to_vec(), ValueUpdate::Tombstone);
        self.checked_flush()?;
        Ok(())
    }

    fn checked_flush(&mut self) -> Result<bool> {
        // Check whether to flush to level 0 sstable.
        if self.memtable.should_flush() {
            SSTable::flush_to_level0(&mut self.memtable, &self.dir, &mut self.manifest)?;
            self.try_compact()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    // Check whether the number of level 0 exceeds 4.
    // Check whether the size of level 1 execeeds 10^1 MB.
    // Check whether the size of level 2 execeeds 10^2 MB.
    // ...
    // Rotate the random chosen key to span whole key space.
    fn try_compact(&mut self) -> Result<()> {
        self.try_level_compact(0)
    }

    fn try_level_compact(&mut self, level: u64) -> Result<()> {
        let level_ids = self.manifest.get_sst_by_level(level);
        if level_ids.is_empty() {
            Ok(())
        } else {
            if level == 0 {
                if level_ids.len() >= 4 {
                    self.manifest.batch_start();
                    let mut overlappings = Vec::new();
                    for id in &level_ids {
                        overlappings.extend(self.manifest.get_overlappings(id));
                    }
                    overlappings.extend(level_ids);
                    SSTGroup::new(&overlappings, &self.dir)?.compact(
                        1,
                        &self.dir,
                        &mut self.manifest,
                    )?;
                    self.try_level_compact(1)?;
                }
            } else if self.manifest.level_byte_size(level, &self.dir)?
                > u64::pow(10, level as u32) * u64::pow(2, 20)
            {
                self.manifest.batch_start();
                let rotate_sst = self.manifest.latest_compact_sst(level); // level is smaller than max_level.
                self.manifest.next_compact(level);
                let mut overlappings = Vec::new();
                overlappings.extend(self.manifest.get_overlappings(&rotate_sst));
                overlappings.push(rotate_sst);
                SSTGroup::new(&overlappings, &self.dir)?.compact(
                    level + 1,
                    &self.dir,
                    &mut self.manifest,
                )?;
                self.try_level_compact(level + 1)?;
            }

            Ok(())
        }
    }

    // pub fn iter_range(&self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) -> StoreIter<'a> {
    // }

    // pub fn iter(&self) -> Result<StoreIter> {
    // StoreIter::new(self)
    // }
}

// Transform references into values.
pub struct MemTableIter<'a> {
    iter: skipmap::Iter<'a, Vec<u8>, ValueUpdate>,
}

impl<'a> Iterator for MemTableIter<'a> {
    type Item = (Vec<u8>, ValueUpdate);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k.clone(), v.clone()))
    }
}

// pub struct StoreIter {
// ssts: SSTGroup,
// whole_iter: GeneralCombinedIter,
// deleted: HashSet<Vec<u8>>,
// }

// impl StoreIter {
// pub fn new(store: &Store) -> Result<StoreIter> {
// let sst_ids = store.manifest.active_sst_ids();
// let ssts = SSTGroup::new(&sst_ids[..], &store.workdir())?;
// let iters: Vec<BoxedIter> = Vec::new();
// iters.push(Box::new(ssts.iter()));
// iters.push(Box::new(MemTableIter { iter: store.memtable.iter(), }));
// Ok(StoreIter {
// ssts,
// whole_iter: GeneralCombinedIter::new(iters)?,
// deleted: HashSet::new(),
// })
// }
// }

// impl Iterator for StoreIter {
// type Item = (Vec<u8>, Vec<u8>);

// fn next(&mut self) -> Option<Self::Item> {
// loop {
// match self.whole_iter.next() {
// Some((k, ValueUpdate::Value(v))) => {
// if !self.deleted.contains(&k) {
// return Some((k.clone(), v.clone()));
// }
// },
// Some((k, ValueUpdate::Tombstone)) => {
// self.deleted.insert(k.clone());
// },
// None => return None,
// }
// }
// }
// }

#[cfg(test)]
mod tests {
    use crate::store::*;
    use crate::test_util::*;
    use std::collections::BTreeMap;

    use anyhow::{anyhow, bail, ensure, Result};
    use rand::Rng;

    #[test]
    fn test_complete() -> Result<()> {
        // Write large amount of data and then read and compare.
        let test_store_dir = create_test_dir()?;
        let mut store = Store::new(&test_store_dir)?;
        let mut good_map = BTreeMap::new();
        for _ in 0..4096 {
            let key = get_random_bytes(1, 4);
            if rand::thread_rng().gen::<f64>() > 0.2 {
                let value = get_random_bytes(1, 2);
                good_map.insert(key.clone(), value.clone());
                store.insert(key, value)?;
            } else {
                good_map.remove(&key);
                store.remove(&key)?;
            }
        }

        for (i, (k, v)) in good_map.iter().enumerate() {
            if &store
                .get(k)
                .ok_or_else(|| anyhow!("Store is missing {i}th pair"))?
                != v
            {
                bail!("Store has incorrect pair");
            }
        }

        // let sst_ids = store.manifest.active_sst_ids();
        // let mut sstables = sst_ids.iter().map(|id| SSTable::load_by_id(&id, &test_store_dir)).collect::<Result<Vec<_>>>()?;
        // sstables.sort();
        // let whole_iter = SSTable::iter_combined(&sstables[..])?;
        // let store_iter = Store::iter(whole_iter);

        // ensure!(good_map.len() != 0, "The Btree map is empty");
        // ensure!(store_iter.count() != 0, "The store iterator is empty");
        // let compared_iter = good_map.iter().zip(store_iter);
        // for ((gk, gv), (dk, dv)) in compared_iter {
        // storeg!(gk);
        // storeg!(dk);
        // storeg!(gv);
        // storeg!(dv);
        // }
        // if !good_map.iter().eq_by(store_iter, |(gk, gv), (dk, dv)| storeg!(gk) == storeg!(&dk) && storeg!(gv) == storeg!(&dv)) {
        // bail!("Database's data is inconsistent with btree map");
        // }

        Ok(())
    }

    #[test]
    fn check_sst_size() -> Result<()> {
        // Chunk write and delete
        // Until it reach certain amount/level. 4MB + 10MB.
        // Check sst file sizes
        let test_store_dir = create_test_dir()?;
        let mut store = Store::new(&test_store_dir)?;
        // 16MB = 2^24 bit.
        for _ in 0..usize::pow(2, 13) {
            let key = get_random_bytes(512, 513);
            if rand::thread_rng().gen::<f64>() > 0.2 {
                let value = get_random_bytes(512, 513);
                store.insert(key, value)?;
            } else {
                store.remove(&key)?;
            }
        }
        dbg!(store.manifest.active_sst_ids());
        if dbg!(store.manifest.max_level()) != 2 {
            bail!(dbg!("Max level of SSTables is not correct"));
        }
        Ok(())
    }
}

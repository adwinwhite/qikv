// For simplcity, we flush memtable if it contains more than certain number of items.
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use crate::memtable::*;
use crate::manifest::*;
use crate::sstable::*;

use growable_bloom_filter::GrowableBloom;
use anyhow::Result;
use skiplist::skipmap;

pub const MEMTABLE_FLUSH_THRESHOLD: usize = 1024;

pub struct DB {
    memtable: MemTable,
    manifest: Manifest,
    bloom: GrowableBloom,
    dir: PathBuf,
}

impl DB {
    pub fn new(db_dir: &Path) -> DB {
        DB {
            memtable: MemTable::new_empty(),
            manifest: Manifest::new(),
            bloom: GrowableBloom::new(0.05, 4096),
            dir: db_dir.to_path_buf(),
        }
    }

    pub fn workdir(&self) -> PathBuf {
        self.dir.clone()
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.bloom.insert(&key);
        self.memtable.insert(key, ValueUpdate::Value(value));
        self.checked_flush()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if !self.bloom.contains(&key) {
            return None;
        }

        if let Some(sst_id) = self.manifest.get_sst_by_key(&key) {
            let sstable = SSTable::load_by_id(&sst_id, &self.dir).expect("Failed to load SSTable");
            match sstable.get(&key) {
                Some(ValueUpdate::Tombstone) | None => None,
                Some(ValueUpdate::Value(v))  => Some(v),
            }
        } else {
            None
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.memtable.insert(key.to_vec(), ValueUpdate::Tombstone);
        self.checked_flush()?;
        Ok(())
    }

    fn checked_flush(&mut self) -> Result<bool> {
        // Check whether to flush to level 0 sstable.
        if self.memtable.len() >= MEMTABLE_FLUSH_THRESHOLD {
            let sst_id = self.manifest.new_sst_id(0);
            SSTable::flush_to_level0(&self.memtable, &self.dir, sst_id.id)?;
            self.manifest.add_sst(sst_id, self.memtable.front().unwrap().0, self.memtable.back().unwrap().0);
            self.memtable.clear();
            self.try_compact();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_compact(&mut self) {}

    // pub fn iter_range(&self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) -> DBIter<'a> {
    // }

    pub fn iter(&self) -> Result<DBIter> {
        DBIter::new(self)
    }
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


pub struct DBIter {
    ssts: SSTGroup,
    whole_iter: GeneralCombinedIter,
    deleted: HashSet<Vec<u8>>,
}

impl DBIter {
    pub fn new(db: &DB) -> Result<DBIter> {
        let sst_ids = db.manifest.active_sst_ids();
        let ssts = SSTGroup::new(&sst_ids[..], &db.workdir())?;
        let iters: Vec<BoxedIter> = Vec::new();
        iters.push(Box::new(ssts.iter()));
        iters.push(Box::new(MemTableIter { iter: db.memtable.iter(), }));
        Ok(DBIter {
            ssts,
            whole_iter: GeneralCombinedIter::new(iters)?,
            deleted: HashSet::new(),
        })
    }
}

impl Iterator for DBIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.whole_iter.next() {
                Some((k, ValueUpdate::Value(v))) => {
                    if !self.deleted.contains(&k) {
                        return Some((k.clone(), v.clone()));
                    }
                },
                Some((k, ValueUpdate::Tombstone)) => {
                    self.deleted.insert(k.clone());
                },
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use crate::db::*;
    use crate::test_util::*;

    use anyhow::{Result, bail, ensure};
    use rand::Rng;

    #[test]
    fn test_complete() -> Result<()> {
        // Write large amount of data and then read and compare.
        let test_db_dir = create_test_dir()?;
        let mut db = DB::new(&test_db_dir);
        let mut good_map = BTreeMap::new();
        for _ in 0..16 {
            let key = get_random_bytes(1, 4);
            if rand::thread_rng().gen::<f64>() > 0.5 {
                let value = get_random_bytes(1, 2);
                good_map.insert(key.clone(), value.clone());
                db.insert(key, value)?;
            } else {
                good_map.remove(&key);
                db.remove(&key)?;
            }
        }

        let sst_ids = db.manifest.active_sst_ids();
        let mut sstables = sst_ids.iter().map(|id| SSTable::load_by_id(&id, &test_db_dir)).collect::<Result<Vec<_>>>()?;
        sstables.sort();
        let whole_iter = SSTable::iter_combined(&sstables[..])?;
        let db_iter = DB::iter(whole_iter);

        ensure!(good_map.len() != 0, "The Btree map is empty");
        ensure!(db_iter.count() != 0, "The db iterator is empty");
        // let compared_iter = good_map.iter().zip(db_iter);
        // for ((gk, gv), (dk, dv)) in compared_iter {
            // dbg!(gk);
            // dbg!(dk);
            // dbg!(gv);
            // dbg!(dv);
        // }
        // if !good_map.iter().eq_by(db_iter, |(gk, gv), (dk, dv)| dbg!(gk) == dbg!(&dk) && dbg!(gv) == dbg!(&dv)) {
            // bail!("Database's data is inconsistent with btree map");
        // }



        Ok(())
    }
}

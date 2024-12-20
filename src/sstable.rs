// Use a very simple format.
// Since main purpose of SStable is to speed up query access, the only additional data we store is sparse index.
// [ Record * N ]
// [ Index * M ]
// [ Size of index ]
//
// Index format :=
//      bincode::serialize(map<key, offset>)
use core::iter::{Iterator, Peekable};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::manifest::*;
use crate::memtable::{MemTable, MemTableKeeper, ValueUpdate};

use anyhow::{anyhow, ensure, Result};
use bincode::{config, Decode, Encode};
use ouroboros::self_referencing;

pub const SSTABLE_DIR: &str = "SST";
pub const SPARSE_INDEX_INTERVAL: u64 = 16;
pub const SSTABLE_FILE_SIZE: u64 = u64::pow(2, 21);

pub type SparseIndex = BTreeMap<Vec<u8>, usize>;
pub type BoxedIter = Box<dyn Iterator<Item = (Vec<u8>, ValueUpdate)>>;

#[derive(Encode, Decode, PartialEq, Eq, Copy, Clone, Debug)]
pub struct SstId {
    pub level: u64,
    pub id: u64,
}

// Ordering by create time.
impl Ord for SstId {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self.level.cmp(&other.level);
        match order {
            Ordering::Equal => other.id.cmp(&self.id),
            _ => order,
        }
    }
}

impl PartialOrd for SstId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SstId {
    pub fn create_file(&self, db_dir: &Path) -> Result<File> {
        let sst_dir = db_dir.join(SSTABLE_DIR).join(self.level.to_string());
        fs::create_dir_all(&sst_dir)?;
        let sst_path = sst_dir.join(self.id.to_string());
        Ok(File::options().write(true).create(true).open(sst_path)?)
    }
}

// Used for sorting.
#[derive(PartialEq, Eq)]
pub struct SSTMetadata<'a> {
    pub level: u64,
    pub id: u64,
    pub first_key: &'a [u8],
    pub last_key: &'a [u8],
}

// For level 0, ordered by create time.
// For level >= 1, Ordered by level and first key and last key.
impl Ord for SSTMetadata<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.level == 0 && other.level == 0 {
            other.id.cmp(&self.id)
        } else {
            let level_cmp = self.level.cmp(&other.level);
            match level_cmp {
                Ordering::Equal => {
                    let first_key_cmp = self.first_key.cmp(other.first_key);
                    match first_key_cmp {
                        Ordering::Equal => self.last_key.cmp(other.last_key),
                        _ => first_key_cmp,
                    }
                }
                _ => level_cmp,
            }
        }
    }
}

impl PartialOrd for SSTMetadata<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// In-memory SSTable used for query and compaction.
#[derive(PartialEq, Eq, Clone)]
pub struct SSTable {
    buf: Vec<u8>,       // Store kv pairs only.
    index: SparseIndex, // Sparse index: key -> offset
    id: SstId,          // Used for sorting.
}

// For level 0, ordered by create time.
// For level >= 1, Ordered by level and first key and last key.
impl Ord for SSTable {
    fn cmp(&self, other: &Self) -> Ordering {
        self.metadata().cmp(&other.metadata())
    }
}

impl PartialOrd for SSTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SSTable {
    pub fn get_id(&self) -> &SstId {
        &self.id
    }
    // Load SSTable from disk.
    // SSTable is named as db_dir/SSTABLE_DIR/level/id.
    pub fn load_by_id(sst_id: &SstId, db_dir: &Path) -> Result<SSTable> {
        dbg!(format!("load sst by id = {sst_id:#?}"));
        let sst_path = db_dir
            .join(SSTABLE_DIR)
            .join(sst_id.level.to_string())
            .join(sst_id.id.to_string());
        let mut file = File::open(sst_path)?;

        // Read index size and then index.
        let index_size_offset = file.seek(SeekFrom::End(-8))?;
        let mut index_size_buf = [0_u8; 8];
        file.read_exact(&mut index_size_buf)?;
        let index_size = u64::from_be_bytes(index_size_buf);
        let mut index_buf = vec![0_u8; index_size as usize];
        let index_offset = index_size_offset - index_size;
        file.seek(SeekFrom::Start(index_offset))?;
        file.read_exact(&mut index_buf)?;
        let index: SparseIndex = bincode::decode_from_slice(&index_buf[..], config::standard())?.0;

        let mut record_buf = vec![0_u8; index_offset as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut record_buf)?;
        Ok(SSTable {
            buf: record_buf,
            index,
            id: *sst_id,
        })
    }

    pub fn remove(store_dir: &Path, sst_id: &SstId) -> Result<()> {
        fs::remove_file(
            store_dir
                .join(SSTABLE_DIR)
                .join(sst_id.level.to_string())
                .join(sst_id.id.to_string()),
        )?;
        Ok(())
    }

    // TODO: use chained iterator for level >= 1. Will greatly reduce the number of iterators thus
    // comparision.
    // pub fn iter_combined(sstables: &[SSTable]) -> Result<CombinedIter> {
    // // Sort sst_ids by create time.
    // ensure!(
    // sstables.is_sorted(),
    // "Input sstables are not sorted in iter_combined()"
    // );
    // Ok(CombinedIter {
    // iter_list: sstables.iter().map(|s| s.iter().peekable()).collect(),
    // previous_key: Vec::new(),
    // })
    // }

    fn flush_to_level0_without_manifest(memtable: &MemTable, db_dir: &Path, id: u64) -> Result<()> {
        // Flush memtable to bytes by chunks(records).
        // And generate sparse index.
        // Write to disk.
        ensure!(!memtable.is_empty(), "Tried to flush empty memtable");

        let sst_dir = db_dir.join(SSTABLE_DIR).join("0");
        fs::create_dir_all(&sst_dir)?;
        let sst_path = sst_dir.join(id.to_string());
        let mut file = File::options().write(true).create(true).open(sst_path)?;

        let mut index = SparseIndex::new();
        index.insert(memtable.front().unwrap().0.clone(), 0);
        let mut offset: usize = 0;
        let mut previous_size = 0;
        for (i, pair) in memtable.iter().enumerate() {
            if i as u64 % SPARSE_INDEX_INTERVAL == 0 {
                index.insert(pair.0.clone(), offset);
            }

            let encoded = bincode::encode_to_vec(pair, config::standard())?;
            file.write_all(&encoded)?;
            offset += encoded.len();
            previous_size = encoded.len();
        }

        // Add the last key to index.
        index.insert(memtable.back().unwrap().0.clone(), offset - previous_size);

        // Write sparse index.
        let encoded = bincode::encode_to_vec(&index, config::standard())?;
        file.write_all(&encoded)?;
        file.write_all(&u64::to_be_bytes(encoded.len() as u64))?;
        file.sync_all()?;

        Ok(())
    }

    pub fn flush_to_level0(
        memtable: &mut MemTableKeeper,
        db_dir: &Path,
        manifest: &mut ManifestKeeper,
    ) -> Result<SstId> {
        manifest.batch_start();
        let sst_id = manifest.latest_sst_id(0);
        dbg!(format!("Flush memtable to sst {sst_id:#?}"));
        manifest.new_id(0);

        Self::flush_to_level0_without_manifest(memtable.container(), db_dir, sst_id.id)?;

        // Add new sst to manifest and commit to disk.
        manifest.add(
            sst_id,
            memtable.front().unwrap().0,
            memtable.back().unwrap().0,
        );
        manifest.commit()?;
        memtable.reset()?;
        Ok(sst_id)
    }

    pub fn metadata(&self) -> SSTMetadata {
        SSTMetadata {
            level: self.id.level,
            id: self.id.id,
            first_key: self.index.first_key_value().unwrap().0, // index is granteed to be non-empty.
            last_key: self.index.last_key_value().unwrap().0,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<ValueUpdate>> {
        // Query sparse index to find the left iterator where left <= key < right.
        //
        // Manifest ensures that key is in the range of this SSTable.

        let mut index_iter = self.index.iter().peekable();
        let mut offset = 0;
        let mut offset_end = self.buf.len();
        while let Some((k, v)) = index_iter.next() {
            if let Some(&(next_k, next_v)) = index_iter.peek() {
                if &k[..] <= key && key < next_k {
                    offset = *v;
                    offset_end = *next_v;
                    break;
                }
            } else if &k[..] <= key {
                offset = *v;
                break;
            }
        }

        // Iterate from offset.
        let mut iter = self.iter_range(offset, offset_end);
        let wrapped_kv = iter.try_find(|wrapped_kv| match wrapped_kv {
            Ok((k, _,)) => Ok(k == key),
            Err(_) => Err(anyhow!("Failed to decode entry in SSTable"))
        });
        wrapped_kv.map(|contained_kv| contained_kv.map(|wrapped| wrapped.unwrap().1))
    }

    pub fn iter(&self) -> SSTableIter {
        self.iter_at(0)
    }

    fn iter_at(&self, start: usize) -> SSTableIter<'_> {
        SSTableIter {
            buf: &self.buf,
            cur: start,
            end: self.buf.len(),
            done: false,
        }
    }

    fn iter_range(&self, start: usize, end: usize) -> SSTableIter<'_> {
        SSTableIter {
            buf: &self.buf,
            cur: start,
            end,
            done: false,
        }
    }
}

pub struct SSTableIter<'a> {
    buf: &'a Vec<u8>,
    cur: usize,
    end: usize, // to support range
    done: bool,
}

// Maybe we should use compression on the whole content.
// And save encoding/decoding here.
// Access bytes directly like in log.
impl<'a> Iterator for SSTableIter<'a> {
    type Item = Result<(Vec<u8>, ValueUpdate)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.end {
            self.done = true;
        }

        if self.cur >= self.buf.len() {
            self.done = true;
        }
        if self.done {
            return None;
        }

        let decoded = bincode::decode_from_slice(&self.buf[self.cur..], config::standard());
        match decoded {
            Ok((pair, size)) => {
                self.cur += size;
                Some(Ok(pair))
            }
            Err(err) => {
                self.done = true;
                Some(Err(anyhow::Error::new(err)))
            }
        }
    }
}

pub struct SSTLevelGroup {
    ids: Vec<SstId>,
    store_dir: PathBuf,
}

impl SSTLevelGroup {
    pub fn new(
        level: u64,
        ids: &[u64],
        store_dir: &Path,
        manifest: &Manifest,
    ) -> Result<SSTLevelGroup> {
        assert!(!ids.is_empty());
        assert!(level >= 1);
        let ids = manifest.sort(
            &ids.iter()
                .map(|&id| SstId { level, id })
                .collect::<Vec<_>>(),
        );
        Ok(SSTLevelGroup {
            ids,
            store_dir: store_dir.to_path_buf(),
        })
    }

    pub fn iter(&self) -> SSTLevelGroupIter {
        SSTLevelGroupIter {
            id_iter: self.ids.iter(),
            store_dir: &self.store_dir,
            sst_iter: None,
            done: false,
        }
    }
}

#[self_referencing]
struct OwnedSSTIter {
    sstable: SSTable,
    #[borrows(sstable)]
    #[covariant]
    table_iter: SSTableIter<'this>,
}

pub struct SSTLevelGroupIter<'a> {
    id_iter: std::slice::Iter<'a, SstId>,
    store_dir: &'a Path,
    sst_iter: Option<OwnedSSTIter>,
    done: bool,
}

impl<'a> Iterator for SSTLevelGroupIter<'a> {
    type Item = Result<(Vec<u8>, ValueUpdate)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            if let Some(sst_iter) = &mut self.sst_iter {
                if let Some(kv) = sst_iter.with_table_iter_mut(|sst_iter| sst_iter.next()) {
                    return Some(kv);
                } else {
                    self.sst_iter = None;
                }
            } else if let Some(id) = self.id_iter.next() {
                let wrapped_sst = SSTable::load_by_id(id, self.store_dir);
                match wrapped_sst {
                    Ok(sst) => {
                        self.sst_iter = Some(
                            OwnedSSTIterBuilder {
                                sstable: sst,
                                table_iter_builder: |sstable: &SSTable| sstable.iter(),
                            }
                            .build(),
                        );
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                };
            } else {
                self.done = true;
                return None;
            }
        }
    }
}

// Owner of a group of sstables.
// When iterating, order of sstables is priority.
// The smaller the higher.
pub struct SSTGroup {
    sstables: Vec<SSTable>,
}

impl SSTGroup {
    pub fn new(sst_ids: &[SstId], store_dir: &Path) -> Result<SSTGroup> {
        let mut sstables = sst_ids
            .iter()
            .map(|id| SSTable::load_by_id(id, store_dir))
            .collect::<Result<Vec<_>>>()?;
        sstables.sort();
        Ok(SSTGroup { sstables })
    }

    // Return the first found value which is also the latest value.
    pub fn get(&self, key: &[u8]) -> Result<Option<ValueUpdate>> {
        for s in &self.sstables {
            if let Some(update) = s.get(key)? {
                return Ok(Some(update));
            }
        }
        Ok(None)
    }

    pub fn iter(&self) -> SSTGroupIter {
        SSTGroupIter {
            iter_list: self.sstables.iter().map(|s| s.iter().peekable()).collect(),
            previous_key: Vec::new(),
        }
    }

    pub fn compact(
        &mut self,
        dest_level: u64,
        db_dir: &Path,
        manifest: &mut ManifestKeeper,
    ) -> Result<()> {
        //  Requires: SSTables are ordered by timestamp. Younger ones are at the beginning.
        //
        // Open all iterators.
        // Compare with last key, ignore duplicate.
        // Produce a single minimum key (young key preferred)
        // Collect current items and filter out None.
        //
        // Prepare the dest file.
        let ids = self.sstables.iter().map(|s| s.get_id()).collect::<Vec<_>>();
        dbg!(format!("Compact ssts {ids:#?}"));
        let mut sst_id = manifest.latest_sst_id(dest_level);
        manifest.new_id(dest_level);
        let mut file = sst_id.create_file(db_dir)?;

        let mut index = SparseIndex::new();

        let mut num_count = 0;
        let mut offset = 0;
        let mut previous_size = 0;
        let mut previous_key = Vec::new();
        let should_purge_tombstone = dest_level >= manifest.max_level();

        for wrapped_kv in self.iter() {
            let (k, v) = wrapped_kv?;
            if v == ValueUpdate::Tombstone && should_purge_tombstone {
                continue;
            }
            let encoded = bincode::encode_to_vec((&k, &v), config::standard())?;
            // Check whether we should write to a new sstable file.
            if offset + encoded.len() > SSTABLE_FILE_SIZE as usize {
                // Write sparse index.
                index.insert(previous_key, offset - previous_size);
                let encoded = bincode::encode_to_vec(&index, config::standard())?;
                file.write_all(&encoded)?;
                file.write_all(&u64::to_be_bytes(encoded.len() as u64))?;
                file.sync_all()?;
                // Add it to manifest.
                manifest.add(
                    sst_id,
                    index.first_key_value().unwrap().0,
                    index.last_key_value().unwrap().0,
                );
                //
                // Create a new sstable file.
                // Reset per file variables.
                sst_id = SstId {
                    level: dest_level,
                    id: sst_id.id + 1,
                };
                manifest.new_id(dest_level);
                file = sst_id.create_file(db_dir)?;
                index = SparseIndex::new();
                num_count = 0;
                offset = 0;
            }
            file.write_all(&encoded)?;
            if num_count % SPARSE_INDEX_INTERVAL == 0 {
                index.insert(k.clone(), offset);
            }
            num_count += 1;
            offset += encoded.len();
            previous_size = encoded.len();
            previous_key = k.clone();
        }

        // Add the last key to index.
        index.insert(previous_key, offset - previous_size);

        // Write sparse index.
        let encoded = bincode::encode_to_vec(&index, config::standard())?;
        file.write_all(&encoded)?;
        file.write_all(&u64::to_be_bytes(encoded.len() as u64))?;
        file.sync_all()?;
        // Add it to manifest.
        manifest.add(
            sst_id,
            index.first_key_value().unwrap().0,
            index.last_key_value().unwrap().0,
        );

        // Finishing compaction.
        manifest.commit()?;

        Ok(())
    }
}

pub struct SSTGroupIter<'a> {
    iter_list: Vec<Peekable<SSTableIter<'a>>>,
    previous_key: Vec<u8>,
}

impl<'a> Iterator for SSTGroupIter<'a> {
    type Item = Result<(Vec<u8>, ValueUpdate)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let min_index = {
                let (kvs, errs): (Vec<_>, Vec<_>) = self
                    .iter_list
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(i, it)| it.peek().map(|peeked| (i, peeked)))
                    .partition(|(_, peeked)| peeked.is_ok());

                if !errs.is_empty() {
                    return Some(Err(anyhow!("Failed to decode entry in SSTable")));
                }
                kvs.iter()
                    .min_by_key(|(_, res)| &res.as_ref().unwrap().0)
                    .map(|(i, _)| *i)
            };
            if let Some(i) = min_index {
                let item = self.iter_list[i].next().unwrap(); // Have peeked.
                match item {
                    Ok((k, v)) => {
                        if self.previous_key != k {
                            self.previous_key = k.clone();
                            return Some(Ok((k, v)));
                        }
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                }
            } else {
                break;
            }
        }
        None
    }
}

// pub struct CombinedIter<'a> {
// iter_list: Vec<Peekable<SSTableIter<'a>>>,
// previous_key: Vec<u8>,
// }

// impl<'a> Iterator for CombinedIter<'a> {
// type Item = (Vec<u8>, ValueUpdate);

// fn next(&mut self) -> Option<Self::Item> {
// loop {
// let min_index = {
// let items = self
// .iter_list
// .iter_mut()
// .enumerate()
// .filter_map(|(i, it)| it.peek().map(|peeked| (i, peeked)));
// items.min_by_key(|(_, (k, _))| k).map(|(i, _)| i)
// };
// if let Some(i) = min_index {
// let (k, v) = self.iter_list[i].next().unwrap();
// if self.previous_key != k {
// self.previous_key = k.clone();
// return Some((k, v));
// }
// } else {
// break;
// }
// }
// None
// }
// }

pub struct GeneralCombinedIter {
    iter_list: Vec<Peekable<BoxedIter>>,
    previous_key: Vec<u8>,
}

impl GeneralCombinedIter {
    pub fn new(iters: Vec<BoxedIter>) -> Result<GeneralCombinedIter> {
        Ok(GeneralCombinedIter {
            iter_list: iters.into_iter().map(|it| it.peekable()).collect(),
            previous_key: Vec::new(),
        })
    }
}

impl Iterator for GeneralCombinedIter {
    type Item = (Vec<u8>, ValueUpdate);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let min_index = {
                let items = self
                    .iter_list
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(i, it)| it.peek().map(|peeked| (i, peeked)));
                items.min_by_key(|(_, (k, _))| k).map(|(i, _)| i)
            };
            if let Some(i) = min_index {
                let (k, v) = self.iter_list[i].next().unwrap();
                if self.previous_key != k {
                    self.previous_key = k.clone();
                    return Some((k, v));
                }
            } else {
                break;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {

    use crate::manifest::*;
    use crate::memtable::ValueUpdate;
    use crate::memtable::*;
    use crate::sstable::*;
    use crate::test_util::*;

    use anyhow::{anyhow, bail, Result};
    use rand::Rng;

    fn new_random_memtable() -> MemTable {
        let mut memtable = MemTable::new();
        // 512
        for _ in 0..512 {
            // 10
            let key = get_random_bytes(1, 10);
            let update = if rand::thread_rng().gen::<f64>() > 0.5 {
                ValueUpdate::Tombstone
            } else {
                // 10
                ValueUpdate::Value(get_random_bytes(1, usize::pow(2, 10)))
            };
            memtable.insert(key, update);
        }
        memtable
    }

    // Log will create a new log file in an empty log dir.
    #[test]
    fn write_flush_read() -> Result<()> {
        // Prepare data to flush.
        let memtable = new_random_memtable();

        // Flush memtable to level 0 SStable file.
        let test_dir_path = create_test_dir()?;
        SSTable::flush_to_level0_without_manifest(&memtable, &test_dir_path, 0)?;

        // Load SStable file and check data.
        let sst_id = SstId { level: 0, id: 0 };
        let sst = SSTable::load_by_id(&sst_id, &test_dir_path)?;
        if !sst
            .iter()
            .map(|wrapped| wrapped.unwrap())
            .eq_by(memtable.iter(), |(sk, sv), (mk, mv)| &sk == mk && &sv == mv)
        {
            bail!("Loaded SST file has different iterator than MemTable's: \nSSTable={:#?}\nMemTable={:#?}", sst.iter().collect::<Vec<_>>(), memtable.iter().collect::<Vec<_>>());
        }

        // Compare using SSTable::get().
        for (k, v) in memtable.iter() {
            if &sst
                .get(k)?
                .ok_or_else(|| anyhow!("No requested key in SSTable according to SSTable::get()"))?
                != v
            {
                bail!("Some pair is missing in the loaded SST file according to SSTable::get()");
            }
        }
        Ok(())
    }

    #[test]
    fn test_lazy_iter() -> Result<()> {
        Ok(())
    }

    // #[test]
    // fn test_combined_iterator() -> Result<()> {
        // // Create a whole memtable and several partitioned memtables to produce sstables.
        // let mut whole = MemTable::new();

        // let test_dir_path = create_test_dir()?;
        // for i in 0..16 {
            // let memtable = new_random_memtable();
            // for (k, v) in memtable.iter() {
                // whole.insert(k.to_vec(), v.clone());
            // }

            // SSTable::flush_to_level0_without_manifest(&memtable, &test_dir_path, i)?;
        // }

        // // Notice order. Younger ones come first.
        // let mut sstables = Vec::new();
        // for i in (0..16).rev() {
            // let id = SstId { level: 0, id: i };
            // let sst = SSTable::load_by_id(&id, &test_dir_path)?;
            // sstables.push(sst);
        // }

        // sstables.sort();
        // let combined_iter = SSTable::iter_combined(&sstables[..])?;
        // ensure!(!whole.is_empty(), "The whole memtable is empty");
        // ensure!(
            // whole.len() == SSTable::iter_combined(&sstables[..])?.count(),
            // "The whole memtable has different count from the combined iterator"
        // );
        // if !combined_iter.eq_by(whole.iter(), |(sk, sv), (mk, mv)| &sk == mk && &sv == mv) {
            // bail!("Combined iterator produces different values from the complete memtable");
        // }

        // Ok(())
    // }

    #[test]
    fn test_compaction() -> Result<()> {
        // Check whether data are equivalent after compaction.
        // Check whether manifest contains expected values.
        //
        let test_dir_path = create_test_dir()?;

        let mut manifest = ManifestKeeper::new(&test_dir_path)?;
        // Compact 4 level 0 SSTables.
        for _ in 0..4 {
            let memtable = new_random_memtable();
            let sst_id = manifest.latest_sst_id(0);
            manifest.new_id(0);
            SSTable::flush_to_level0_without_manifest(&memtable, &test_dir_path, sst_id.id)?;
            manifest.commit()?;
        }
        // Will change active sstables.
        let old_sst_ids = manifest.active_sst_ids();
        SSTGroup::new(&manifest.get_sst_by_level(0), &test_dir_path)?.compact(
            1,
            &test_dir_path,
            &mut manifest,
        )?;

        // Load previous sstable files.
        let old_group = SSTGroup::new(&old_sst_ids, &test_dir_path)?;
        let old_combined_iter = old_group.iter();
        // Load current active sstable files.
        let sst_ids = manifest.active_sst_ids();
        let new_group = SSTGroup::new(&sst_ids, &test_dir_path)?;
        let combined_iter = new_group.iter();

        if !old_combined_iter.eq_by(combined_iter, |kv1, kv2| kv1.unwrap() == kv2.unwrap()) {
            bail!("SSTables files not equal after compaction");
        }

        //
        // Compact 4 level 0 SSTables and 10 level 1 SSTables.
        //
        // Compact 1 level 1 and its overlapping level 2 SSTables.
        Ok(())
    }

    // #[test]
    // fn test_purge_tombstone() -> Result<()> {
    // todo!();
    // }
    #[test]
    fn test_level_iterator() -> Result<()> {
        let test_dir_path = create_test_dir()?;

        let mut manifest = ManifestKeeper::new(&test_dir_path)?;
        // Compact 4 level 0 SSTables.
        let mut sst_ids = Vec::new();
        for _ in 0..4 {
            let memtable = new_random_memtable();
            let sst_id = manifest.latest_sst_id(0);
            manifest.new_id(0);
            SSTable::flush_to_level0_without_manifest(&memtable, &test_dir_path, sst_id.id)?;
            sst_ids.push(sst_id);
            manifest.commit()?;
        }
        // Will change active sstables.
        SSTGroup::new(&sst_ids, &test_dir_path)?.compact(1, &test_dir_path, &mut manifest)?;

        // Compare data with/out lazy loading.
        let sst_group = SSTGroup::new(&manifest.get_sst_by_level(1), &test_dir_path)?;
        let non_lazy_iter = sst_group.iter();

        let sst_level_group = SSTLevelGroup::new(
            1,
            &manifest
                .get_sst_by_level(1)
                .iter()
                .map(|si| si.id)
                .collect::<Vec<_>>(),
            &test_dir_path,
            &manifest,
        )?;
        let lazy_iter = sst_level_group.iter();

        ensure!(
            lazy_iter.eq_by(non_lazy_iter, |wrapped_kv, kv| wrapped_kv.unwrap()
                == kv.unwrap()),
            "Lazy loading iterator emits different data from eager one"
        );
        Ok(())
    }
}

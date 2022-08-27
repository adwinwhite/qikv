// Use a very simple format.
// Since main purpose of SStable is to speed up query access, the only additional data we store is sparse index.
// [ Record * N ]
// [ Index * M ]
// [ Size of index ]
//
// Index format :=
//      bincode::serialize(map<key, offset>)
use core::iter::{Iterator, Peekable};
use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::cmp::Ordering;

use crate::memtable::{MemTable, ValueUpdate};
use crate::manifest::*;

use anyhow::{ensure, Result};
use bincode::config;

pub const SSTABLE_DIR: &str = "SST";
pub const SPARSE_INDEX_INTERVAL: u64 = 16;
pub const SSTABLE_FILE_SIZE: u64 = u64::pow(2, 21);

pub type SparseIndex = BTreeMap<Vec<u8>, usize>;

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
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
            _               => order, 
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

// In-memory SSTable used for query and compaction.
#[derive(PartialEq, Eq, Clone)]
pub struct SSTable {
    buf: Vec<u8>,       // Store records only.
    index: SparseIndex, // Sparse index: key -> offset
    id: SstId,          // Used for sorting.
}

impl Ord for SSTable {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for SSTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SSTable {
    // Load SSTable from disk.
    // SSTable is named as db_dir/SSTABLE_DIR/level/id.
    pub fn load_by_id(sst_id: &SstId, db_dir: &Path) -> Result<SSTable> {
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

    pub fn iter_combined(sstables: &[SSTable]) -> Result<CombinedIter> {
        // Sort sst_ids by create time.
        ensure!(sstables.is_sorted(), "Input sstables are not sorted in iter_combined()");
        Ok(CombinedIter {
            iter_list: sstables.iter().map(|s| s.iter().peekable()).collect(),
            previous_key: Vec::new(),
        })
        
    }
    // TODO: use a new file every 2MB.
    pub fn compact(sst_ids: &[SstId], dest_level: u64, db_dir: &Path, manifest: &mut Manifest) -> Result<()> {
        //  Requires: SSTables are ordered by timestamp. Younger ones are at the beginning.
        //
        // Open all iterators.
        // Compare with last key, ignore duplicate.
        // Produce a single minimum key (young key preferred)
        // Collect current items and filter out None.
        //
        // Prepare the dest file.
        let mut file = manifest.new_sst_id(dest_level).create_file(db_dir)?;

        let mut index = SparseIndex::new();

        let mut num_count = 0;
        let mut offset = 0;
        let mut sst_ids = sst_ids.to_vec();
        sst_ids.sort();
        let sstables: Result<Vec<_>> = sst_ids.iter().map(|id| Self::load_by_id(&id, db_dir)).collect();

        for (k, v) in Self::iter_combined(&sstables?[..])? {
            let encoded = bincode::encode_to_vec((&k, &v), config::standard())?;
            // Check whether we should write to a new sstable file.
            if offset + encoded.len() > SSTABLE_FILE_SIZE as usize {
                // Write sparse index.
                let encoded = bincode::encode_to_vec(&index, config::standard())?;
                file.write(&encoded)?;
                file.write(&u64::to_be_bytes(encoded.len() as u64))?;
                // Create a new sstable file.
                // Reset per file variables.
                file = manifest.new_sst_id(dest_level).create_file(db_dir)?;
                index = SparseIndex::new();
                num_count = 0;
                offset = 0;
            }
            file.write(&encoded)?;
            if num_count % SPARSE_INDEX_INTERVAL == 0 {
                index.insert(k.clone(), offset);
            }
            num_count += 1;
            offset += encoded.len();
        }


        // Write sparse index.
        let encoded = bincode::encode_to_vec(&index, config::standard())?;
        file.write(&encoded)?;
        file.write(&u64::to_be_bytes(encoded.len() as u64))?;
        Ok(())
    }

    pub fn flush_to_level0(memtable: &MemTable, db_dir: &Path, id: u64) -> Result<()> {
        // Flush memtable to bytes by chunks(records).
        // And generate sparse index.
        // Write to disk.
        ensure!(memtable.len() != 0, "Tried to flush empty memtable");

        let sst_dir = db_dir.join(SSTABLE_DIR).join("0");
        fs::create_dir_all(&sst_dir)?;
        let sst_path = sst_dir.join(id.to_string());
        let mut file = File::options().write(true).create(true).open(sst_path)?;

        let mut index = SparseIndex::new();
        index.insert(memtable.front().unwrap().0.clone(), 0);
        let mut offset: usize = 0;
        for (i, pair) in memtable.iter().enumerate() {
            if i as u64 % SPARSE_INDEX_INTERVAL == 0 {
                index.insert(pair.0.clone(), offset);
            }

            let encoded = bincode::encode_to_vec(&pair, config::standard())?;
            file.write(&encoded)?;
            offset += encoded.len();
        }

        // Write sparse index.
        let encoded = bincode::encode_to_vec(&index, config::standard())?;
        file.write(&encoded)?;
        file.write(&u64::to_be_bytes(encoded.len() as u64))?;
        Ok(())
    }


    pub fn get(&self, key: &[u8]) -> Option<ValueUpdate> {
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
            } else {
                if &k[..] <= key {
                    offset = *v;
                    break;
                }
            }
        }

        // Iterate from offset.
        let mut iter = self.iter_range(offset, offset_end);
        Some(iter.find(|(k, _)| k == key)?.1)
    }

    pub fn iter(&self) -> SSTableIter<'_> {
        SSTableIter {
            buf: &self.buf,
            cur: 0,
            end: self.buf.len(),
            done: false,
        }
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
    type Item = (Vec<u8>, ValueUpdate);

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

        let (pair, size): (Self::Item, usize) =
            bincode::decode_from_slice(&self.buf[self.cur..], config::standard())
                .expect("Failed to decode sstable entry");
        self.cur += size;
        Some(pair)
    }
}

pub struct CombinedIter<'a> {
    iter_list: Vec<Peekable<SSTableIter<'a>>>,
    previous_key: Vec<u8>,
}

impl<'a> Iterator for CombinedIter<'a> {
    type Item = (Vec<u8>, ValueUpdate);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let min_index = {
                let items = self.iter_list
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(i, it)| it.peek().map_or(None, |peeked| Some((i, peeked))));
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

    use crate::memtable::ValueUpdate;
    use crate::memtable::*;
    use crate::sstable::*;
    use crate::test_util::*;
    use crate::manifest::*;

    use anyhow::{anyhow, bail, Result};
    use rand::Rng;

    fn new_random_memtable() -> MemTable {
        let mut memtable = MemTable::new_empty();
        for _ in 0..512 {
            let key = get_random_bytes(1, 10);
            let update = if rand::thread_rng().gen::<f64>() > 0.5 {
                ValueUpdate::Tombstone
            } else {
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
        SSTable::flush_to_level0(&memtable, &test_dir_path, 0)?;

        // Load SStable file and check data.
        let sst_id = SstId { level: 0, id: 0 };
        let sst = SSTable::load_by_id(&sst_id, &test_dir_path)?;
        if !sst
            .iter()
            .eq_by(memtable.iter(), |(sk, sv), (mk, mv)| &sk == mk && &sv == mv)
        {
            bail!("Loaded SST file has different iterator than MemTable's: \nSSTable={:#?}\nMemTable={:#?}", sst.iter().collect::<Vec<_>>(), memtable.iter().collect::<Vec<_>>());
        }

        // Compare using SSTable::get().
        for (k, v) in memtable.iter() {
            if &sst.get(k).ok_or(anyhow!(
                "No requested key in SSTable according to SSTable::get()"
            ))? != v
            {
                bail!("Some pair is missing in the loaded SST file according to SSTable::get()");
            }
        }
        Ok(())
    }

    #[test]
    fn test_combined_iterator() -> Result<()> {
        // Create a whole memtable and several partitioned memtables to produce sstables.
        let mut whole = MemTable::new_empty();

        let test_dir_path = create_test_dir()?;
        for i in 0..16 {
            let memtable = new_random_memtable();
            for (k, v) in memtable.iter() {
                whole.insert(k.to_vec(), v.clone());
            }

            SSTable::flush_to_level0(&memtable, &test_dir_path, i)?;
        }

        // Notice order. Younger ones come first.
        let mut sstables = Vec::new();
        for i in (0..16).rev() {
            let id = SstId {
                level: 0,
                id: i,
            };
            let sst = SSTable::load_by_id(&id, &test_dir_path)?;
            sstables.push(sst);
        }

        let combined_iter = SSTable::iter_combined(&sstables[..]);
        ensure!(whole.len() != 0, "The whole memtable is empty");
        if !combined_iter?
            .eq_by(whole.iter(), |(sk, sv), (mk, mv)| &sk == mk && &sv == mv)
        {
            bail!("Combined iterator produces different values from the complete memtable");
        }


        Ok(())
    }

    #[test]
    fn test_compaction() -> Result<()> {
        // Check whether data are equivalent after compaction.
        // Check whether manifest contains expected values.
        //
        let test_dir_path = create_test_dir()?;

        let mut manifest = Manifest::new();
        // Compact 4 level 0 SSTables.
        let mut sst_ids = Vec::new();
        for _ in 0..4 {
            let memtable = new_random_memtable();
            let sst_id = manifest.new_sst_id(0);
            SSTable::flush_to_level0(&memtable, &test_dir_path, sst_id.id)?;
            manifest.add_sst(sst_id, memtable.front().unwrap().0, memtable.back().unwrap().0);
            sst_ids.push(sst_id);
        }
        sst_ids.sort();
        // Will change active sstables.
        let old_manifest = manifest.clone();
        SSTable::compact(&sst_ids[..], 1, &test_dir_path, &mut manifest)?;

        // Load previous sstable files.
        let mut old_sstables = Vec::new();
        let mut old_sst_ids = old_manifest.active_sst_ids();
        old_sst_ids.sort();
        for id in old_sst_ids {
            let sstable = SSTable::load_by_id(&id, &test_dir_path)?;
            old_sstables.push(sstable);
        }
        let old_combined_iter = SSTable::iter_combined(&old_sstables);
        // Load current active sstable files.
        let mut sstables = Vec::new();
        let mut sst_ids = manifest.active_sst_ids();
        sst_ids.sort();
        for id in sst_ids {
            let sstable = SSTable::load_by_id(&id, &test_dir_path)?;
            sstables.push(sstable);
        }
        let combined_iter = SSTable::iter_combined(&sstables);

        if !old_combined_iter?.eq(combined_iter?) {
            bail!("SSTables files not equal after compaction");
        }


        //
        // Compact 4 level 0 SSTables and 10 level 1 SSTables.
        //
        // Compact 1 level 1 and its overlapping level 2 SSTables.
        Ok(())
    }
}

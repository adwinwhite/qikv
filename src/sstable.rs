
use crate::memtable::MemTable;

pub struct SSTable {
}

impl SSTable {
    pub fn flush_to_level0(memtable: &MemTable) {}

    pub fn compact(sstables: &[SSTable]) {}
}

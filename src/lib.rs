#![feature(iter_array_chunks)]
#![feature(map_first_last)]
#![feature(iter_order_by)]
#![feature(is_sorted)]

#![allow(unused_imports)]

pub mod log;
pub mod memtable;
pub mod sstable;
pub mod manifest;
pub mod store;

// Use custom encoding so that iterator over sstable can return references.
// pub mod encode {
    // // format := [ varstring | delete flag | [ varstring] ]
    // // varstring := [ len as u64 | payload ]

    // use std::mem;
    // use crate::memtable::ValueUpdate;

    // pub type PayloadSize = u32;

    // pub fn encode((key, update): (&Vec<u8>, &ValueUpdate)) -> Vec<u8> {
        // match update {
            // ValueUpdate::Value(v) => {
                // let mut encoded = Vec::with_capacity(mem::size_of::<PayloadSize>() + key.len() + 1 + mem::size_of::<PayloadSize>() + v.len());
                // encoded.extend_from_slice(&(key.len() as PayloadSize).to_le_bytes());
                // encoded.extend_from_slice(&key);
                // encoded.push(0);
                // encoded.extend_from_slice(&(v.len() as PayloadSize).to_le_bytes());
                // encoded.extend_from_slice(&v);
                // encoded
            // },
            // ValueUpdate::Tombstone => {
                // let mut encoded = Vec::with_capacity(mem::size_of::<PayloadSize>() + key.len() + 1);
                // encoded.extend_from_slice(&(key.len() as PayloadSize).to_le_bytes());
                // encoded.extend_from_slice(&key);
                // encoded.push(1);
                // encoded
            // }
        // }
    // }

    // pub fn decode(data: &[u8]) -> (&[u8],
// }



#[cfg(test)]
pub mod test_util {


    use std::path::PathBuf;
    use anyhow::Result;
    use rand::Rng;
    use tempdir::TempDir;

    pub fn get_random_key_range(start: usize, end: usize) -> (Vec<u8>, Vec<u8>) {
        let mut rng = rand::thread_rng();

        let length1 = rng.gen_range(start..end);
        let mut data1: Vec<u8> = vec![0; length1];
        rng.fill(&mut data1[..]);

        let length2 = rng.gen_range(start..end);
        let mut data2: Vec<u8> = vec![0; length2];
        rng.fill(&mut data2[..]);

        if data1 <= data2 {
            (data1, data2)
        } else {
            (data2, data1)
        }
    }

    pub fn get_random_bytes(start: usize, end: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let length = rng.gen_range(start..end);
        let mut data: Vec<u8> = vec![0; length];
        rng.fill(&mut data[..]);
        data
    }

    pub fn create_test_dir() -> Result<PathBuf> {
        let test_dir = TempDir::new("qikv-test")?;
        Ok(test_dir.into_path())
    }

}

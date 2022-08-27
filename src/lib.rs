#![feature(iter_array_chunks)]
#![feature(map_first_last)]
#![feature(iter_order_by)]
#![feature(is_sorted)]

pub mod log;
pub mod memtable;
pub mod sstable;
pub mod manifest;
pub mod db;



#[cfg(test)]
pub mod test_util {


    use std::path::PathBuf;
    use anyhow::Result;
    use rand::Rng;
    use tempdir::TempDir;


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

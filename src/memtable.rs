// We use skiplist as container.
// No hard deletion.
// Insertion with same key is update.
//
//
//
//
use std::path::Path;

use crate::log::LogReader;

use skiplist::SkipMap;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use bincode;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum ValueUpdate {
    Tombstone,
    Value(Vec<u8>),
}


#[derive(PartialEq, Eq)]
pub struct MemTable {
    container: SkipMap<Vec<u8>, ValueUpdate>,
}

impl MemTable {
    // Generate memtable from log or just new one.
    // Use a new empty table if anything unexpected happens.
    // Though it can lead to data missing.
    pub fn new(dir_path: &Path) -> Result<MemTable> {
        let mut table = SkipMap::new();
        match LogReader::new(dir_path) {
            Ok(reader) => {
                for entry in reader.iter() {
                    let (key, update): (Vec<u8>, ValueUpdate) = bincode::deserialize(&entry[..])?;
                    table.insert(key, update);
                }
                Ok(MemTable { container: table, })
            },
            Err(err)   => {
                // Oops. Something went wrong. 
                // Let's give a warning and ignore it.
                eprintln!("Something went wrong when initializing LogReader. \n
                           Create an empty MemTable instead. \n
                           {:#?}", err);
                
                Ok(MemTable { container: table, })
            }
        }
    }

    // pub fn from_log() {}

    pub fn insert(&mut self, key: Vec<u8>, update: ValueUpdate) -> Option<ValueUpdate> {
        self.container.insert(key, update)
    }

    pub fn get(&self, key: &[u8]) -> Option<&ValueUpdate> {
        self.container.get(key)
    }
}

#[cfg(test)]
mod tests {
    

    use crate::test_util::*;
    use crate::log::{LogWriter, LOG_FILE_MAX_SIZE, PAYLOAD_MAX_SIZE};
    use crate::memtable::{ValueUpdate, MemTable};
    

    use rand::Rng;
    use anyhow::{Result, bail, ensure};

    #[test]
    fn recover_from_log() -> Result<()> {
        // Set up log_writer and memtable.
        // Write to both.
        // Generate a new memtable from the log.
        // Compare two memtables.
        let test_dir_path = create_test_dir()?;
        let mut log_writer = LogWriter::new(&test_dir_path)?;
        let mut table = MemTable::new(&test_dir_path)?;

        for _ in 0..1024 {
            let key = get_random_bytes(1, 10);
            let update = if rand::thread_rng().gen::<f64>() > 0.5 {
                ValueUpdate::Tombstone
            } else {
                ValueUpdate::Value(get_random_bytes(1, usize::pow(2, 10)))
            };
            let payload = bincode::serialize(&(&key, &update))?;
            ensure!(payload.len() < PAYLOAD_MAX_SIZE as usize, "Payload is larger than what log allows");
            log_writer.write(&payload)?;
            table.insert(key, update);
        }
        ensure!(log_writer.len()? <= LOG_FILE_MAX_SIZE, "Log writer wrote too much into a single log file");

        let recovered_table = MemTable::new(&test_dir_path)?;
        if recovered_table == table {
            Ok(())
        } else {
            bail!("Recovered memtable is not valid");
        }
    }
}

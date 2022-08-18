
// Log format
// [ length | payload ]
// Length := 
//      length of payload in bytes
//      2 byte for simplcity
//      so max size of our payload is 32kB.
// Payload can be record.
// Record :=
//      [ type | varstring | [varstring] ]
//      type := 
//          Insert or Tombstone
//          1 byte
//      varstring :=
//          [ length | data ]
//      latter varstring exists only when type is Insert
//
// Checksum may be added later when I understand how to deal with incorrect checksum.
//
// Log files' name are increasing number which makes switching to another log file easier.
// Max size of a log file is 4 MB.
// When there is not enough space for the next coming payload, fill the rest space with zero and
// switch to a new log file.
//
// Should log be async or sync? Sync for now. Better provide an option.
//
use std::io::{Read, Write};
use std::iter::Iterator;
use std::fs::File;
use std::path::Path;
use std::vec::Vec;

use anyhow::{Result, bail};

const LOG_FILENAME: &str = "RECOVERY_LOG";
const LOG_MAX_SIZE: u64 = 4 * u64::pow(2, 20);
const PAYLOAD_MAX_SIZE: usize = usize::pow(2, 16);

pub struct LogWriter {
    file: File,
}

impl LogWriter {
    // Path will where log files are placed.
    // Directories will be created if not exist.
    pub fn new(dir_path: &str) -> Result<LogWriter> {
        let log_path = Path::new(dir_path).join(LOG_FILENAME);
        let file = File::options()
            .append(true)
            .create(true)
            .open(log_path)?;
        Ok(LogWriter { file, })
    }

    // Write paylaod to current log file.
    pub fn write(&mut self, payload: &[u8]) -> Result<()> {
        let total_length = payload.len() + 2;
        let mut data = Vec::with_capacity(total_length);
        data.extend(u16::try_from(payload.len())?.to_be_bytes());
        data.extend(payload);
        let size_written = self.file.write(&data)?;
        self.file.flush()?;
        if size_written == total_length {
            Ok(())
        } else {
            bail!("Size of data written is incorrect");
        }
    }


}

pub struct LogIter<'a> {
    buf: &'a Vec<u8>,
    cur: usize,    // cursor for iterator.
    done: bool
}

impl<'a> Iterator for LogIter<'a> {
    type Item = &'a [u8];

    // Done if size is 0.
    // Assume data is not corrupted.
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // Read size of next payload.
        let size = u16::from_be_bytes(self.buf[self.cur..self.cur + 2].try_into().ok()?);

        if size == 0 {
            self.done = true;
            return None;
        }

        self.cur += 2 + size as usize;
        Some(&self.buf[self.cur + 2 .. self.cur + 2 + size as usize])
    }
}

pub struct LogReader {
    buf: Vec<u8> // Array on stack will use too much space aka 4MB/10MB on linux.
}

impl LogReader {
    // Return None if no valid log file found.
    pub fn new(dir_path: &str) -> Result<LogReader> {
        // Prepare the buffer of 4MB.
        let mut buf = Vec::with_capacity(LOG_MAX_SIZE.try_into()?);

        // Check whether size is 4MB.
        let log_path = Path::new(dir_path).join(LOG_FILENAME);
        let mut file = File::open(log_path)?;
        if file.metadata()?.len() > LOG_MAX_SIZE {
            bail!("The size of log file is larger than defined");
        }

        file.read_to_end(&mut buf)?;
        Ok(LogReader { buf,})
    }

    pub fn iter(&self) -> LogIter<'_> {
        LogIter {
            buf: &self.buf,
            cur: 0,
            done: false,
        }
    }

}



#[cfg(test)]
mod tests {
    use std::fs;

    use crate::log::*;

    use chrono::Utc;
    use anyhow::{Result, ensure};
    use rand::Rng;

    fn get_random_bytes() -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let length = rng.gen_range(1..PAYLOAD_MAX_SIZE);
        let mut data: Vec<u8> = Vec::with_capacity(length);
        rng.fill(&mut data[..]);
        data
    }

    // Log will create a new log file in an empty log dir.
    #[test]
    fn write_empty_read() -> Result<()> {
        // Set up an empty test dir.
        let test_dir_path = Utc::now().to_rfc3339() + "-qikv-test";
        fs::create_dir_all(&test_dir_path)?;

        // Init log and write data.
        let mut writer = LogWriter::new(&test_dir_path)?;
        let mut data: Vec<u8> = Vec::new();
        for _ in 0..100 {
            let payload = get_random_bytes();
            data.extend(&payload);
            writer.write(&payload)?;
        }

        // Compare read data.
        let reader = LogReader::new(&test_dir_path)?;
        let mut cursor = 0;
        for entry in reader.iter() {
            let payload = entry;
            ensure!(data[cursor .. cursor + payload.len()] == payload[..], "Data read is different from what was written. {:?} != {:?}", &data[cursor .. cursor + payload.len()], &payload[..]); 
            cursor += payload.len();
        }
        // Clean up
        fs::remove_dir_all(test_dir_path)?;
        Ok(())
    }
}
    


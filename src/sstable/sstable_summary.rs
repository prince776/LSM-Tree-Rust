use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    mem::size_of,
};

use super::PersistFormat;

// SSTableSummary file would be a long list of key,value pairs using same PersistFormat as
// SSTable, only difference is that value refers to offset in the data file.
pub struct SSTableSummary {
    index: BTreeMap<String, i64>,
}

impl SSTableSummary {
    pub fn get_entry_offset(&self, key: &str) -> Option<&i64> {
        self.index.get(key)
    }

    pub fn from_file(file_name: &str) -> SSTableSummary {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(file_name)
            .expect("Failed to open file");

        let mut summary = SSTableSummary {
            index: BTreeMap::new(),
        };

        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)
            .expect("Failed to read summary file");

        let mut idx = 0;
        // Assuming the file is valid for now.
        while idx < buf.len() {
            let (bytes_read, entry) = PersistFormat::deserialize(&buf[idx..]);
            idx += bytes_read;

            let value: i64 = entry
                .value
                .trim()
                .parse()
                .expect("Summary table has value that is not an int");

            summary.index.insert(entry.key, value);
        }

        return summary;
    }

    pub fn upsert(&mut self, key: String, value: i64) {
        self.index.insert(key, value);
    }

    pub fn flush(&mut self, file_name: &str) -> Result<(), io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_name)
            .expect("Failed to open file");

        for (key, value) in &self.index {
            let entry = PersistFormat::new(key.clone(), value.to_string()).serialize();
            file.write(&entry)?;
        }

        return Ok(());
    }
}

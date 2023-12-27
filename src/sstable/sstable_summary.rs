use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read},
    mem::size_of,
};

use super::PersistFormat;

// SSTableSummary file would be a long list of key,value pairs using same PersistFormat as
// SSTable, only difference is that value refers to offset in the data file.
pub struct SSTableSummary {
    index: HashMap<String, i64>,
}

impl SSTableSummary {
    pub fn from_file(file_name: &str) -> SSTableSummary {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(file_name)
            .expect("Failed to open file");

        let mut summary = SSTableSummary {
            index: HashMap::new(),
        };

        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)
            .expect("Failed to read summary file");

        let mut idx = 0;
        // Assuming the file is valid for now.
        while idx < buf.len() {
            let (bytes_read, entry) = PersistFormat::deserialize(buf.as_slice());
            idx += bytes_read;

            let value = entry
                .value
                .parse::<i64>()
                .expect("Summary table has value that is not an int");

            summary.index.insert(entry.key, value);
        }

        return summary;
    }
}

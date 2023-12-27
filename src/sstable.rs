mod sstable_summary;
use self::sstable_summary::SSTableSummary;
use std::{
    fs::OpenOptions,
    io::{Read, Seek, Write},
    mem::size_of,
};

pub struct SSTable {
    summary: SSTableSummary,
    summary_file_name: String,
    data_files_count: i64,
}

struct PersistFormat {
    key: String,
    value: String,
}

impl SSTable {
    pub fn new(summary_file_name: &str) -> SSTable {
        let summary = SSTableSummary::from_file(summary_file_name);
        let data_files_count = summary.existing_data_files_count;
        SSTable {
            summary: summary,
            summary_file_name: String::from(summary_file_name),
            data_files_count: data_files_count,
        }
    }
}

impl SSTable {
    fn get_data_file_name(&self, file_num: i64) -> String {
        format!("{}_data_{}", self.summary_file_name, file_num)
    }

    pub fn write_table(&mut self, data: Vec<(String, String)>) -> Result<(), std::io::Error> {
        let data_file_name = self.get_data_file_name(self.data_files_count);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(data_file_name)
            .expect("Failed to open file");

        // Entry offset is like: first 30 bits represent offset in file, and the remaining top bits
        // represent the data file number. Could make a wrapper for this but meh.
        let mut entry_offset: i64 = (1 << 30) * self.data_files_count;

        for (key, value) in data {
            let key_clone = key.clone();
            let entry = PersistFormat::new(key, value).serialize();
            file.write(entry.as_slice())?;

            self.summary.upsert(key_clone, entry_offset);

            entry_offset += entry.len() as i64;
        }

        self.data_files_count += 1;

        self.summary.existing_data_files_count = self.data_files_count;
        self.summary.flush(&self.summary_file_name)?;

        return Ok(());
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let entry_offset = self.summary.get_entry_offset(key)?;

        let file_num = entry_offset >> 30;
        let file_offset = (entry_offset % (1 << 30)) as usize;

        // Ideally this should be cached to some degree.
        let data_file_name = self.get_data_file_name(file_num);
        let mut file = OpenOptions::new()
            .read(true)
            .open(data_file_name)
            .expect("Failed to open data file that should exist");

        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)
            .expect("Failed to read data file");

        let (_, entry) = PersistFormat::deserialize(&buf[file_offset..]);

        return Some(entry.value);
    }
}

impl PersistFormat {
    fn new(key: String, value: String) -> PersistFormat {
        PersistFormat { key, value }
    }

    fn serialize(&self) -> Vec<u8> {
        let key_len = self.key.len() as i64;
        let value_len = self.value.len() as i64;

        return [
            &key_len.to_be_bytes(),
            &value_len.to_be_bytes(),
            self.key.as_bytes(),
            self.value.as_bytes(),
        ]
        .concat();
    }

    fn deserialize(buf: &[u8]) -> (usize, PersistFormat) {
        const ISIZE: usize = size_of::<i64>();

        let mut idx = 0;
        let int_bytes: [u8; ISIZE] = buf[idx..idx + ISIZE].try_into().unwrap();
        let key_len = i64::from_be_bytes(int_bytes) as usize;
        idx += ISIZE;

        let int_bytes: [u8; ISIZE] = buf[idx..idx + ISIZE].try_into().unwrap();
        let value_len = i64::from_be_bytes(int_bytes) as usize;
        idx += ISIZE;

        let key = String::from_utf8(buf[idx..idx + key_len].to_vec()).unwrap();
        idx += key_len;

        let value = String::from_utf8(buf[idx..idx + value_len].to_vec()).unwrap();
        idx += value_len;

        return (idx, PersistFormat { key, value });
    }
}

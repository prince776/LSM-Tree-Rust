mod sstable_summary;
use self::sstable_summary::SSTableSummary;
use std::{fs::OpenOptions, io::Write, mem::size_of};

pub struct SSTable {
    summary: SSTableSummary,
    summary_file_name: String,
    data_files_count: i32,
}

struct PersistFormat {
    key: String,
    value: String,
}

impl SSTable {
    pub fn new(summary_file_name: &str) -> SSTable {
        SSTable {
            summary: SSTableSummary::from_file(summary_file_name),
            summary_file_name: String::from(summary_file_name),
            data_files_count: 0,
        }
    }
}

impl SSTable {
    pub fn write_table(&mut self, data: Vec<(String, String)>) -> Result<(), std::io::Error> {
        let data_file_name = format!("{}_data_{}", self.summary_file_name, self.data_files_count);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(data_file_name)
            .expect("Failed to open file");

        for (key, value) in data {
            let entry = PersistFormat::new(key, value);
            file.write(entry.serialize().as_slice())?;
        }

        self.data_files_count += 1;
        return Ok(());
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

        let value = String::from_utf8(buf[idx..idx + key_len].to_vec()).unwrap();
        idx += value_len;

        return (idx, PersistFormat { key, value });
    }
}

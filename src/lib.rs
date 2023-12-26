use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter},
};

use sstable::SSTable;

mod sstable;

pub struct LsmTree {
    file_num: i32,
    memtable: HashMap<String, String>,
}

impl LsmTree {
    pub fn new() -> LsmTree {
        return LsmTree {
            file_num: 0,
            memtable: HashMap::new(),
        };
    }

    const FILE_PREFIX: &'static str = "sstable_";
}

impl LsmTree {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.memtable.get(key)
    }

    pub fn upsert(&mut self, key: String, value: String) {
        self.memtable.insert(key, value);
    }

    pub fn delete(self) {
        unimplemented!()
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        let sstable_entries: Vec<_> = self.memtable.clone().into_iter().collect();
        self.memtable.clear();

        let file_name = format!(
            "{prefix}{num}",
            prefix = LsmTree::FILE_PREFIX,
            num = self.file_num
        );
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_name)
            .expect("Failed to open file");
        let file_writer = BufWriter::new(file);

        let mut sstable = SSTable::with_data(file_writer, sstable_entries);
        sstable.write_table().expect("Failed to write sstable");

        self.file_num += 1;
        return Ok(());
    }
}

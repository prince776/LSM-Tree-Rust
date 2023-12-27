use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter},
};

use sstable::SSTable;
mod sstable;

pub struct LsmTree {
    op_count: i32,
    memtable: HashMap<String, String>,
    sstable: SSTable,
}

impl LsmTree {
    pub fn new(summary_file_name: &str) -> LsmTree {
        return LsmTree {
            op_count: 0,
            memtable: HashMap::new(),
            sstable: SSTable::new(summary_file_name),
        };
    }

    const FLUSH_THRESHOLD: i32 = 100;
}

impl LsmTree {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.memtable.get(key)
    }

    pub fn upsert(&mut self, key: String, value: String) {
        self.memtable.insert(key, value);
        self.op_count += 1;

        if self.op_count >= LsmTree::FLUSH_THRESHOLD {
            self.flush().expect("Failed to flush data to disk");
            self.op_count = 0;
        }
    }

    pub fn delete(self) {
        unimplemented!()
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        let sstable_entries: Vec<_> = self.memtable.clone().into_iter().collect();
        self.memtable.clear();

        self.sstable
            .write_table(sstable_entries)
            .expect("Failed to write sstable data file");
        return Ok(());
    }
}

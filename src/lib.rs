use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::{self, BufWriter},
};

use sstable::SSTable;
mod sstable;

pub struct LsmTree {
    op_count: i32,
    memtable: BTreeMap<String, String>,
    sstable: SSTable,
}

impl LsmTree {
    pub fn new(summary_file_name: &str) -> LsmTree {
        return LsmTree {
            op_count: 0,
            memtable: BTreeMap::new(),
            sstable: SSTable::new(summary_file_name),
        };
    }

    const FLUSH_THRESHOLD: i32 = 100;
}

impl Drop for LsmTree {
    fn drop(&mut self) {
        self.flush()
            .expect("Failed to flush before dropping lsm tree instance");
    }
}

impl LsmTree {
    pub fn get(&self, key: &str) -> Option<String> {
        let res = self.memtable.get(key);
        if let Some(val) = res {
            return Some(val.clone());
        }

        return self.sstable.get(key);
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
        if self.memtable.len() == 0 {
            return Ok(());
        }

        let sstable_entries: Vec<_> = self.memtable.clone().into_iter().collect();
        self.memtable.clear();

        self.sstable
            .write_table(sstable_entries)
            .expect("Failed to write sstable data file");
        return Ok(());
    }
}

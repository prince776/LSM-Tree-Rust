use std::collections::HashMap;

use sstable::SSTable;

mod sstable;

pub struct LsmTree {
    memtable: HashMap<String, String>,
}

impl LsmTree {
    pub fn new() -> LsmTree {
        return LsmTree {
            memtable: HashMap::new(),
        };
    }
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

    pub fn flush(&mut self) {}
}

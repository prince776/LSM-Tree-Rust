use std::{fmt::format, io};

use lsm_tree_rust::LsmTree;

fn main() {
    let mut db = LsmTree::new("db_sstable");

    let not_found_str = String::from("No Value Found!!");
    for i in 0..1000 {
        let k = format!("Key{i}");
        let v = format!("Value{i}");

        db.upsert(k, v);
    }

    println!(
        "Value for Key100: {}",
        db.get("Key100").unwrap_or(not_found_str)
    );
}

use std::{fmt::format, io};

use lsm_tree_rust::LsmTree;

fn main() {
    let not_found_str = String::from("No Value Found!!");

    // Use one instance to create the db and persist on disk.
    {
        let mut db = LsmTree::new("db_sstable");

        for i in 0..1099 {
            let k = format!("Key{i}");
            let v = format!("Value{i}");

            db.upsert(k, v);
        }
    }

    // Now use persisted data to get correct data.
    {
        let db = LsmTree::new("db_sstable");
        let key = String::from("Key1057");
        println!(
            "Value for {}: {}",
            key,
            db.get(&key).unwrap_or(not_found_str)
        );
    }
}

use std::{fmt::format, io};

use lsm_tree_rust::LsmTree;

fn main() {
    let mut db = LsmTree::new();

    let not_found_str = String::from("No Value Found!!");
    for i in 0..1000 {
        let k = format!("Key{i}");
        let v = format!("Value{i}");

        db.upsert(k, v);
    }
}

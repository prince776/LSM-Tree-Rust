use std::io;

use lsm_tree_rust::LsmTree;

fn main() {
    let mut db = LsmTree::new();

    let k1 = String::from("Key1");
    let v1 = String::from("Value1");

    db.upsert(k1, v1);

    let not_found_str = String::from("No Value Found!!");

    let t1 = db.get("Key1").unwrap_or(&not_found_str);
    println!("Got val: {t1}")
}

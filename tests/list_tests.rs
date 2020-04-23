#[macro_use]
extern crate quickcheck;

use sledis::{
    lists::{ListStore, Meta},
    Store,
};

use sled::{Config, Tree};

fn set_up_list_store() -> Tree {
    Config::default()
        .cache_capacity(10_000)
        .temporary(true)
        .open()
        .unwrap()
        .open_tree("tree")
        .unwrap()
}

quickcheck! {
    fn list_push() -> bool {
        let store = set_up_list_store();
        true
    }
}

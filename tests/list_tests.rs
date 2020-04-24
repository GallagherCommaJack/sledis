#[macro_use]
extern crate quickcheck;

use sledis::lists::ListStore;

use sled::{Config, Tree};

fn set_up_list_store() -> Tree {
    Config::default()
        .cache_capacity(100_000)
        .temporary(true)
        .open()
        .unwrap()
        .open_tree("tree")
        .unwrap()
}

quickcheck! {
    fn list_mutation(key: Vec<u8>, val: Vec<Vec<u8>>) -> bool {
        let store = set_up_list_store();

        // new list is in fact empty
        let list_meta = store.list_create(key.as_slice()).unwrap();
        assert_eq!(0, list_meta.len);
        true
    }

}

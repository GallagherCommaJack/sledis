use quickcheck::{Arbitrary, Gen};

use quickcheck_macros::*;
use sled::{Config, Tree};
use sledis::*;
use std::collections::BTreeMap;

fn set_up_table_store() -> Tree {
    Config::default()
        .cache_capacity(100_000)
        .temporary(true)
        .open()
        .unwrap()
        .open_tree("tree")
        .unwrap()
}

struct Models {
    store: Tree,
    model: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Models {
    fn new() -> Self {
        let store = set_up_table_store();
        let model = BTreeMap::new();
        Self { store, model }
    }

    fn validate(&self) -> bool {
        self.store.len() == self.model.iter().map(|(_, kvs)| 1 + kvs.len()).sum()
            && self.model.iter().all(|(name, kvs)| {
                self.store
                    .table_get_meta(name)
                    .expect("get failed")
                    .expect("missing meta")
                    .len()
                    == kvs.len() as u64
                    && kvs.iter().all(|(key, val)| {
                        self.store
                            .table_get(name, key)
                            .expect("get failed")
                            .expect("missing val")
                            .as_ref()
                            == val.as_slice()
                    })
            })
    }

    fn insert(&mut self, name: Vec<u8>, key: Vec<u8>, val: Vec<u8>) {
        let in_tree = self
            .store
            .table_insert(&name, &key, val.clone())
            .expect("failed to add");
        let in_model = self.model.entry(name).or_default().insert(key, val);
        assert_eq!(
            in_tree.as_ref().map(AsRef::as_ref),
            in_model.as_ref().map(AsRef::as_ref)
        );
    }

    fn remove(&mut self, name: Vec<u8>, key: Vec<u8>) {
        let in_tree = self
            .store
            .table_remove(&name, &key)
            .expect("failed to remove");
        let in_model = self.model.get_mut(&name).and_then(|kv| kv.remove(&key));
        assert_eq!(
            in_tree.as_ref().map(AsRef::as_ref),
            in_model.as_ref().map(AsRef::as_ref)
        );
    }

    fn get(&mut self, name: Vec<u8>, key: Vec<u8>) {
        let in_tree = self.store.table_get(&name, &key).expect("failed to get");
        let in_model = self.model.get(&name).and_then(|kv| kv.get(&key));
        assert_eq!(
            in_tree.as_ref().map(AsRef::as_ref),
            in_model.as_ref().map(AsRef::as_ref)
        );
    }
}

mod many_tables {
    use super::*;

    #[derive(Debug, Clone)]
    enum TablesOp {
        Insert(Vec<u8>, Vec<u8>, Vec<u8>),
        Remove(Vec<u8>, Vec<u8>),
        Get(Vec<u8>, Vec<u8>),
    }

    impl Arbitrary for TablesOp {
        fn arbitrary<G: Gen>(gen: &mut G) -> Self {
            match u8::arbitrary(gen) % 2 {
                0 => TablesOp::Insert(
                    Vec::arbitrary(gen),
                    Vec::arbitrary(gen),
                    Vec::arbitrary(gen),
                ),
                1 => TablesOp::Remove(Vec::arbitrary(gen), Vec::arbitrary(gen)),
                2 => TablesOp::Get(Vec::arbitrary(gen), Vec::arbitrary(gen)),
                _ => unreachable!(),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            match self {
                TablesOp::Insert(name, key, value) => Box::new(
                    (name.clone(), key.clone(), value.clone())
                        .shrink()
                        .map(|(n, k, v)| TablesOp::Insert(n, k, v)),
                ),
                TablesOp::Remove(name, key) => Box::new(
                    (name.clone(), key.clone())
                        .shrink()
                        .map(|(n, k)| TablesOp::Remove(n, k)),
                ),
                TablesOp::Get(name, key) => Box::new(
                    (name.clone(), key.clone())
                        .shrink()
                        .map(|(n, k)| TablesOp::Get(n, k)),
                ),
            }
        }
    }

    impl Models {
        fn apply_op(&mut self, op: TablesOp) {
            match op {
                TablesOp::Insert(name, key, value) => self.insert(name, key, value),
                TablesOp::Remove(name, key) => self.remove(name, key),
                TablesOp::Get(name, key) => self.get(name, key),
            }
        }
    }

    #[quickcheck]
    fn always_equiv(ops: Vec<TablesOp>) -> bool {
        let mut model = Models::new();
        for op in ops {
            model.apply_op(op);
        }
        model.validate()
    }
}

mod one_table {
    use super::*;

    const NAME: &[u8] = b"KEY";

    #[derive(Debug, Clone)]
    enum TableOp {
        Insert(Vec<u8>, Vec<u8>),
        Remove(Vec<u8>),
        Get(Vec<u8>),
    }

    impl Arbitrary for TableOp {
        fn arbitrary<G: Gen>(gen: &mut G) -> Self {
            match u8::arbitrary(gen) % 2 {
                0 => TableOp::Insert(Vec::arbitrary(gen), Vec::arbitrary(gen)),
                1 => TableOp::Remove(Vec::arbitrary(gen)),
                2 => TableOp::Get(Vec::arbitrary(gen)),
                _ => unreachable!(),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            match self {
                TableOp::Insert(key, value) => Box::new(
                    (key.clone(), value.clone())
                        .shrink()
                        .map(|(k, v)| TableOp::Insert(k, v)),
                ),
                TableOp::Remove(key) => Box::new(key.clone().shrink().map(|k| TableOp::Remove(k))),
                TableOp::Get(key) => Box::new(key.clone().shrink().map(|k| TableOp::Get(k))),
            }
        }
    }

    impl Models {
        fn apply_uq_op(&mut self, op: TableOp) {
            match op {
                TableOp::Insert(key, value) => self.insert(NAME.to_vec(), key, value),
                TableOp::Remove(key) => self.remove(NAME.to_vec(), key),
                TableOp::Get(key) => self.get(NAME.to_vec(), key),
            }
        }
    }

    #[quickcheck]
    fn always_equiv(ops: Vec<TableOp>) -> bool {
        let mut model = Models::new();
        for op in ops {
            model.apply_uq_op(op);
        }
        model.validate()
    }
}

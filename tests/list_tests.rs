#[macro_use]
extern crate quickcheck;

use quickcheck::{Arbitrary, Gen};
use sled::{Config, Tree};
use sledis::lists::ListStore;
use std::collections::VecDeque;

fn set_up_list_store() -> Tree {
    Config::default()
        .cache_capacity(100_000)
        .temporary(true)
        .open()
        .unwrap()
        .open_tree("tree")
        .unwrap()
}

#[derive(Debug, Clone)]
enum DequeuOp {
    PushFront(Vec<u8>),
    PopFront,
    PushBack(Vec<u8>),
    PopBack,
}

impl Arbitrary for DequeuOp {
    fn arbitrary<G: Gen>(gen: &mut G) -> Self {
        match gen.next_u32() % 3 {
            0 => Self::PushFront(Vec::arbitrary(gen)),
            1 => Self::PopFront,
            2 => Self::PushBack(Vec::arbitrary(gen)),
            3 => Self::PopBack,
            _ => unreachable!(),
        }
    }
}

fn deep_eq<S: ListStore, T: AsRef<[u8]>>(store: &S, deque: VecDeque<T>) -> bool {
    true
}

quickcheck! {

    // test API identicallity to std::collections::VecDeque.
    fn chaos_test_list(key: Vec<u8>, op_list: Vec<DequeuOp>) -> bool {

        let name = b"chaos_test_list_name";
        let store = set_up_list_store();
        store.list_create(name).unwrap();

        let mut dequeue = VecDeque::new();
        op_list.iter().for_each(|op| match op {
            DequeuOp::PushFront(val) => {
                dequeue.push_front(val.clone());
                store.list_push_front(name, val.clone()).unwrap();
            },
            DequeuOp::PushBack(val) => {
                dequeue.push_back(val.clone());
                store.list_push_back(name, val.clone()).unwrap();
            },
            DequeuOp::PopFront=> {
                let deq_res = dequeue.pop_front();
                let store_res = store.list_pop_front(name).unwrap();
            },
            DequeuOp::PopBack => {
                let deq_res = dequeue.pop_front();
                let store_res = store.list_pop_front(name).unwrap();
            },
        });
        true
    }

}

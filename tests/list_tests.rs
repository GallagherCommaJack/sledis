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

// assert a list and a dequeue have the same contents
fn deep_eq<S: ListStore, T: AsRef<[u8]> + std::fmt::Debug>(
    store: &S,
    name: &[u8],
    deque: &VecDeque<T>,
) {
    for idx in 0..store.list_len(name).ok().unwrap().unwrap() {
        assert_eq!(
            store.list_get(name, idx).ok().unwrap().unwrap(),
            deque[idx as usize],
            "Store and Dequeue did not have identical contents"
        );
    }
}

// assert the values popped from a std dequeue match that of a value popped
// from a sledis list.
fn pop_eq(deq_ret: Option<Vec<u8>>, sled_ret: Option<sled::IVec>) -> bool {
    match (deq_ret, sled_ret) {
        (None, None) => true,
        (Some(d), Some(s)) => d.as_slice() == s.as_ref(),
        _ => false,
    }
}

quickcheck! {

    // test API identicallity to std::collections::VecDeque.
    fn chaos_test_list(op_list: Vec<DequeuOp>) -> bool {

        // init list store
        let name = b"chaos_test_list_name";
        let store = set_up_list_store();
        store.list_create(name).unwrap();

        // init dequeue
        let mut dequeue = VecDeque::new();

        op_list.iter().for_each(|op| match op {
            DequeuOp::PushFront(val) => {
                dequeue.push_front(val.clone());
                store.list_push_front(name, val.clone()).unwrap();
                deep_eq(&store, name, &dequeue);
            },
            DequeuOp::PushBack(val) => {
                dequeue.push_back(val.clone());
                store.list_push_back(name, val.clone()).unwrap();
                deep_eq(&store, name, &dequeue);
            },
            DequeuOp::PopFront=> {
                let deq_res = dequeue.pop_front();
                let store_res = store.list_pop_front(name).unwrap();
                assert!(pop_eq(deq_res, store_res), "PopFront returned a bad value.");
            },
            DequeuOp::PopBack => {
                let deq_res = dequeue.pop_front();
                let store_res = store.list_pop_front(name).unwrap();
                assert!(pop_eq(deq_res, store_res), "PopBack returned a bad value.");
            },
        });

        true
    }

}

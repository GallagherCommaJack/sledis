use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::*;
use sledis::*;
use std::collections::VecDeque;

mod common;
use common::TempDb;

#[derive(Debug, Clone)]
enum DequeuOp {
    PushFront(Vec<u8>),
    PopFront,
    PushBack(Vec<u8>),
    PopBack,
}

impl Arbitrary for DequeuOp {
    fn arbitrary<G: Gen>(gen: &mut G) -> Self {
        match gen.next_u32() % 4 {
            0 => Self::PushFront(Vec::arbitrary(gen)),
            1 => Self::PopFront,
            2 => Self::PushBack(Vec::arbitrary(gen)),
            3 => Self::PopBack,
            _ => unreachable!(),
        }
    }
}

// assert a list and a dequeue have the same contents
fn deep_eq(store: &Conn, name: &[u8], deque: &VecDeque<Vec<u8>>) -> bool {
    let len = store.list_len(name).expect("store error");

    (0..len).all(|idx| {
        store
            .list_get(name, idx)
            .expect("store error")
            .expect("element not found")
            == deque[idx as usize]
    })
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

#[quickcheck]
// test API identicallity to std::collections::VecDeque.
fn chaos_test_list_with_name((ref name, ref ops): (Vec<u8>, Vec<DequeuOp>)) -> bool {
    // init list store
    let store = TempDb::new().expect("failed to create temporary db");

    // init dequeue
    let mut dequeue = VecDeque::new();

    let ops_corr = ops.iter().all(|op| match op {
        DequeuOp::PushFront(val) => {
            dequeue.push_front(val.clone());
            store.list_push_front(name, val.as_slice().into()).unwrap();
            true
        }
        DequeuOp::PushBack(val) => {
            dequeue.push_back(val.clone());
            store.list_push_back(name, val.as_slice().into()).unwrap();
            true
        }
        DequeuOp::PopFront => {
            let deq_res = dequeue.pop_front();
            let store_res = store.list_pop_front(name).unwrap();
            pop_eq(deq_res, store_res)
        }
        DequeuOp::PopBack => {
            let deq_res = dequeue.pop_front();
            let store_res = store.list_pop_front(name).unwrap();
            pop_eq(deq_res, store_res)
        }
    });

    let res_eq = deep_eq(&store, name, &dequeue);

    ops_corr && res_eq
}

#[quickcheck]
// test API identicallity to std::collections::VecDeque.
fn chaos_test_list_no_name(ops: Vec<DequeuOp>) -> bool {
    // init list store
    let name = b"chaos_test_no_name";
    let store = TempDb::new().expect("failed to create temporary db");

    // init dequeue
    let mut dequeue = VecDeque::new();

    let ops_corr = ops.iter().all(|op| match op {
        DequeuOp::PushFront(val) => {
            dequeue.push_front(val.clone());
            store.list_push_front(name, val.as_slice().into()).unwrap();
            true
        }
        DequeuOp::PushBack(val) => {
            dequeue.push_back(val.clone());
            store.list_push_back(name, val.as_slice().into()).unwrap();
            true
        }
        DequeuOp::PopFront => {
            let deq_res = dequeue.pop_front();
            let store_res = store.list_pop_front(name).unwrap();
            pop_eq(deq_res, store_res)
        }
        DequeuOp::PopBack => {
            let deq_res = dequeue.pop_front();
            let store_res = store.list_pop_front(name).unwrap();
            pop_eq(deq_res, store_res)
        }
    });

    let res_eq = deep_eq(&store, name, &dequeue);

    ops_corr && res_eq
}

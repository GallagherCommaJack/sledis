use criterion::*;
use sledis::*;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Clone)]
pub struct TempDb {
    conn: sledis::Conn,
    _dir: Arc<tempfile::TempDir>,
}

impl Default for TempDb {
    fn default() -> Self {
        let _dir = Arc::new(tempfile::tempdir().expect("failed to create tempdir"));
        let conn = sled::Config::default()
            .path(_dir.path())
            .cache_capacity(4 << 30)
            .mode(sled::Mode::HighThroughput)
            .open_sledis()
            .expect("failed to open db");
        TempDb { _dir, conn }
    }
}

impl std::ops::Deref for TempDb {
    type Target = sledis::Conn;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl std::ops::DerefMut for TempDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

const KEY_SIZES: &[usize] = &[8];
const VAL_SIZES: &[usize] = &[512];

fn list_ops(c: &mut Criterion) {
    for num_threads in (0..10)
        .map(|p| 2usize.pow(p))
        .take_while(|p| *p <= num_cpus::get())
    {
        for key_size in KEY_SIZES {
            for val_size in VAL_SIZES {
                let mut group = c.benchmark_group(&format!(
                    "list ops, {} threads, key size: {}, val size: {}",
                    num_threads, key_size, val_size
                ));

                let keys = (0..num_threads)
                    .map(move |i| vec![i as u8; *key_size])
                    .collect::<Vec<_>>();
                let val: sled::IVec = vec![1u8; *val_size].as_slice().into();

                let store = TempDb::default();

                group.bench_function("list push back", |b| {
                    b.iter_custom(|iters| {
                        let keys = keys.clone();

                        store.clear().expect("failed to clear store");

                        let start = Instant::now();

                        let countdown = Arc::new(AtomicU64::new(iters));
                        let handles = keys
                            .into_iter()
                            .map(|key| {
                                let store = store.clone();
                                let val = val.clone();
                                let countdown = countdown.clone();
                                std::thread::spawn(move || {
                                    while countdown.fetch_sub(1, Ordering::AcqRel) <= iters {
                                        store
                                            .list_push_back(&key, val.as_ref().into())
                                            .expect("failed to push")
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        for handle in handles {
                            handle.join().unwrap()
                        }

                        start.elapsed()
                    })
                });

                group.bench_function("list push front", |b| {
                    b.iter_custom(|iters| {
                        let keys = keys.clone();

                        store.clear().expect("failed to clear store");

                        let start = Instant::now();

                        let countdown = Arc::new(AtomicU64::new(iters));
                        let handles = keys
                            .into_iter()
                            .map(|key| {
                                let store = store.clone();
                                let val = val.clone();
                                let countdown = countdown.clone();
                                std::thread::spawn(move || {
                                    while countdown.fetch_sub(1, Ordering::AcqRel) <= iters {
                                        store
                                            .list_push_front(&key, val.as_ref().into())
                                            .expect("failed to push")
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        for handle in handles {
                            handle.join().unwrap()
                        }

                        start.elapsed()
                    })
                });

                group.bench_function("list pop back after push back", |b| {
                    b.iter_custom(|iters| {
                        let keys = keys.clone();
                        store.clear().expect("failed to clear store");
                        for key in &keys {
                            for _ in 0..iters {
                                store
                                    .list_push_back(&key, val.clone())
                                    .expect("failed to push");
                            }
                        }

                        let start = Instant::now();

                        let countdown = Arc::new(AtomicU64::new(iters));
                        let handles = keys
                            .into_iter()
                            .map(|key| {
                                let store = store.clone();
                                let countdown = countdown.clone();
                                std::thread::spawn(move || {
                                    while countdown.fetch_sub(1, Ordering::AcqRel) <= iters {
                                        store.list_pop_back(&key).expect("failed to push");
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        for handle in handles {
                            handle.join().unwrap()
                        }

                        start.elapsed()
                    })
                });

                group.bench_function("list pop back after push front", |b| {
                    b.iter_custom(|iters| {
                        let keys = keys.clone();
                        store.clear().expect("failed to clear store");

                        for key in &keys {
                            for _ in 0..iters {
                                store
                                    .list_push_front(&key, val.clone())
                                    .expect("failed to push");
                            }
                        }

                        let start = Instant::now();

                        let countdown = Arc::new(AtomicU64::new(iters));
                        let handles = keys
                            .into_iter()
                            .map(|key| {
                                let store = store.clone();
                                let countdown = countdown.clone();
                                std::thread::spawn(move || {
                                    while countdown.fetch_sub(1, Ordering::AcqRel) <= iters {
                                        store.list_pop_back(&key).expect("failed to push");
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        for handle in handles {
                            handle.join().unwrap()
                        }

                        start.elapsed()
                    })
                });

                group.bench_function("list pop front after push front", |b| {
                    b.iter_custom(|iters| {
                        let keys = keys.clone();
                        store.clear().expect("failed to clear store");

                        for key in &keys {
                            for _ in 0..iters {
                                store
                                    .list_push_front(&key, val.clone())
                                    .expect("failed to push");
                            }
                        }

                        let start = Instant::now();

                        let countdown = Arc::new(AtomicU64::new(iters));
                        let handles = keys
                            .into_iter()
                            .map(|key| {
                                let store = store.clone();
                                let countdown = countdown.clone();
                                std::thread::spawn(move || {
                                    while countdown.fetch_sub(1, Ordering::AcqRel) <= iters {
                                        store.list_pop_front(&key).expect("failed to push");
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        for handle in handles {
                            handle.join().unwrap()
                        }

                        start.elapsed()
                    })
                });

                group.bench_function("list pop front after push back", |b| {
                    b.iter_custom(|iters| {
                        let keys = keys.clone();
                        store.clear().expect("failed to clear store");

                        for key in &keys {
                            for _ in 0..iters {
                                store
                                    .list_push_back(&key, val.clone())
                                    .expect("failed to push");
                            }
                        }

                        let start = Instant::now();

                        let countdown = Arc::new(AtomicU64::new(iters));
                        let handles = keys
                            .into_iter()
                            .map(|key| {
                                let store = store.clone();
                                let countdown = countdown.clone();
                                std::thread::spawn(move || {
                                    while countdown.fetch_sub(1, Ordering::AcqRel) <= iters {
                                        store.clone().list_pop_front(&key).expect("failed to push");
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        for handle in handles {
                            handle.join().unwrap()
                        }

                        start.elapsed()
                    })
                });
            }
        }
    }
}

criterion_group!(lists, list_ops);
criterion_main!(lists);

use criterion::*;
use std::time::Instant;

pub struct TempDb {
    conn: sledis::Conn,
    _dir: tempfile::TempDir,
}

impl TempDb {
    pub fn new() -> Self {
        let _dir = tempfile::tempdir().expect("failed to create tempdir");
        let conn = sledis::Conn::open(_dir.path()).expect("failed to open db");
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
const VAL_SIZES: &[usize] = &[8];

// const KEY_SIZES: &[usize] = &[8, 32, 128, 512, 2048];
// const VAL_SIZES: &[usize] = &[8, 32, 128, 512, 2048];

fn serial_ops(c: &mut Criterion) {
    for key_size in KEY_SIZES {
        for val_size in VAL_SIZES {
            let mut group = c.benchmark_group(&format!(
                "serial ops, key size: {}, val size: {}",
                key_size, val_size
            ));

            let key = vec![1u8; *key_size];
            let val = vec![1u8; *val_size];

            let store = TempDb::new();

            group.bench_function("serial push back", |b| {
                b.iter_custom(|iters| {
                    store.clear().expect("failed to clear store");
                    let start = Instant::now();
                    for _ in 0..iters {
                        store
                            .list_push_back(&key, val.as_slice().into())
                            .expect("failed to push");
                    }
                    start.elapsed()
                })
            });

            group.bench_function("serial push front", |b| {
                b.iter_custom(|iters| {
                    store.clear().expect("failed to clear store");
                    let start = Instant::now();
                    for _ in 0..iters {
                        store
                            .list_push_front(&key, val.as_slice().into())
                            .expect("failed to push");
                    }
                    start.elapsed()
                })
            });

            group.bench_function("serial pop back after push back", |b| {
                b.iter_custom(|iters| {
                    store.clear().expect("failed to clear store");
                    for _ in 0..iters {
                        store
                            .list_push_back(&key, val.as_slice().into())
                            .expect("failed to push");
                    }

                    let start = Instant::now();

                    for _ in 0..iters {
                        store
                            .list_pop_back(&key)
                            .expect("failed to pop")
                            .expect("missing value");
                    }
                    start.elapsed()
                })
            });

            group.bench_function("serial pop back after push front", |b| {
                b.iter_custom(|iters| {
                    store.clear().expect("failed to clear store");
                    for _ in 0..iters {
                        store
                            .list_push_front(&key, val.as_slice().into())
                            .expect("failed to push");
                    }

                    let start = Instant::now();

                    for _ in 0..iters {
                        store
                            .list_pop_back(&key)
                            .expect("failed to pop")
                            .expect("missing value");
                    }
                    start.elapsed()
                })
            });

            group.bench_function("serial pop front after push front", |b| {
                b.iter_custom(|iters| {
                    store.clear().expect("failed to clear store");
                    for _ in 0..iters {
                        store
                            .list_push_front(&key, val.as_slice().into())
                            .expect("failed to push");
                    }

                    let start = Instant::now();

                    for _ in 0..iters {
                        store
                            .list_pop_front(&key)
                            .expect("failed to pop")
                            .expect("missing value");
                    }
                    start.elapsed()
                })
            });

            group.bench_function("serial pop front after push back", |b| {
                b.iter_custom(|iters| {
                    store.clear().expect("failed to clear store");
                    for _ in 0..iters {
                        store
                            .list_push_back(&key, val.as_slice().into())
                            .expect("failed to push");
                    }

                    let start = Instant::now();

                    for _ in 0..iters {
                        store
                            .list_pop_front(&key)
                            .expect("failed to pop")
                            .expect("missing value");
                    }
                    start.elapsed()
                })
            });
        }
    }
}

criterion_group!(lists, serial_ops);
criterion_main!(lists);

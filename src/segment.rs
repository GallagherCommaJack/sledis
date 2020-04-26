use std::ops::{Deref, DerefMut};

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct Segment<Inner = sled::IVec> {
    inner: Inner,
    start: usize,
    end: usize,
}

impl<Inner: AsRef<[u8]>> AsRef<[u8]> for Segment<Inner> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.guard_validate();
        unsafe { self.inner.as_ref().get_unchecked(self.start..self.end) }
    }
}

impl<Inner: AsRef<[u8]>> Deref for Segment<Inner> {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<Inner: AsRef<[u8]> + DerefMut<Target = [u8]>> DerefMut for Segment<Inner> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.guard_validate();
        unsafe {
            self.inner
                .deref_mut()
                .get_unchecked_mut(self.start..self.end)
        }
    }
}

impl<Inner: AsRef<[u8]>> Segment<Inner> {
    #[inline(always)]
    pub fn new(inner: Inner) -> Self {
        let end = inner.as_ref().len();
        Self {
            inner,
            start: 0,
            end,
        }
    }

    #[inline(always)]
    fn guard_validate(&self) {
        debug_assert!(self.start <= self.inner.as_ref().len());
        debug_assert!(self.end <= self.inner.as_ref().len());
    }
}

impl<Inner: AsRef<[u8]> + Clone> Segment<Inner> {
    #[inline(always)]
    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(at <= self.len());

        let new = Segment {
            inner: self.inner.clone(),
            start: self.start + at,
            end: self.end,
        };

        self.end = self.start + at;

        new
    }
}

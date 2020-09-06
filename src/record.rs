use sled::IVec;
use std::{
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
};
use thiserror::*;

#[repr(u8)]
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum Tag {
    Blob = 0,
    Table = 1,
    List = 2,
}

impl TryFrom<u8> for Tag {
    type Error = RecordError;
    fn try_from(inp: u8) -> Result<Self, Self::Error> {
        match inp {
            0 => Ok(Tag::Blob),
            1 => Ok(Tag::Table),
            2 => Ok(Tag::List),
            _ => Err(RecordError::BadTag),
        }
    }
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Record {
    FromRaw(Tag, sled::IVec),
    FromData(Tag, sled::IVec),
}

impl Record {
    pub fn tag(&self) -> Tag {
        match self {
            Record::FromRaw(tag, _) => *tag,
            Record::FromData(tag, _) => *tag,
        }
    }

    pub fn data(&self) -> IVec {
        match self {
            Self::FromRaw(_, iv) => iv.subslice(1, iv.len() - 1),
            Self::FromData(_, iv) => iv.clone(),
        }
    }

    pub(crate) fn into_raw(self) -> IVec {
        match self {
            Record::FromRaw(_, iv) => iv,
            Record::FromData(tag, iv) => {
                // TODO: avoid heap allocation for short ivec's?
                // TODO: profile to figure out how much I care
                let mut out = Vec::with_capacity(1 + iv.len());
                out.push(tag as u8);
                out.extend_from_slice(&iv);
                out.into()
            }
        }
    }

    pub(crate) fn decode(iv: IVec) -> Result<Self, RecordError> {
        if iv.is_empty() {
            return Err(RecordError::EmptyInput);
        }

        Ok(Self::FromRaw(iv[0].try_into()?, iv))
    }
}

impl AsRef<[u8]> for Record {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::FromRaw(_, iv) => &iv.as_ref()[1..],
            Self::FromData(_, iv) => iv.as_ref(),
        }
    }
}

impl AsMut<[u8]> for Record {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Self::FromRaw(_, iv) => &mut iv.as_mut()[1..],
            Self::FromData(_, iv) => iv.as_mut(),
        }
    }
}

impl Deref for Record {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::FromRaw(_, iv) => &iv.as_ref()[1..],
            Self::FromData(_, iv) => iv.as_ref(),
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::FromRaw(_, iv) => &mut iv.as_mut()[1..],
            Self::FromData(_, iv) => iv.as_mut(),
        }
    }
}

#[derive(Error, Debug)]
pub enum RecordError {
    #[error("empty input")]
    EmptyInput,
    #[error("bad tag")]
    BadTag,
}

use super::*;

#[derive(Default, Clone, Eq, PartialEq, Debug)]
pub struct Meta {
    pub len: u64,
    pub lowest_key: Option<Segment>,
    pub highest_key: Option<Segment>,
}

impl Meta {
    pub fn encode(&self) -> Vec<u8> {
        let len_lowest = self
            .lowest_key
            .as_ref()
            .map_or_else(|| 0, |k| escaped_size(k.as_ref()));

        let len_highest = self.highest_key.as_ref().map_or_else(|| 0, |k| k.len());

        let capacity = 8 + len_lowest + 2 + len_highest + 2;

        let mut out = Vec::with_capacity(capacity);

        out.extend_from_slice(&self.len.to_be_bytes());

        if let Some(key) = self.lowest_key.as_ref() {
            escape_into(key.as_ref(), &mut out);
        }

        out.extend_from_slice(&TERMINATOR);

        if let Some(key) = self.highest_key.as_ref() {
            escape_into(key.as_ref(), &mut out);
        }

        out.extend_from_slice(&TERMINATOR);

        out
    }

    pub fn decode(mut input: Segment) -> Option<Self> {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(input.get(0..8)?);
        let len = u64::from_be_bytes(buf);

        if input.len() == 8 {
            return Some(Meta {
                len,
                lowest_key: None,
                highest_key: None,
            });
        }

        let first_terminator = find_terminator(input.get(8..)?).ok()?;
        let mut rest_with_terminator = input.split_off(first_terminator);
        let mut rest = rest_with_terminator.split_off(2);

        let lowest_key = Some(input);

        let highest_key = if rest.is_empty() {
            None
        } else if rest.len() < 2 {
            return None;
        } else {
            if rest.split_off(rest.len() - 2).as_ref() != &TERMINATOR {
                return None;
            }
            Some(rest)
        };

        Some(Meta {
            len,
            lowest_key,
            highest_key,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn lowest_key(&self) -> Option<Segment> {
        self.lowest_key.clone()
    }

    pub fn highest_key(&self) -> Option<Segment> {
        self.highest_key.clone()
    }
}

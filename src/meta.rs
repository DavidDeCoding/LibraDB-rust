use crate::consts::{PAGE_SIZE, PAGE_ID_SIZE};

pub const META_PAGE_NUM: u64 = 0;

#[derive(Debug)]
pub struct Meta {
    pub root: u64,
    pub freelist_page: u64,
}

impl Meta {

    pub fn new() -> Meta {
        Meta {
            root: u64::MAX,
            freelist_page: u64::MAX
        }
    }

    pub fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut data: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];

        let mut pos = 0;
        data[pos..pos+PAGE_ID_SIZE].clone_from_slice(&self.root.to_le_bytes());
        pos += PAGE_ID_SIZE;

        data[pos..pos+PAGE_ID_SIZE].clone_from_slice(&self.freelist_page.to_le_bytes());

        data
    }

    pub fn deserialize(buf: [u8; PAGE_SIZE]) -> Meta {
        let mut pos = 0;

        let mut u64_bytes: [u8;PAGE_ID_SIZE] = [0u8;PAGE_ID_SIZE];
        for n in 0..PAGE_ID_SIZE {
            u64_bytes[n] = buf[pos+n];
        }

        let root = u64::from_le_bytes(u64_bytes);
        pos += PAGE_ID_SIZE;

        for n in 0..PAGE_ID_SIZE {
            u64_bytes[n] = buf[pos+n];
        }
        let freelist_page = u64::from_le_bytes(u64_bytes);

        Meta {
            root,
            freelist_page
        }
    }
}


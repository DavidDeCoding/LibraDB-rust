use crate::{consts::{PAGE_ID_SIZE, PAGE_SIZE}, meta::META_PAGE_NUM};

#[derive(Debug)]
pub struct Freelist {
    pub max_page: u64,
    pub released_pages: Vec<u64>,
}

impl Freelist {

    pub fn new() -> Freelist {
        Freelist {
            max_page: META_PAGE_NUM,
            released_pages: vec![]
        }
    }

    pub fn get_next_page(&mut self) -> u64 {
        match self.released_pages.pop() {
            Some(page_id) => page_id,
            None => {
                self.max_page += 1;
                self.max_page
            }
        }
    }

    pub fn release_page(&mut self, page_id: u64) {
        self.released_pages.push(page_id)
    }

    pub fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut data: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];

        let mut pos = 0;
        data[pos..pos+PAGE_ID_SIZE].clone_from_slice(&self.max_page.to_le_bytes());
        pos += PAGE_ID_SIZE;

        data[pos..pos+PAGE_ID_SIZE].clone_from_slice(&self.released_pages.len().to_le_bytes());
        pos += PAGE_ID_SIZE;

        for page_id in 0..self.released_pages.len() {
            data[pos..pos+PAGE_ID_SIZE].clone_from_slice(&self.released_pages[page_id].to_le_bytes());
            pos += PAGE_ID_SIZE;
        }

        data
    }

    pub fn deserialize(buf: [u8; PAGE_SIZE]) -> Freelist {
        let mut pos = 0;

        let mut u64_bytes = [0u8; PAGE_ID_SIZE];
        for i in 0..PAGE_ID_SIZE {
            u64_bytes[i] = buf[pos+i];
        }
        pos += PAGE_ID_SIZE;
        let max_page = u64::from_le_bytes(u64_bytes);

        for i in 0..PAGE_ID_SIZE {
            u64_bytes[i] = buf[pos+i];
        }
        pos += PAGE_ID_SIZE;
        let released_page_count = usize::from_le_bytes(u64_bytes);

        let mut released_pages = vec![];
        for i in 0..released_page_count {
            for i in 0..PAGE_ID_SIZE {
                u64_bytes[i] = buf[pos+i];
            }
            pos += PAGE_ID_SIZE;
            released_pages.push(u64::from_le_bytes(u64_bytes));
        }

        Freelist {
            max_page,
            released_pages
        }

    }
}
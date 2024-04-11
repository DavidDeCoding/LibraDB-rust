use crate::{consts::PAGE_SIZE, error::CustomError, freelist::Freelist, meta::{Meta, META_PAGE_NUM}, node::Node};
use std::{fs::File, io::{Read, Seek, SeekFrom, Write}};
use std::path::Path;

#[derive(Clone)]
pub struct Options {
    pub page_size: usize,
    pub min_fill_percent: f32,
    pub max_fill_percent: f32,
    pub path: &'static str,
}

pub const DEFAULT_OPTIONS: Options = Options {
    page_size: PAGE_SIZE,
    min_fill_percent: 0.5,
    max_fill_percent: 0.95,
    path: "./db"
};

#[derive(Debug)]
pub struct Page {
    id: u64,
    pub data: [u8; PAGE_SIZE],
}

#[derive(Debug)]
pub struct DAL {
    file: File,
    page_size: usize,
    min_fill_percent: f32,
    max_fill_percent: f32,

    pub meta: Option<Meta>,
    pub freelist: Option<Freelist>
}

impl DAL {
    pub fn new_dal(options: Options) -> Result<DAL, CustomError> {
        let path = options.path;
        if !Path::new(&path).exists() {
            match File::create(path) {
                Ok(file) => {
                    let mut dal = DAL {
                        file,
                        page_size: options.page_size,
                        min_fill_percent: options.min_fill_percent,
                        max_fill_percent: options.max_fill_percent,
                        meta: Some(Meta::new()),
                        freelist: Some(Freelist::new()),
                    };
                    let page_id: u64; 
                    match dal.get_next_page() {
                        Ok(_page_id) => {
                            page_id = _page_id
                        }
                        Err(error) => {
                            return Err(error);
                        }
                    }
                    
                    match dal.meta {
                        Some(ref mut meta) => {
                            meta.freelist_page = page_id;
                        }
                        None => {
                            return Err(CustomError::new("Meta not created correctly".to_string()));
                        }
                    }
                    match dal.write_freelist() {
                        Ok(_) => {},
                        Err(error) => {
                            return Err(error);
                        }
                    }
                    
                    let mut root_collection = Node::new(u64::MAX, vec![], vec![]);
                    match dal.write_node(&mut root_collection) {
                        Ok(()) => {
                            match dal.meta {
                                Some(ref mut meta) => {
                                    meta.root = root_collection.page_id;
                                }
                                None => {
                                    return Err(CustomError::new("Meta not created correctly".to_string()));
                                }
                            }
                        }
                        Err(error) => {
                            return Err(error);
                        }
                    }

                    match dal.write_meta() {
                        Ok(_) => {},
                        Err(error) => {
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    return Err(CustomError::new(error.to_string()));
                }
            }
        }
        
        match File::options().read(true).write(true).open(path) {
            Ok(file) => {
                let mut dal = DAL {
                    file,
                    page_size: options.page_size,
                    min_fill_percent: options.min_fill_percent,
                    max_fill_percent: options.max_fill_percent,
                    meta: None,
                    freelist: None,
                };

                match dal.read_meta() {
                    Ok(meta) => {
                        dal.meta = Some(meta);
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
                
                match dal.read_freelist() {
                    Ok(freelist) => {
                        dal.freelist = Some(freelist);
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }

                Ok(dal)
            }
            Err(error) => {
                return Err(CustomError::new(error.to_string()));
            }
        }
    }

    pub fn get_next_page(&mut self) -> Result<u64, CustomError> {
        match self.freelist {
            Some(ref mut freelist) => Ok(freelist.get_next_page()),
            None => Err(CustomError::new("Freelist not initialized".to_string()))
        }
    }

    fn read_meta(&self) -> Result<Meta, CustomError> {
        match self.read_page(META_PAGE_NUM) {
            Ok(page) => Ok(Meta::deserialize(page.data)),
            Err(error) => Err(error)
        }
    }

    pub fn write_meta(&self) -> Result<Page, CustomError> {
        let mut page = self.allocate_empty_page();
        page.id = META_PAGE_NUM;
        match self.meta.as_ref() {
            Some(meta) => {
                page.data = meta.serialize();

                match self.write_page(&page) {
                    Ok(()) => Ok(page),
                    Err(error) => Err(error)
                }
            }
            None => Err(CustomError::new("Meta not set before writing Meta".to_string()))
        }
    }

    fn read_freelist(&self) -> Result<Freelist, CustomError> {
        match self.meta {
            Some(ref meta) => {
                match self.read_page(meta.freelist_page) {
                    Ok(page) => Ok(Freelist::deserialize(page.data)),
                    Err(error) => Err(error)
                }
            }
            None => Err(CustomError::new("Meta not initialized before accessing Freelist".to_string()))
        }
    }

    pub fn write_freelist(&self) -> Result<Page, CustomError> {
        let mut page = self.allocate_empty_page();
        match self.meta.as_ref() {
            Some(meta) => {
                page.id = meta.freelist_page;
                
                match self.freelist.as_ref() {
                    Some(freelist) => {
                        page.data = freelist.serialize();
                    }
                    None => {
                        return Err(CustomError::new("Freelist not initialized".to_string()));
                    }
                }

                match self.write_page(&page) {
                    Ok(()) => Ok(page),
                    Err(error) => Err(error)
                }
            }
            None => {
                return Err(CustomError::new("Meta not initialized before writing Freelist".to_string()));
            }
        }
    }

    fn allocate_empty_page(&self) -> Page {
        Page {
            id: u64::MAX,
            data: [0u8; PAGE_SIZE]
        }
    }

    pub fn read_page(&self, page_id: u64) -> Result<Page, CustomError> {
        match self.file.try_clone() {
            Ok(mut file) => {
                let mut page = self.allocate_empty_page();
                page.id = page_id;

                let offset = page_id * (self.page_size as u64);
                match file.seek(SeekFrom::Start(offset)) {
                    Ok(_) => {
                        match file.read_exact(&mut page.data) {
                            Ok(()) => Ok(page),
                            Err(error) => {
                                Err(CustomError::new(error.to_string()))
                            }
                        }
                    }
                    Err(error) => Err(CustomError::new(error.to_string()))
                }
            }
            Err(error) => Err(CustomError::new(error.to_string()))
        }
        
    }

    fn write_page(&self, page: &Page) -> Result<(), CustomError> {
        match self.file.try_clone() {
            Ok(mut file) => {
                let offset = page.id * (self.page_size as u64);
                match file.seek(SeekFrom::Start(offset)) {
                    Ok(_) => {
                        match file.write_all(&page.data) {
                            Ok(()) => Ok(()),
                            Err(error) => Err(CustomError::new(error.to_string()))
                        }
                    }
                    Err(error) => Err(CustomError::new(error.to_string()))
                }
            }
            Err(error) => Err(CustomError::new(error.to_string()))
        }
        
    }

    pub fn max_threshold(&self) -> f32 {
        self.max_fill_percent * (self.page_size as f32)
    }

    pub fn min_threshold(&self) -> f32 {
        self.min_fill_percent * (self.page_size as f32)
    }

    pub fn get_node(&self, page_id: u64) -> Result<Node, CustomError> {
        match self.read_page(page_id) {
            Ok(page) => {
                match Node::deserialize(page.data) {
                    Ok(node) => {
                        let mut node = node;
                        node.page_id = page_id;
                        Ok(node)
                    }
                    Err(error) => {
                        Err(error)
                    }
                }
                
            }
            Err(error) => Err(error)
        }
    }

    pub fn write_node(&mut self, node: &mut Node) -> Result<(), CustomError> {
        let mut page = self.allocate_empty_page();
        if node.page_id == u64::MAX {
            let page_id: u64;
            match self.get_next_page() {
                Ok(_page_id) => {
                    page_id = _page_id;
                }
                Err(error) => {
                    return Err(error);
                }
            }
            page.id = page_id;
            node.page_id = page_id;
        } else {
            page.id = node.page_id;
        }

        page.data = node.serialize();

        self.write_page(&page)
    }

    pub fn delete_node(&mut self, node: &Node) {
        match self.freelist {
            Some(ref mut freelist) => {
                freelist.release_page(node.page_id)
            }
            None => {
                panic!("Invalid node with page_id: {}, cannot be deleted.",  node.page_id);
            }
        }
    }

    pub fn is_over_populated(&self, node: &Node) -> bool {
        (node.node_size() as f32) > self.max_threshold()
    }

    pub fn is_under_populated(&self, node: &Node) -> bool {
        (node.node_size() as f32) < self.min_threshold()
    }

    pub fn get_split_index(&self, node: &Node) -> usize {
        let mut size = 0;
        size += 3; 

        for i in 0..node.items.len() {
            size += node.element_size(i);

            if size as f32 > self.min_threshold() && i < node.items.len() - 1 {
                return i + 1;
            }
        }

        usize::MAX
    }


}

#[cfg(test)]
mod tests {
    use super::{Options, DAL, DEFAULT_OPTIONS};
    use std::{fs, path::Path};


    #[test]
    fn new_dal_created_and_read() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_dal_test_internal"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }
        
        match DAL::new_dal(options.clone()) {
            Ok(dal) => {
                assert_eq!(dal.meta.unwrap().freelist_page, 1);
            }
            Err(_) => assert!(false, "dal failed to create!!!")
        }

        match DAL::new_dal(options) {
            Ok(ref mut dal) => {
                assert_eq!(dal.meta.as_ref().unwrap().freelist_page, 1);
            }
            Err(_) => assert!(false, "dal failed to create!!!")
        }

    }
}

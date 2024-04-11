use std::{collections::HashMap, sync::{RwLockReadGuard, RwLockWriteGuard}};
use crate::{collection::Collection, db::DB, dal::DAL, error::CustomError, node::{Item, Node}};

pub struct Tx<'a> {
    pub dal: RwLockReadGuard<'a, DAL>,
}

impl <'a> Tx<'a> {
    pub fn new(db: &'a DB) -> Tx<'a> {
        let dal = db.dal.read().unwrap();
        Tx {
            dal,
        }
    }

    pub fn new_node(&mut self, items: Vec<Item>, child_nodes: Vec<u64>) -> Result<Node, CustomError> {
        Err(CustomError::new("Readable Transaction cannot create new node".to_string()))
    }

    pub fn get_node(&self, page_id: u64) -> Result<Node, CustomError> {
        self.dal.get_node(page_id)
    }

    pub fn write_node(&mut self, node: &mut Node) -> Result<(), CustomError> {
        Err(CustomError::new("Readable Transaction cannot write node".to_string()))
    }

    pub fn delete_node(&mut self, node: &Node) -> Result<(), CustomError> {
        Err(CustomError::new("Readable Transaction cannot write node".to_string()))
    }

    pub fn rollback(self) -> Result<(), CustomError> {
        drop(self.dal);

        Ok(())
    }

    pub fn commit(self) -> Result<(), CustomError> {
        drop(self.dal);
        
        Ok(())
    }

    pub fn get_root_collection(&self) -> Result<Collection, CustomError> {
        match self.dal.meta {
            Some(ref meta) => Ok(Collection::new("".to_string(), meta.root)),
            None => {
                return Err(CustomError::new("Meta not initialized".to_string()));
            }
        } 
    }

    pub fn get_collection(&self, name: String) -> Result<Option<Collection>, CustomError> {
        match self.get_root_collection() {
            Ok(mut root_collection) => {
                match root_collection.find(name, self) {
                    Ok(Some(item)) => Ok(Some(Collection::deserialize(item))),
                    Ok(None) => Ok(None),
                    Err(error) => Err(error)
                }
            }
            Err(error) => Err(error)
        }
    }

    pub fn create_collection(&mut self, name: String) -> Result<Collection, CustomError> {
        Err(CustomError::new("Readable Transaction cannot create collection".to_string()))
    }

    pub fn delete_collection(&mut self, name: String) -> Result<(), CustomError> {
        Err(CustomError::new("Readable Transaction cannot create collection".to_string()))
    }


}

pub struct TxMut<'a> {
    dirty_nodes: HashMap<u64, Node>,
    pages_to_delete: Vec<u64>,

    allocated_page_ids: Vec<u64>,

    pub dal: RwLockWriteGuard<'a, DAL>,
}

impl<'a> TxMut<'a> {
    pub fn new(db: &'a DB) -> TxMut<'a> {
        let dal = db.dal.write().unwrap();
        TxMut {
            dirty_nodes: HashMap::new(),
            pages_to_delete: vec![],
            allocated_page_ids: vec![],

            dal
        }  
    }

    pub fn new_node(&mut self, items: Vec<Item>, child_nodes: Vec<u64>) -> Result<Node, CustomError> {
        match self.dal.get_next_page() {
            Ok(page_id) => Ok(Node::new(page_id, items, child_nodes)),
            Err(error) => {
                panic!("Error creating new node: {:?}", error);
            }
        }
        
    }

    pub fn get_node(&self, page_id: u64) -> Result<Node, CustomError> {
        match self.dirty_nodes.get(&page_id) {
            Some(node) => Ok((*node).clone()),
            None => self.dal.get_node(page_id)
        }
    }

    pub fn write_node(&mut self, node: &mut Node) -> Result<(), CustomError> {
        self.dirty_nodes.insert(node.page_id, (*node).clone());
        Ok(())
    }

    pub fn delete_node(&mut self, node: &Node) {
        self.pages_to_delete.push(node.page_id);
    }

    pub fn rollback(mut self) -> Result<(), CustomError> {
        self.dirty_nodes.drain();
        self.pages_to_delete.drain(0..);
        while let Some(page_id) = self.allocated_page_ids.pop() {
            match self.dal.freelist {
                Some(ref mut freelist) => {
                    freelist.release_page(page_id)
                }
                None => {
                    return Err(CustomError::new("Freelist not initialized".to_string()));
                }
            }
        }

        drop(self.dal);

        Ok(())
    }

    pub fn commit(mut self) -> Result<(), CustomError> {
        for node in self.dirty_nodes.values_mut() {
            match self.dal.write_node(node) {
                Ok(()) => {}
                Err(error) => {
                    return Err(error);
                }
            }
        }

        while let Some(page_id) = self.pages_to_delete.pop() {
            match self.dal.freelist {
                Some(ref mut freelist) => {
                    freelist.release_page(page_id)
                }
                None => {
                    return Err(CustomError::new("Freelist not initialized".to_string()));
                }
            }
        }

        match self.dal.write_freelist() {
            Ok(_) => {}
            Err(error) => {
                return Err(error);
            }
        }

        self.dirty_nodes.drain();
        self.allocated_page_ids.drain(0..);

        drop(self.dal);
        
        Ok(())
    }

    pub fn get_root_collection(&mut self) -> Result<Collection, CustomError> {
        match self.dal.meta {
            Some(ref meta) => Ok(Collection::new("".to_string(), meta.root)),
            None => {
                return Err(CustomError::new("Meta not initialized".to_string()));
            }
        } 
    }

    pub fn get_collection(&mut self, name: String) -> Result<Option<Collection>, CustomError> {
        match self.get_root_collection() {
            Ok(root_collection) => {
                match root_collection.find_mut(name, self) {
                    Ok(Some(item)) => Ok(Some(Collection::deserialize(item))),
                    Ok(None) => Ok(None),
                    Err(error) => Err(error)
                }
            }
            Err(error) => Err(error)
        }
    }

    pub fn create_collection(&mut self, name: String) -> Result<Collection, CustomError> {
        let mut node;
        match self.new_node(vec![], vec![]) {
            Ok(_node) => {
                node = _node; 
            }
            Err(error) => {
                return Err(error);
            }
        } 
        match self.write_node(&mut node) {
            Ok(()) => {
                let collection = Collection::new(name, node.page_id);
                self.write_new_collection(collection)
            }
            Err(error) => Err(error)
        }
    }

    fn write_new_collection(&mut self, collection: Collection) -> Result<Collection, CustomError>  {
        let mut collection = collection;
        let collection_in_bytes_item = collection.serialize();

        match self.get_root_collection() {
            Ok(mut root_collection) => {
                match root_collection.put(collection.name.clone(), collection_in_bytes_item.value, self) {
                    Ok(()) => Ok(collection),
                    Err(error) => Err(error)
                }

            }
            Err(error) => Err(error)
        }
    }

    pub fn delete_collection(&mut self, name: String) -> Result<(), CustomError> {
        match self.get_root_collection() {
            Ok(mut root_collection) => root_collection.remove(name, self),
            Err(error) => Err(error)
        }
    }

}

#[cfg(test)]
mod tests {
    use crate::{collection::Collection, dal::{Options, DEFAULT_OPTIONS}, db::DB, node::Item};
    use std::{fs, path::Path, sync::Arc, thread};

    #[test]
    fn create_collection() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_tx_test_internal_1"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }

        match DB::open(options) {
            Ok(db) => {
                let mut tx = db.write_tx();

                match tx.create_collection("test_collection".to_string()) {
                    Ok(collection) => {
                        assert_eq!(collection.name, "test_collection".to_string());
                    }
                    Err(error) => {
                        assert!(false, "Collection creation failed with error: {:?}", error);
                    }
                }

                match tx.commit() {
                    Ok(()) => {}
                    Err(error) => {
                        assert!(false, "Transaction failed to commit with error: {:?}", error);
                    }
                }

                let tx = db.read_tx();

                match tx.get_collection("test_collection".to_string()) {
                    Ok(collection) => {
                        assert_eq!(collection.is_some(), true);
                        assert_eq!(collection.unwrap().name, "test_collection".to_string());
                    }
                    Err(error) => {
                        assert!(false, "Get collection failed with error : {:?}", error);
                    }
                }

                match tx.commit() {
                    Ok(()) => {},
                    Err(error) => {
                        assert!(false, "Transaction commit failed with error: {:?}", error);
                    }
                }

            }
            Err(_) => {
                assert!(false, "DB not created successfully!")
            }
        }
    }

    #[test]
    fn open_multiple_read_simultaneously() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_tx_test_internal_2"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }

        match DB::open(options) {
            Ok(db) => {
                let tx1 = db.read_tx();
                let tx2 = db.read_tx();

                match tx1.get_collection("non_existing_collection".to_string()) {
                    Ok(_) => {}
                    Err(error) => {
                        assert!(false, "Get Collection failed with error: {:?}", error);
                    }
                }

                match tx2.get_collection("non_existing_collection".to_string()) {
                    Ok(_) => {}
                    Err(error) => {
                        assert!(false, "Get Collection failed with error: {:?}", error);
                    }
                }

                tx1.commit();
                tx2.commit();
            }
            Err(_) => {
                assert!(false, "DB not created successfully!")
            }
        }
    }

    #[test]
    fn open_read_and_write_tx_simultaneously() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_tx_test_internal_3"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }

        match DB::open(options) {
            Ok(db) => {
                let db = Arc::new(db);

                let tx = db.read_tx();

                let db_clone = Arc::clone(&db);
                let t1 = thread::spawn(move || {
                    let mut tx = db_clone.write_tx();
                    
                
                    let db_new_clone = Arc::clone(&db_clone);
                    let t2 = thread::spawn(move|| {
                        let tx = db_new_clone.read_tx();

                        match tx.get_collection("test_collection".to_string()) {
                            Ok(Some(collection)) => {
                                assert_eq!(collection.name, "test_collection".to_string());
                            }
                            Ok(None) => {
                                assert!(false, "Get collection returned nothing");
                            }
                            Err(error) => {
                                assert!(false, "Get collection failed with {:?}", error);
                            }
                        }

                        match tx.commit() {
                            Ok(()) => {}
                            Err(error) => {
                                assert!(false, "Failed with error: {:?}", error);
                            }
                        }
                    });

                    match tx.create_collection("test_collection".to_string()) {
                        Ok(_) => {}
                        Err(error) => {
                            assert!(false, "Failed to create collection with error: {:?}", error);
                        }
                    }

                    tx.commit();
                    
                    t2.join().unwrap();
                });

                match tx.get_collection("test_collection".to_string()) {
                    Ok(Some(_)) => {
                        assert!(false, "Found unknown collection");
                    }
                    Ok(None) => {}
                    Err(error) => {
                        assert!(false, "Get collection failed with error: {:?}", error);
                    }
                }

                tx.commit();

                t1.join().unwrap();
            }
            Err(_) => {
                assert!(false, "DB not created successfully!")
            }
        }
    }

    #[test]
    fn rollback_test() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_tx_test_internal_4"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }

        match DB::open(options) {
            Ok(db) => {
                let mut tx = db.write_tx();

                let mut child_0;
                match tx.new_node(vec![Item::new("1".to_string(), "1".to_string()), Item::new("2".to_string(), "2".to_string())], vec![]) {
                    Ok(node) => {
                        child_0 = node;
                    }
                    Err(error) => {
                        panic!("Failed to create new node: {:?}", error);
                    }
                }
                match tx.write_node(&mut child_0) {
                    Ok(()) => {}
                    Err(error) => {
                        assert!(false, "Failed to write node: {:?}", error);
                    }
                }

                let mut child_1; 
                match tx.new_node(vec![Item::new("4".to_string(), "4".to_string()), Item::new("5".to_string(), "5".to_string())], vec![]) {
                    Ok(node) => {
                        child_1 = node;
                    }
                    Err(error) => {
                        panic!("Failed to create new node: {:?}", error);
                    }
                }
                match tx.write_node(&mut child_1) {
                    Ok(()) => {}
                    Err(error) => {
                        assert!(false, "Failed to write node: {:?}", error);
                    }
                }

                let mut root;
                match tx.new_node(vec![Item::new("3".to_string(), "3".to_string())], vec![child_0.page_id, child_1.page_id]) {
                    Ok(node) => {
                        root = node;
                    }
                    Err(error) => {
                        panic!("Root node failed to create: {:?}", error);
                    }
                }
                match tx.write_node(&mut root) {
                    Ok(()) => {}
                    Err(error) => {
                        assert!(false, "Failed to write node: {:?}", error);
                    }
                }

                match tx.write_new_collection(Collection::new("test_collection".to_string(), root.page_id)) {
                    Ok(_) => {}
                    Err(error) => {
                        assert!(false, "Failed to create collection with error: {:?}", error);
                    } 
                }

                match tx.commit() {
                    Ok(()) => {}
                    Err(error) => {
                        assert!(false, "Failed to commit the transaction with error: {:?}", error);
                    }
                }

                let mut tx2 = db.write_tx();

                match tx2.get_collection("test_collection".to_string()) {
                    Ok(Some(mut collection)) => {
                        let item = Item::new("9".to_string(), "9".to_string());
                        match collection.put(item.key, item.value, &mut tx2) {
                            Ok(()) => {},
                            Err(error) => {
                                assert!(false, "Failed to add item to the collection with error: {:?}", error);
                            }
                        }

                        match tx2.rollback() {
                            Ok(()) => {}
                            Err(error) => {
                                assert!(false, "Failed to rollback transaction with error: {:?}", error);
                            }
                        }
                    }
                    Ok(None) => {
                        assert!(false, "Failed to get collection: test_collection");
                    }
                    Err(error) => {
                        assert!(false, "Failed to get collection with error: {:?}", error);
                    }
                }

                let mut tx3 = db.read_tx();

                match tx3.get_collection("test_collection".to_string()) {
                    Ok(Some(collection)) => {
                        match collection.find("9".to_string(), &mut tx3) {
                            Ok(Some(item)) => {
                                assert!(false, "Item {:?} was found, rollback failed", item);
                            }
                            Ok(None) => {}
                            Err(error) => {
                                assert!(false, "Failed to get Item with error: {:?}", error);
                            }
                        }
                    }
                    Ok(None) => {
                        assert!(false, "Failed to get collection: test_collection");
                    }
                    Err(error) => {
                        assert!(false, "Failed to get collection with error: {:?}", error);
                    }
                }

                tx3.commit();
            }
            Err(_) => {
                assert!(false, "DB not created successfully!")
            }
        }
    }
}
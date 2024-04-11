use crate::consts::{COLLECTION_SIZE, PAGE_ID_SIZE};
use crate::error::CustomError;
use crate::node::{Item, Node};
use crate::tx::{Tx, TxMut};

#[derive(Debug)]
pub struct Collection {
    pub name: String,
    root: u64,
    counter: u64
}

impl Collection  {

    pub fn new(name: String, root: u64) -> Collection {
        Collection {
            name,
            root,
            counter: 0,
        }
    }

    pub fn empty() -> Collection {
        Collection {
            name: "".to_string(),
            root: u64::MAX,
            counter: 0,
        }
    }

    pub fn serialize(&mut self) -> Item {
        let mut bytes: [u8; COLLECTION_SIZE] = [0u8; COLLECTION_SIZE];
        
        let mut left_pos = 0;
        bytes[left_pos..left_pos+PAGE_ID_SIZE].clone_from_slice(&self.root.to_le_bytes());
        left_pos += PAGE_ID_SIZE;

        bytes[left_pos..left_pos+PAGE_ID_SIZE].clone_from_slice(&self.counter.to_le_bytes());
        
        let bytes_as_str = unsafe {
            std::str::from_utf8_unchecked(&bytes)
        };
        Item::new(self.name.clone(), bytes_as_str.to_owned())

    }

    pub fn deserialize(item: Item) -> Collection {
        let mut collection = Collection::empty();
        collection.name = item.key;

        if item.value.len() > 0 {
            let buf = item.value.as_bytes();

            let mut left_pos = 0;
            let mut u64_bytes = [0u8; PAGE_ID_SIZE];
            for n in 0..PAGE_ID_SIZE {
                u64_bytes[n] = buf[left_pos+n];
            }
            left_pos += PAGE_ID_SIZE;
            collection.root = u64::from_le_bytes(u64_bytes);

            u64_bytes = [0u8; PAGE_ID_SIZE];
            for n in 0..PAGE_ID_SIZE {
                u64_bytes[n] = buf[left_pos+n];
            }
            collection.counter = u64::from_le_bytes(u64_bytes);
        }

        collection
    }

    pub fn id(&mut self) -> u64 {
        let id = self.counter;
        self.counter += 1;
        return id;
    }

    pub fn find(&self, key: String, tx: &Tx) -> Result<Option<Item>, CustomError> {
        let root = tx.get_node(self.root);
        match root {
            Ok(root) => {
                match root.find_key(&key, true, tx) {
                    Ok((index, containing_node, _)) => {
                        if index == usize::MAX {
                            return Ok(None);
                        }
                        
                        Ok(Some(containing_node.items[index].clone()))
                    }
                    Err(error) => Err(error)
                }
            }
            Err(error) => Err(error)
        }

        
    }

    pub fn find_mut(&self, key: String, tx: &TxMut) -> Result<Option<Item>, CustomError> {
        let root = tx.get_node(self.root);
        match root {
            Ok(root) => {
                match root.find_key_mut(&key, true, tx) {
                    Ok((index, containing_node, _)) => {
                        if index == usize::MAX {
                            return Ok(None);
                        }
                        
                        Ok(Some(containing_node.items[index].clone()))
                    }
                    Err(error) => Err(error)
                }
            }
            Err(error) => Err(error)
        }

        
    }

    pub fn put(&mut self, key: String, value: String, tx: &mut TxMut) -> Result<(), CustomError> {

        let item = Item::new(key, value);
        let mut root: Node;
        if self.root == u64::MAX {
            match tx.new_node(vec![], vec![]) {
                Ok(node) => {
                    root = node;
                }
                Err(error) => {
                    return Err(error);
                }
            }
            match tx.write_node(&mut root) {
                Ok(()) => {
                    self.root = root.page_id;
                }
                Err(error) => {
                    return Err(error);
                }
            }
        } else {
            match tx.get_node(self.root) {
                Ok(_root) => {
                    root = _root;
                }
                Err(error) => {
                    return Err(error)
                }
            }
        }

        match root.find_key_mut(&item.key, false, tx) {
            Ok((insertion_index, node_to_insert_in, ancestors_index)) => {
                let mut node_to_insert_in = node_to_insert_in;
                
                if insertion_index < node_to_insert_in.items.len() && node_to_insert_in.items[insertion_index].key == *(&item.key) {
                    node_to_insert_in.items[insertion_index] = item;
                } else {
                    node_to_insert_in.add_item(item, insertion_index);
                }
                
                node_to_insert_in.write_self_node(tx);

                match self.get_nodes(&ancestors_index, tx) {
                    Ok(mut ancestors) => {
                        if ancestors.len() >= 2 {
                            for i in (0..=ancestors.len()-2).rev() {
                                let mut p_node = ancestors[i].clone();
                                let mut node = ancestors[i+1].clone();
                                let node_index = ancestors_index[i+1];
                                if node.is_over_populated(tx) {
                                    p_node.split(&mut node, node_index, tx);
                                }
                                ancestors[i] = p_node;
                                ancestors[i+1] = node;
                            }
                        }
                        
                        let mut root = ancestors[0].clone();
                        if root.is_over_populated(tx) {
                            let mut new_root;
                            match tx.new_node(vec![], vec![root.page_id]) {
                                Ok(node) => {
                                    new_root = node;
                                }
                                Err(error) => {
                                    return Err(error);
                                }
                            }
                            new_root.split(&mut root, 0, tx);

                            match tx.write_node(&mut new_root) {
                                Ok(()) => {
                                    self.root = new_root.page_id;
                                },
                                Err(error) => {
                                    return Err(error);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }

                Ok(())
            }
            Err(error) => {
                return Err(error);
            }
        }
        
    }

    pub fn remove(&mut self, key: String, tx: &mut TxMut) -> Result<(), CustomError> {
        match tx.get_node(self.root) {
            Ok(root) => {
                match root.find_key_mut(&key, true, tx) {
                    Ok((remove_item_index, mut node_to_remove_from, mut ancestor_indexes)) => {

                        if remove_item_index == usize::MAX {
                            return Ok(());
                        }

                        if node_to_remove_from.is_leaf() {
                            node_to_remove_from.remove_item_from_leaf(remove_item_index, tx);
                        } else {
                            match node_to_remove_from.remove_item_from_internal(remove_item_index, tx) {
                                Ok(affected_nodes) => {
                                    ancestor_indexes.extend(affected_nodes);

                                    match self.get_nodes(&ancestor_indexes, tx) {
                                        Ok(mut ancestors) => {
                                            for i in (0..=ancestors.len()-2).rev() {
                                                let mut p_node = ancestors[i].clone();
                                                let mut node = ancestors[i+1].clone();
                                                if node.is_under_populated(tx) {
                                                    match p_node.rebalance_remove(&mut node, ancestor_indexes[i+1], tx) {
                                                        Ok(()) => {
                                                            let root = ancestors[0].clone();
                                                            if root.items.len() == 0 && root.child_nodes.len() > 0 {
                                                                tx.delete_node(&root);
                                                                self.root = ancestors[1].page_id;
                                                            }
                                                        }
                                                        Err(error) => {
                                                            return Err(error);
                                                        }
                                                    }
                                                }
                                                ancestors[i] = p_node;
                                                ancestors[i+1] = node;
                                            }
                                        }
                                        Err(error) => {
                                            return Err(error);
                                        }
                                    }
                                }
                                Err(error) => {
                                    return Err(error);
                                }
                            }
                        }

                        Ok(())
                    }
                    Err(error) => Err(error)
                }
            }
            Err(error) => Err(error)
        }

    }

    fn get_nodes(&mut self, indexes: &Vec<usize>, tx: &mut TxMut) -> Result<Vec<Node>, CustomError> {
        let root: Node;
        match tx.get_node(self.root) {
            Ok(node) => {
                root = node;
            }
            Err(error) => {
                return Err(error);
            }
        }

        let mut child = root.clone();
        let mut nodes = vec![root];
        for i in 1..indexes.len() {
            match tx.get_node(child.child_nodes[indexes[i]]) {
                Ok(node) => {
                    child = node.clone();
                    nodes.push(node);
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }

        Ok(nodes)
    }

}

#[cfg(test)]
mod tests {
    use crate::{dal::{Options, DEFAULT_OPTIONS}, db::DB};
    use core::panic;
    use std::{fs, path::Path};

    use super::Collection;

    #[test]
    fn new_collection_read_write_nodes() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_collection_test_internal_1"
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

                let mut root_node;
                match tx.new_node(vec![], vec![]) {
                    Ok(node) => {
                        root_node = node;
                    }
                    Err(error) => {
                        panic!("Failed to create new node: {:?}", error);
                    }
                }
                
                match tx.write_node(&mut root_node) {
                    Ok(()) => {}
                    Err(_) => {
                        assert!(false,  "Root node creation failed!")
                    }
                }

                let mut collection = Collection::new(
                    "Collection1".to_string(), 
                    root_node.page_id,
                );

                let key1 = "key1".to_string();
                let value1 = "value1".to_string();

                match collection.put(key1.clone(), value1.clone(), &mut tx) {
                    Ok(()) => {
                        match collection.find_mut(key1.clone(), &mut tx) {
                            Ok(optional_item) => {
                                match optional_item {
                                    Some(item) => {
                                        assert_eq!(item.key, key1);
                                        assert_eq!(item.value, value1);
                                    }
                                    None => {
                                        assert!(false, "Collection.find sent empty item which is incorrect");
                                    }
                                }
                            }
                            Err(_) => {
                                assert!(false, "Collection.find failed")
                            }
                        }
                    },
                    Err(error) => {
                        assert!(false, "Collection.put failed with {:?}", error);
                    }
                }


                let key2 = "key2".to_string();
                let value2 = "value2".to_string();

                match collection.put(key2.clone(), value2.clone(), &mut tx) {
                    Ok(()) => {
                        match collection.find_mut(key2.clone(), &mut tx) {
                            Ok(optional_item) => {
                                match optional_item {
                                    Some(item) => {
                                        assert_eq!(item.key, key2);
                                        assert_eq!(item.value, value2);
                                    }
                                    None => {
                                        assert!(false, "Collection.find sent empty item which is incorrect");
                                    }
                                }
                            }
                            Err(_) => {
                                assert!(false, "Collection.find failed")
                            }
                        }
                    },
                    Err(_) => {
                        assert!(false, "Collection.put failed");
                    }
                }

                match tx.commit() {
                    Ok(()) => {
                        assert!(true, "Transaction commit successful");
                    }
                    Err(error) => {
                        assert!(false, "Transaction commit unsuccessful with {:?}", error);
                    }
                }

            }
            Err(_) => {
                assert!(false, "DB not created successfully!")
            }
        }


    }

    #[test]
    fn add_thousand_nodes() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_collection_test_internal_2"
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
            Ok(ref mut db) => {
                let mut tx = db.write_tx();

                let mut root_node;
                match tx.new_node(vec![], vec![]) {
                    Ok(node) => {
                        root_node = node;
                    }
                    Err(error) => {
                        panic!("Failed to create new root node due to: {:?}", error);
                    }
                }
                match tx.write_node(&mut root_node) {
                    Ok(_) => {},
                    Err(error) => {
                        panic!("Failed to write root node due to: {:?}", error);
                    }
                }

                let mut collection = Collection::new(
                    "Collection1".to_string(), 
                    root_node.page_id,
                );

                for i in 1..=1000 {
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);

                    match collection.put(key.clone(), value.clone(), &mut tx) {
                        Ok(()) => {

                        }
                        Err(error) => {
                            assert!(false, "Failed with {:?}", error);
                        }
                    }
                }

                match tx.commit() {
                    Ok(()) => {
                        assert!(true, "Transaction commit successful");
                    }
                    Err(error) => {
                        assert!(false, "Transaction commit unsuccessful with {:?}", error)
                    }
                }

                let tx = db.read_tx();

                for i in 1..=1000 {
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);

                    match collection.find(key.clone(), &tx) {
                        Ok(Some(item)) =>{
                            assert_eq!(value, item.value);
                        }
                        Ok(None) => {
                            assert!(false, "No item found");
                        }
                        Err(error) => {
                            assert!(false, "Error occured while retrieving: {:?}", error);
                        }
                    }
                }

                match tx.commit() {
                    Ok(()) => {
                        assert!(true, "Transaction commit successful");
                    }
                    Err(error) => {
                        assert!(false, "Transaction commit unsuccessful with {:?}", error)
                    }
                }

                let mut tx = db.write_tx();

                for i in 1..=1000 {
                    let key = format!("key{}", i);
                    
                    match collection.remove(key.clone(), &mut tx) {
                        Ok(()) => {
                            match collection.find_mut(key.clone(), &mut tx) {
                                Ok(Some(item)) => {
                                    assert!(false, "Item not removed: {:?}", item);
                                }
                                Ok(None) => {
                                    assert!(true, "Item removed");
                                }
                                Err(error) => {
                                    assert!(false, "Error occured while retrieving: {:?}", error);
                                }
                            }
                        }
                        Err(error) => {
                            assert!(false, "Error occured while removing: {:?}", error)
                        }
                    }
                }

                match tx.commit() {
                    Ok(()) => {
                        assert!(true, "Transaction commit successful");
                    }
                    Err(error) => {
                        assert!(false, "Transaction commit unsuccessful with {:?}", error)
                    }
                }
            }
            Err(_) => {
                assert!(false, "Dal not created successfully!")
            }
        }


    }

}
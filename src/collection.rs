use crate::error::CustomError;
use crate::node::{Item, Node};
use crate::dal::DAL;

pub struct Collection {
    name: String,
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

    pub fn id(&mut self) -> u64 {
        let id = self.counter;
        self.counter += 1;
        return id;
    }

    pub fn find(&mut self, key: String, dal: &mut DAL) -> Result<Option<Item>, CustomError> {

        let root = dal.get_node(self.root);
        match root {
            Ok(root) => {
                match root.find_key(&key, true, dal) {
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

    pub fn put(&mut self, key: String, value: String, dal: &mut DAL) -> Result<(), CustomError> {

        let item = Item::new(key, value);
        let mut root: Node;
        if self.root == u64::MAX {
            root = Node::empty();
            match dal.write_node(&mut root) {
                Ok(()) => {
                    self.root = root.page_id;
                }
                Err(error) => {
                    return Err(error);
                }
            }
            
        } else {
            match dal.get_node(self.root) {
                Ok(_root) => {
                    root = _root;
                }
                Err(error) => {
                    return Err(error)
                }
            }
        }

        match root.find_key(&item.key, false, dal) {
            Ok((insertion_index, node_to_insert_in, ancestors_index)) => {
                let mut node_to_insert_in = node_to_insert_in;

                
                if insertion_index < node_to_insert_in.items.len() && node_to_insert_in.items[insertion_index].key == *(&item.key) {
                    node_to_insert_in.items[insertion_index] = item;
                } else {
                    node_to_insert_in.add_item(item, insertion_index);
                }
                
                node_to_insert_in.write_self_node(dal);

                match self.get_nodes(&ancestors_index, dal) {
                    Ok(mut ancestors) => {
                        if ancestors.len() >= 2 {
                            for i in (0..=ancestors.len()-2).rev() {
                                let mut p_node = ancestors[i].clone();
                                let mut node = ancestors[i+1].clone();
                                let node_index = ancestors_index[i+1];
                                if node.is_over_populated(dal) {
                                    p_node.split(&mut node, node_index, dal);
                                }
                                ancestors[i] = p_node;
                                ancestors[i+1] = node;
                            }
                        }
                        
                        let mut root = ancestors[0].clone();
                        if root.is_over_populated(dal) {
                            let mut new_root = Node::new(vec![], vec![root.page_id]);
                            new_root.split(&mut root, 0, dal);

                            match dal.write_node(&mut new_root) {
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

    pub fn remove(&mut self, key: String, dal: &mut DAL) -> Result<(), CustomError> {
        match dal.get_node(self.root) {
            Ok(root) => {
                match root.find_key(&key, true, dal) {
                    Ok((remove_item_index, mut node_to_remove_from, mut ancestor_indexes)) => {

                        if remove_item_index == usize::MAX {
                            return Ok(());
                        }

                        if node_to_remove_from.is_leaf() {
                            node_to_remove_from.remove_item_from_leaf(remove_item_index, dal);
                        } else {
                            match node_to_remove_from.remove_item_from_internal(remove_item_index, dal) {
                                Ok(affected_nodes) => {
                                    ancestor_indexes.extend(affected_nodes);

                                    match self.get_nodes(&ancestor_indexes, dal) {
                                        Ok(mut ancestors) => {
                                            for i in (0..=ancestors.len()-2).rev() {
                                                let mut p_node = ancestors[i].clone();
                                                let mut node = ancestors[i+1].clone();
                                                if node.is_under_populated(dal) {
                                                    match p_node.rebalance_remove(&mut node, ancestor_indexes[i+1], dal) {
                                                        Ok(()) => {
                                                            let root = ancestors[0].clone();
                                                            if root.items.len() == 0 && root.child_nodes.len() > 0 {
                                                                dal.delete_node(&root);
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

    fn get_nodes(&mut self, indexes: &Vec<usize>, dal: &mut DAL) -> Result<Vec<Node>, CustomError> {
        let root: Node;
        match dal.get_node(self.root) {
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
            match dal.get_node(child.child_nodes[indexes[i]]) {
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
    use crate::{dal::{Options, DAL, DEFAULT_OPTIONS}, meta, node::Node};
    use core::panic;
    use std::{fs, path::Path};

    use super::Collection;

    #[test]
    fn new_collection_read_write_nodes() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_collection_test_internal"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }

        match DAL::new_dal(options) {
            Ok(ref mut dal) => {
                let mut root_node = Node::empty();
                match dal.write_node(&mut root_node) {
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

                match collection.put(key1.clone(), value1.clone(), dal) {
                    Ok(()) => {
                        match collection.find(key1.clone(), dal) {
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

                match collection.put(key2.clone(), value2.clone(), dal) {
                    Ok(()) => {
                        match collection.find(key2.clone(), dal) {
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

            }
            Err(_) => {
                assert!(false, "Dal not created successfully!")
            }
        }


    }

    #[test]
    fn add_thousand_nodes() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_collection_test_internal"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }

        match DAL::new_dal(options) {
            Ok(ref mut dal) => {
                let mut root_node = Node::empty();
                match dal.write_node(&mut root_node) {
                    Ok(_) => {},
                    Err(error) => {
                        panic!("Failed to create root node due to: {:?}", error);
                    }
                }

                let mut collection = Collection::new(
                    "Collection1".to_string(), 
                    root_node.page_id,
                );

                for i in 1..=1000 {
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);

                    match collection.put(key.clone(), value.clone(), dal) {
                        Ok(()) => {

                        }
                        Err(error) => {
                            assert!(false, "Failed with {:?}", error);
                        }
                    }
                }

                for i in 1..=1000 {
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);

                    match collection.find(key.clone(), dal) {
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

                for i in 1..=1000 {
                    let key = format!("key{}", i);
                    
                    match collection.remove(key.clone(), dal) {
                        Ok(()) => {
                            match collection.find(key.clone(), dal) {
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
            }
            Err(_) => {
                assert!(false, "Dal not created successfully!")
            }
        }


    }

}
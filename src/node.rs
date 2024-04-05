use std::mem;
use crate::consts::{PAGE_ID_SIZE, PAGE_SIZE};
use crate::dal::DAL;
use crate::error::CustomError;

#[derive(Clone, Debug)]
pub struct Item {
    pub key: String,
    pub value: String,
}

impl Item {

    pub fn new(key: String, value: String) -> Item {
        Item {
            key,
            value
        }
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub page_id: u64,
    pub items: Vec<Item>,
    pub child_nodes: Vec<u64>,
}

impl Node
{

    fn is_last(index: usize, parent_node: &Node) -> bool {
        index == parent_node.items.len()
    }

    fn is_first(index: usize) -> bool {
        index == 0
    }

    pub fn is_leaf(&self) -> bool {
        self.child_nodes.len() == 0
    }

    pub fn new(items: Vec<Item>, child_nodes: Vec<u64>) -> Node {
        Node {
            page_id: u64::MAX,
            items,
            child_nodes,
        }
    }

    pub fn empty() -> Node {
        Node {
            page_id: u64::MAX,
            items: vec![],
            child_nodes: vec![],
        }
    }

    pub fn write_self_node(&mut self, dal: &mut DAL) {
        match dal.write_node(self) {
            Ok(()) => {},
            Err(error) => {
                panic!("{:?}", error);
            }
        }
    }

    pub fn write_node(&self, node: &mut Node, dal: &mut DAL) {
        match dal.write_node(node) {
            Ok(()) => {},
            Err(error) => {
                panic!("{:?}", error);
            }
        }
    }

    pub fn write_nodes(&self, nodes: Vec<&mut Node>, dal: &mut DAL) {
        for node in nodes {
            self.write_node(node, dal);
        }
    }

    fn get_node(&self, page_num: u64, dal: &mut DAL) -> Result<Node, CustomError> {
        dal.get_node(page_num)
    }

    pub fn is_over_populated(&self, dal: &DAL) -> bool {
        dal.is_over_populated(self)
    }

    pub fn can_spare_an_element(&self, dal: &DAL) -> bool {
        let split_index = dal.get_split_index(self);
        if split_index == usize::MAX {
            return false;
        }
        return true;
    }

    pub fn is_under_populated(&self, dal: &DAL) -> bool {
        dal.is_under_populated(self)
    }

    pub fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];

        let mut left_pos = 0;
        let mut right_pos = buf.len() - 1;
        
        let mut bit_set_var: u8 = 0;
        if self.is_leaf() {
            bit_set_var = 1;
        }
        buf[left_pos..left_pos+1].clone_from_slice(&bit_set_var.to_le_bytes());
        left_pos += 1;

        let len_of_items = self.items.len() as u16;
        buf[left_pos..left_pos+2].clone_from_slice(&len_of_items.to_le_bytes());
        left_pos += 2;

        for i in 0..self.items.len() {
            let item = self.items[i].clone();

            if !self.is_leaf() {
                let child_node = self.child_nodes[i].clone();

                buf[left_pos..left_pos+PAGE_ID_SIZE].clone_from_slice(&child_node.to_le_bytes());
                left_pos += PAGE_ID_SIZE;
            }

            let key_len = item.key.as_bytes().len();
            let val_len = item.value.as_bytes().len();

            let offset = right_pos - key_len - val_len - 2;
            buf[left_pos..left_pos+2].clone_from_slice(&(offset as u16).to_le_bytes());
            left_pos += 2;

            right_pos -= val_len;
            buf[right_pos..right_pos+val_len].clone_from_slice(&item.value.as_bytes());

            right_pos -= 1;
            buf[right_pos..right_pos+1].clone_from_slice(&(val_len as u8).to_le_bytes());

            right_pos -= key_len;
            buf[right_pos..right_pos+key_len].clone_from_slice(&item.key.as_bytes());

            right_pos -= 1;
            buf[right_pos..right_pos+1].clone_from_slice(&(key_len as u8).to_le_bytes());

            if left_pos >= right_pos {
                panic!("LeftPos > RightPos - {:?}", self);
            }
        }

        if !self.is_leaf() {
            let last_child_node = self.child_nodes[self.child_nodes.len() - 1].clone();

            buf[left_pos..left_pos+PAGE_ID_SIZE].clone_from_slice(&last_child_node.to_le_bytes());
        }
        
        buf
    }

    pub fn deserialize(buf: [u8; PAGE_SIZE]) -> Result<Node, CustomError> {
        let mut node = Node::empty();

        let mut left_pos = 0;

        let mut u8_bytes: [u8;1] = [0u8;1];
        u8_bytes[0] = buf[left_pos];
        let is_leaf = u8::from_le_bytes(u8_bytes) as u16;
        left_pos += 1;

        let mut u16_bytes: [u8;2] = [0u8;2];
        for n in 0..2 {
            u16_bytes[n] = buf[left_pos+n];
        }
        left_pos += 2;
        let item_len = u16::from_le_bytes(u16_bytes) as usize;

        for _ in 0..item_len {
            
            if is_leaf == 0 {
                let mut u64_bytes: [u8; PAGE_ID_SIZE] = [0u8; PAGE_ID_SIZE];
                for n in 0..PAGE_ID_SIZE {
                    u64_bytes[n] = buf[left_pos+n];
                }
                left_pos += PAGE_ID_SIZE;
                node.child_nodes.push(u64::from_le_bytes(u64_bytes));
            }

            u16_bytes = [0u8; 2];
            for n in 0..2 {
                u16_bytes[n] = buf[left_pos+n];
            }
            left_pos += 2;
            let mut offset = u16::from_le_bytes(u16_bytes) as usize;

            u8_bytes = [0u8; 1];
            u8_bytes[0] = buf[offset];
            offset += 1;
            let key_len = u8::from_le_bytes(u8_bytes) as usize;

            let key: String;
            match String::from_utf8(buf[offset..offset+key_len].to_vec()) {
                Ok(string) => {
                    key = string;
                    offset += key_len;
                }
                Err(error) => {
                    return Err(CustomError::new(error.to_string()));
                }
            }

            u8_bytes = [0u8; 1];
            u8_bytes[0] = buf[offset];
            offset += 1;
            let val_len = u8::from_le_bytes(u8_bytes) as usize;

            let value: String;
            match String::from_utf8(buf[offset..offset+val_len].to_vec()) {
                Ok(string) => {
                    value = string;
                    offset += val_len;
                }
                Err(error) => {
                    return Err(CustomError::new(error.to_string()));
                }
            }
            node.items.push(Item::new(key, value));
        }

        if is_leaf == 0 {
            let mut u64_bytes = [0u8; PAGE_ID_SIZE];
            for n in 0..PAGE_ID_SIZE {
                u64_bytes[n] = buf[left_pos+n];
            }
            left_pos += PAGE_ID_SIZE;
            node.child_nodes.push(u64::from_le_bytes(u64_bytes));
        }

        Ok(node)
    }

    pub fn add_item(&mut self, item: Item, insertion_index: usize) -> usize {
        if self.items.len() == insertion_index {
            self.items.push(item);
            return insertion_index;
        }

        self.items.insert(insertion_index, item);
        insertion_index
    }

    pub fn element_size(&self, i: usize) -> usize {
        let mut size = 0;
        size += &self.items[i].key.len();
        size += &self.items[i].value.len();
        size += PAGE_ID_SIZE;
        size
    }

    pub fn node_size(&self) -> usize {
        let mut size = 0;
        size += 3;

        for i in 0..self.items.len() {
            size += self.element_size(i)
        }

        size += PAGE_ID_SIZE;
        size
    }

    pub fn find_key(&self, key: &String, exact: bool, dal: &mut DAL) -> Result<(usize, Node, Vec<usize>), CustomError> {
        let mut ancestors_indexes = vec![0];

        match Self::find_key_helper(self.clone(), key, exact, &mut ancestors_indexes, dal) {
            Ok((index, containing_node)) => Ok((index, containing_node, ancestors_indexes)),
            Err(error) => Err(error)
        }
    }

    fn find_key_helper(node: Node, key: &String, exact: bool, ancestor_indexes: &mut Vec<usize>, dal: &mut DAL) -> Result<(usize, Node), CustomError> {
        let (was_found, index) = node.find_key_in_node(key);
        if was_found {
            return Ok((index, node))
        }
        
        if node.is_leaf() {
            if exact {
                return Ok((usize::MAX, node))
            }
            return Ok((index, node))
        }

        (*ancestor_indexes).push(index);

        match node.get_node(node.child_nodes[index], dal) {
            Ok(next_child) => Self::find_key_helper(next_child, key, exact, ancestor_indexes, dal),
            Err(error) => Err(error)
        }
    }

    fn find_key_in_node(&self, key: &String) -> (bool, usize) {
        for (i, item) in self.items.iter().enumerate() {
            if *key == item.key {
                return (true, i);
            }

            if *key < item.key {
                return (false, i);
            }
        }

        (false, self.items.len())
    }

    pub fn split(&mut self, node_to_split: &mut Node, node_to_split_index: usize, dal: &mut DAL) {
        let split_index = dal.get_split_index(node_to_split); // Add split index

        let middle_item = node_to_split.items.remove(split_index);
        
        let mut new_node = Node::new(vec![], vec![]);

        if node_to_split.is_leaf() {
            new_node.items.extend(node_to_split.items.split_off(split_index));
        } else {
            new_node.items.extend(node_to_split.items.split_off(split_index));
            new_node.child_nodes.extend(node_to_split.child_nodes.split_off(split_index + 1));
        }
        
        match dal.write_node(&mut new_node) {
            Ok(()) => {},
            Err(error) => {
                panic!("{:?}", error);
            }
        }

        self.add_item(middle_item, node_to_split_index);
        if self.child_nodes.len() == node_to_split_index + 1 {
            self.child_nodes.push(new_node.page_id);
        } else {
            self.child_nodes.insert(node_to_split_index + 1, new_node.page_id);
        }

        self.write_self_node(dal);
        self.write_node(node_to_split, dal);
    }

    pub fn remove_item_from_leaf(&mut self, index: usize, dal: &mut DAL) {
        self.items.remove(index);
        self.write_self_node(dal);
    }

    pub fn remove_item_from_internal(&mut self, index: usize, dal: &mut DAL) -> Result<Vec<usize>, CustomError> {
        let mut affected_nodes = vec![];
        affected_nodes.push(index);

        let mut a_node_res = self.get_node(self.child_nodes[index], dal);
        
        while let Ok(ref mut a_node) = a_node_res {
            if !a_node.is_leaf() {
                let traversing_index = self.child_nodes.len() - 1;
                
                match a_node.get_node(a_node.child_nodes[traversing_index], dal) {
                    Ok(node) => {
                        a_node_res = Ok(node);
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }

                affected_nodes.push(traversing_index);
            } else {
                break;
            }
        }

        match a_node_res {
            Ok(ref mut a_node) => {
                self.items[index] = a_node.items.pop().unwrap();
                self.write_self_node(dal);
                self.write_node(a_node, dal);

                Ok(affected_nodes)
            }
            Err(error) => Err(error)
        }
    }

    fn rotate_right(a_node: &mut Node, p_node: &mut Node, b_node: &mut Node, b_node_index: usize) {

        let a_node_item = a_node.items.pop().unwrap();

        let mut p_node_item_index = b_node_index - 1;
        if Self::is_first(b_node_index) {
            p_node_item_index = 0;
        }
        let p_node_item = p_node.items.remove(p_node_item_index);
        p_node.items.insert(p_node_item_index, a_node_item);

        b_node.items.insert(0, p_node_item);

        if !a_node.is_leaf() {
            let child_node_to_shift = a_node.child_nodes.pop().unwrap();
            b_node.child_nodes.insert(0, child_node_to_shift);
        }

    }

    fn rotate_left(a_node: &mut Node, p_node: &mut Node, b_node: &mut Node, b_node_index: usize) {
        let b_node_item = b_node.items.remove(0);

        let mut p_node_item_index = b_node_index;
        if Self::is_last(b_node_index, p_node) {
            p_node_item_index = p_node.items.len() - 1;
        }
        let p_node_item = p_node.items.remove(p_node_item_index);
        p_node.items.insert(p_node_item_index, b_node_item);

        a_node.items.push(p_node_item);

        if !b_node.is_leaf() {
            let child_node_to_shift = b_node.child_nodes.remove(0);
            a_node.child_nodes.push(child_node_to_shift);
        }
    }

    fn merge(&mut self, b_node: &mut Node, b_node_index: usize, dal: &mut DAL) -> Result<(), CustomError> {
        let mut a_node = self.get_node(self.child_nodes[b_node_index-1], dal);
        match a_node {
            Ok(ref mut a_node) => {
                let p_node_item = self.items.remove(b_node_index-1);
                a_node.items.push(p_node_item);

                a_node.items.extend(b_node.items.drain(0..));
                self.child_nodes.remove(b_node_index);
                if !a_node.is_leaf() {
                    a_node.child_nodes.extend(b_node.child_nodes.drain(0..));
                }

                self.write_self_node(dal);
                self.write_node(a_node, dal);
                dal.delete_node(b_node);

                Ok(())
            }
            Err(error) => Err(error)
        }
    }

    pub fn rebalance_remove(&mut self, unbalanced_node: &mut Node, unbalanced_node_index: usize, dal: &mut DAL) -> Result<(), CustomError> {
        if unbalanced_node_index != 0 {
            let left_node = self.get_node(self.child_nodes[unbalanced_node_index-1], dal);
            match left_node {
                Ok(mut left_node) => {
                    if left_node.can_spare_an_element(dal) {
                        Self::rotate_right(&mut left_node, self, unbalanced_node, unbalanced_node_index);
                        
                        self.write_self_node(dal);
                        self.write_nodes(vec![&mut left_node, unbalanced_node], dal);
                        
                        return Ok(());
                    }
                }
                Err(error) => {
                    return Err(error);
                }
            }
            
        }

        if unbalanced_node_index != self.child_nodes.len() - 1 {
            let right_node = self.get_node(self.child_nodes[unbalanced_node_index+1], dal);
            match right_node {
                Ok(mut right_node) => {
                    if right_node.can_spare_an_element(dal) {
                        Self::rotate_left(unbalanced_node, self, &mut right_node, unbalanced_node_index);
                        
                        self.write_self_node(dal);
                        self.write_nodes(vec![unbalanced_node, &mut right_node], dal);
                        
                        return Ok(());
                    }
                }
                Err(error) => {
                    return Err(error);
                }
            }
            
        }

        if unbalanced_node_index == 0 {
            let mut right_node = self.get_node(self.child_nodes[unbalanced_node_index+1], dal);
            match right_node {
                Ok(ref mut right_node) => {
                    return self.merge(right_node, unbalanced_node_index+1, dal);
                }
                Err(error) => {
                    return Err(error)
                }
            }
        }

        
        self.merge(unbalanced_node, unbalanced_node_index, dal)
    }
}



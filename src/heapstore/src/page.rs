#![allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused)]

use log::Level;

use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use std::convert::TryInto;
use std::fmt;
use std::mem;
use std::env;

// Type to hold any value smaller than the size of a page.
// We choose u16 because it is sufficient to represent any slot that fits in a 4096-byte-sized page. 
// Note that you will need to cast Offset to usize if you want to use it to index an array.
pub type Offset = u16;
// For debug
const BYTES_PER_LINE: usize = 40;

/// Page struct. This must occupy not more than PAGE_SIZE when serialized.
/// In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// If you delete a value, you do not need reclaim header space the way you must reclaim page
/// body space. E.g., if you insert 3 values then delete 2 of them, your header can remain 26 
/// bytes & subsequent inserts can simply add 6 more bytes to the header as normal.
/// The rest must filled as much as possible to hold values.
pub(crate) struct Page {
    // Header – 8 bytes total
    page_id: PageId, // 2 bytes
    num_slots: u16, // 2 bytes, both empty and used slots
    end_free: u16, // 2 bytes, start of records
    empty_array: Vec<SlotId>, // 6 bytes each, empty slots, ordered by slot id
    slot_array : Vec<SlotInfoType>, // 6 bytes each, nonempty slot, ordered by address,

    /// The data for data
    data: [u8; PAGE_SIZE],
}

struct SlotInfo {
    slot_id : SlotId, // 2 bytes
    length : u16, // 2 bytes
    address : u16, // 2 bytes
}
type SlotInfoType = SlotInfo;


/// The functions required for page
impl Page {
    /// Create a new page
    #[allow(unused)]
    pub fn new(page_id: PageId) -> Self {
        let mut p = Page {
            page_id: page_id,
            num_slots: 0, 
            end_free: PAGE_SIZE as u16,
            empty_array: Vec::new(),
            slot_array: Vec::new(),
            data: [0; PAGE_SIZE]
        };
        return p;
    }

    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId {
        return self.page_id;
    }

    // Reorganizes pages so there are no gaps between records
    pub fn compact_page(&mut self) {
        let non_record_space:usize = self.get_free_space() + self.get_header_size();
        let mut data_temp: [u8; PAGE_SIZE] = [0; PAGE_SIZE]; 
        let mut pointer:usize = 0; 

        // Writes each record to data_temp so that there are no gaps
        for i in 0..self.slot_array.len() {
            let start:usize = self.slot_array[i].address as usize;
            let end:usize = start + self.slot_array[i].length as usize;
            data_temp[pointer..pointer+self.slot_array[i].length as usize].clone_from_slice(&self.data[start..end]);
            pointer = pointer+self.slot_array[i].length as usize;
        }

        // Writes each data_temp back to data
        self.data[non_record_space..PAGE_SIZE].clone_from_slice(&data_temp[0..(PAGE_SIZE - non_record_space)]);

        // Readdresses all the records
        let mut last: u16 = PAGE_SIZE as u16;
        for i in (0..self.slot_array.len()).rev() {
            last = last - self.slot_array[i].length;
            self.slot_array[i].address = last ;
        }

        // Updates end of free space
        self.end_free = self.slot_array[0].address
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should replace the slotId on the next insert.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    #[allow(dead_code)]
    #[allow(unused)]
    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        // If there is not enough free space in the page, return none
        if self.get_free_space() < (bytes.len() + 6) {
            return None;
        }

        // If there is not enough space to add a new slot into the header, compact the page
        if self.get_header_size() + 6 >= self.end_free.into() {
            self.compact_page();
        }

        // Gets the first available slot id
        let mut new_slot_id: SlotId = self.slot_array.len() as SlotId;
        if self.empty_array.len() > 0 { 
            new_slot_id = self.empty_array.pop().unwrap();
        }
        else {
            self.num_slots += 1;
        }

        // Trys to insert new between existing records in the optimal place
        if self.slot_array.len() > 0
        {
            let mut best_address: Option<u16> = None; // Used to find a valid place to insert data that minimizes leftover
            let mut best_leftover: Option<usize> = None; // Diff between size of record to be inserted and space available at best address
            let mut curr_leftover: usize = 0; // Diff between size of record to be inserted and space available at current address
            let mut opening_size: usize = 0; // Used to store the size of the opening between records
            let mut index: usize = self.slot_array.len() + 1; // Where to insert SlotInfo in slot_array
            
            // Checks if record can fit between any slot and finds the best slot
            for i in (0..self.slot_array.len()).rev()
            {
                // Get the opening size
                let end_of_i = self.slot_array[i].address as usize + self.slot_array[i].length as usize;
                if i == self.slot_array.len() - 1 {
                    opening_size = PAGE_SIZE as usize - end_of_i;
                }
                else {
                    opening_size = self.slot_array[i + 1].address as usize - end_of_i;
                }

                // If the opening can fit the record, check if it minimizes leftover
                if opening_size >= bytes.len() {
                    curr_leftover = opening_size - bytes.len();
                    match best_leftover {
                        Some(bl) => { 
                            if bl > curr_leftover{
                                best_leftover = Some(curr_leftover.clone());
                                best_address = Some(end_of_i.clone().try_into().unwrap());
                                index = i.clone() + 1;
                            }
                        }
                        None => {
                            best_address = Some(end_of_i.clone().try_into().unwrap());
                            best_leftover = Some((opening_size - bytes.len()).try_into().unwrap());
                            index = i.clone() + 1;
                        }
                    }
                }
            }
            
            // If a valid opening was found, insert new record there and update header
            match best_address {
                Some(new_address) => { 
                    let new_slot_info: SlotInfoType = SlotInfo {slot_id: new_slot_id, address: new_address, length: bytes.len() as u16};
                    self.slot_array.insert(index, new_slot_info);
                    self.data[new_address as usize..new_address as usize + bytes.len()].clone_from_slice(&bytes);
                    return Some(new_slot_id);
                }
                None => {} 
            }
        }

        // Otherwise adds the record to the beginning of all records
        // If there is enough space without the page being compacted
        if (self.end_free - (self.get_header_size() as u16 + 6) < bytes.len() as u16) {
            self.compact_page();
        }

        let new_address = self.end_free - bytes.len() as u16;
        let new_slot_info: SlotInfoType = SlotInfo {slot_id: new_slot_id, address: new_address, length: bytes.len() as u16};
        self.slot_array.insert(0, new_slot_info);
        self.end_free = new_address;
        self.data[new_address as usize..new_address as usize + bytes.len()].clone_from_slice(&bytes);
        return Some(new_slot_id);
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    #[allow(dead_code)]
    #[allow(unused)]
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        for i in 0..self.slot_array.len() {
            if self.slot_array[i].slot_id == slot_id {
                let index = self.slot_array[i].address as usize;
                return Some(self.data[index..index+self.slot_array[i].length as usize].to_vec());
            }
        }
        return None;
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    #[allow(dead_code)]
    #[allow(unused)]
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        // Check all full slots
        for i in 0..self.slot_array.len() {
            if self.slot_array[i].slot_id == slot_id {
                if i == 0 {
                    self.end_free += self.slot_array[i].length;
                }

                // Insert the empty slot into the right spot
                let mut index = 0;
                while index < self.empty_array.len() && self.empty_array[index] > slot_id {
                    index += 1;
                }
                self.empty_array.insert(index, slot_id);

                self.slot_array.remove(i);
                return Some(());
            }
        }
        return None;
    }

    /// Deserialize bytes into Page
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    #[allow(dead_code)]
    #[allow(unused)]
    pub fn from_bytes(data: &[u8]) -> Self {
        //Builds header
        let page_id_temp: PageId = PageId::from_le_bytes(data[0..2].try_into().unwrap());
        let num_slots_temp: u16 = u16::from_le_bytes(data[2..4].try_into().unwrap());
        let end_free_temp: u16 = u16::from_le_bytes(data[4..6].try_into().unwrap());

        let mut empty_array_temp: Vec<SlotId> = Vec::new();;
        let mut slot_array_temp : Vec<SlotInfoType> = Vec::new();
        
        // Builds each slot_info object and then inserts into empty_array or slot_array
        for i in 0..num_slots_temp {
        let start: usize = (8 + i * 6) as usize;
        let slot_id: SlotId = SlotId::from_le_bytes(data[(start..start+2)].try_into().unwrap());
        let address: u16 = u16::from_le_bytes(data[(start+2)..(start+4)].try_into().unwrap());
        let length: u16 = u16::from_le_bytes(data[(start+4)..(start + 6)].try_into().unwrap());
        
            let slot_info = SlotInfo {
                slot_id: slot_id,
                length: length,
                address: address,
            };

            if slot_info.length != 0 {
                let mut index = 0; 
                while index < slot_array_temp.len() && slot_array_temp[index].address < slot_info.address {
                    index += 1;
                }
                slot_array_temp.insert(index, slot_info);
            }
            else {
                empty_array_temp.insert(empty_array_temp.len().try_into().unwrap(), slot_info.slot_id.into());
            }
        }

        let mut p = Page {
            page_id: page_id_temp,
            num_slots: num_slots_temp, 
            end_free: end_free_temp,
            empty_array: empty_array_temp,
            slot_array: slot_array_temp,
            data: [0u8; PAGE_SIZE]
        };
        p.data.clone_from_slice(&data[0..PAGE_SIZE]);
        return p;
    }

    /// Serialize page into a byte array. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    /// HINT: Do not use the self debug ({:?}) in this function, which calls this function.
    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::new();

        vec.extend(self.page_id.to_le_bytes().iter().cloned());
        vec.extend(self.num_slots.to_le_bytes().iter().cloned());
        vec.extend(self.end_free.to_le_bytes().iter().cloned());
        let empty_space: u16 = 0; // I only use 6 bytes in the header

        vec.extend(empty_space.to_le_bytes().iter().cloned());

        for i in 0..self.slot_array.len() {
            vec.extend(self.slot_array[i].slot_id.to_le_bytes().iter().cloned());
            vec.extend(self.slot_array[i].address.to_le_bytes().iter().cloned());
            vec.extend(self.slot_array[i].length.to_le_bytes().iter().cloned());
        }

        for i in 0..self.empty_array.len() {
            vec.extend(self.empty_array[i].to_le_bytes().iter().cloned());
            vec.extend(empty_space.to_le_bytes().iter().cloned());
            vec.extend(empty_space.to_le_bytes().iter().cloned());
        }

        vec.extend(self.data[self.get_header_size()..PAGE_SIZE].iter().cloned());
        return vec;
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        return ((self.slot_array.len() + self.empty_array.len()) * 6 + 8).into()
    }

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests. Optional for you to use in your code, but strongly suggested
    #[allow(dead_code)]
    pub(crate) fn get_free_space(&self) -> usize {
        // Calculates the amounts of space between records
        let mut reclaimable:usize = 0;   
        if self.slot_array.len() > 1 {
            for i in 0..self.slot_array.len()
            {   
                let end_of_i = self.slot_array[i].address as usize + self.slot_array[i].length as usize;
                if i == self.slot_array.len() - 1
                {
                    reclaimable += PAGE_SIZE as usize - end_of_i
                }
                else {
                    reclaimable += self.slot_array[i + 1].address as usize - end_of_i
                }
            }
        }

        // Then adds the free space between header and data 
        return reclaimable + (self.end_free as usize - self.get_header_size())
    }

    /// Utility function for comparing the bytes of another page.
    /// Returns a vec  of Offset and byte diff
    #[allow(dead_code)]
    pub fn compare_page(&self, other_page: Vec<u8>) -> Vec<(Offset, Vec<u8>)> {
        let mut res = Vec::new();
        let bytes = self.to_bytes();
        assert_eq!(bytes.len(), other_page.len());
        let mut in_diff = false;
        let mut diff_start = 0;
        let mut diff_vec: Vec<u8> = Vec::new();
        for (i, (b1,b2)) in bytes.iter().zip(&other_page).enumerate(){
            if b1 != b2 {
                if !in_diff {
                    diff_start = i;
                    in_diff = true;
                }
                diff_vec.push(*b1);
            } else {
                if in_diff {
                    //end the diff
                    res.push((diff_start as Offset, diff_vec.clone()));
                    diff_vec.clear();
                    in_diff = false;
                }
            }
        }
        res
    }
}

pub struct PageIntoIter {
    page: Page,
    index: SlotId,
}

/// The implementation of the (consuming) page iterator.
/// This should return the values in slotId order (ascending)
impl Iterator for PageIntoIter {
    // Each item returned by the iterator is the bytes for the value and the slot id.
    type Item = (Vec<u8>, SlotId);
   
    fn next(&mut self) -> Option<Self::Item> {
        if usize::from(self.index) > self.page.num_slots.into() {
            return None;
        }
        else{
            match self.page.get_value(self.index) {
                Some(v) => {
                    let item:Self::Item = (v, self.index.clone());
                    self.index += 1;
                    return Some(item);
                }
                None => {
                    // If slot is empty go to next slot
                    self.index += 1;
                    return self.next();
                }
            }
        }
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = (Vec<u8>, SlotId);
    type IntoIter = PageIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        PageIntoIter {
            page: self,
            index: 0,
        }
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //let bytes: &[u8] = unsafe { any_as_u8_slice(&self) };
        let p = self.to_bytes();
        let mut buffer = "".to_string();
        let len_bytes = p.len();

        // If you want to add a special header debugger to appear before the bytes add it here
        // buffer += "Header: \n";

        let mut pos = 0;
        let mut remaining;
        let mut empty_lines_count = 0;
        //: u8 = bytes_per_line.try_into().unwrap();
        let comp = [0; BYTES_PER_LINE];
        //hide the empty lines
        while pos < len_bytes {
            remaining = len_bytes - pos;
            if remaining > BYTES_PER_LINE {
                let pv = &(p)[pos..pos + BYTES_PER_LINE];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    buffer += &format!("{} ", empty_lines_count);
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                buffer += &format!("[0x{:08x}] ", pos);
                for i in 0..BYTES_PER_LINE {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => buffer += &format!("{:02x} ", pv[i]),
                    };
                }
            } else {
                let pv = &(p.clone())[pos..pos + remaining];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    //buffer += "empty\n";
                    //println!("working");
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    buffer += &format!("{} ", empty_lines_count);
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                buffer += &format!("[0x{:08x}] ", pos);
                for i in 0..remaining {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => buffer += &format!("{:02x} ", pv[i]),
                    };
                }
            }
            buffer += "\n";
            pos += BYTES_PER_LINE;
        }
        if empty_lines_count != 0 {
            buffer += &format!("{} ", empty_lines_count);
            buffer += "empty lines were hidden\n";
        }
        write!(f, "{}", buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;
    use rand::Rng;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(PAGE_SIZE - p.get_header_size(), p.get_free_space());
    }

    #[test]
    fn debug_page_insert() {
        init();
        let mut p = Page::new(0);
        let n = 20;
        let size = 20;
        let vals = get_ascending_vec_of_byte_vec_02x(n, size, size);
        for x in &vals {
            p.add_value(x);
        }
        println!("{:?}", p);
        assert_eq!(
            p.get_free_space(),
            PAGE_SIZE - p.get_header_size() - n * size
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_free_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_free_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }
    

    // #[test]
    // fn hs_page_get_first_free_space() {
    //     init();
    //     let mut p = Page::new(0);
    //     let _b1 = get_random_byte_vec(100);
    //     let _b2 = get_random_byte_vec(50);
    // }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.to_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.to_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.to_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.to_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes3.clone(), 2)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x.0);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.to_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(Some((tuple_bytes.clone(), 4)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_same_size() {
        init();
        let size = 800;
        let values = get_ascending_vec_of_byte_vec_02x(6, size, size);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_larger_size() {
        init();
        let size = 500;
        let values = get_ascending_vec_of_byte_vec_02x(8, size, size);
        let larger_val = get_random_byte_vec(size * 2 - 20);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(Some(5), p.add_value(&values[5]));
        assert_eq!(Some(6), p.add_value(&values[6]));
        assert_eq!(Some(7), p.add_value(&values[7]));
        assert_eq!(values[5], p.get_value(5).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(()), p.delete_value(6));
        assert_eq!(None, p.get_value(6));
        assert_eq!(Some(1), p.add_value(&larger_val));
        assert_eq!(larger_val, p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_smaller_size() {
        init();
        let size = 800;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size / 4),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_multi_ser() {
        init();
        let size = 500;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        let bytes = p.to_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(values[0], p2.get_value(0).unwrap());
        assert_eq!(values[1], p2.get_value(1).unwrap());
        assert_eq!(values[2], p2.get_value(2).unwrap());
        assert_eq!(Some(3), p2.add_value(&values[3]));
        assert_eq!(Some(4), p2.add_value(&values[4]));

        let bytes2 = p2.to_bytes();
        let mut p3 = Page::from_bytes(&bytes2);
        assert_eq!(values[0], p3.get_value(0).unwrap());
        assert_eq!(values[1], p3.get_value(1).unwrap());
        assert_eq!(values[2], p3.get_value(2).unwrap());
        assert_eq!(values[3], p3.get_value(3).unwrap());
        assert_eq!(values[4], p3.get_value(4).unwrap());
        assert_eq!(Some(5), p3.add_value(&values[5]));
        assert_eq!(Some(6), p3.add_value(&values[6]));
        assert_eq!(Some(7), p3.add_value(&values[7]));
        assert_eq!(None, p3.add_value(&values[0]));

        let bytes3 = p3.to_bytes();
        let p4 = Page::from_bytes(&bytes3);
        assert_eq!(values[0], p4.get_value(0).unwrap());
        assert_eq!(values[1], p4.get_value(1).unwrap());
        assert_eq!(values[2], p4.get_value(2).unwrap());
        assert_eq!(values[7], p4.get_value(7).unwrap());
    }

    #[test]
    pub fn hs_page_stress_test() {
        init();
        let mut p = Page::new(23);
        let mut original_vals: VecDeque<Vec<u8>> =
            VecDeque::from_iter(get_ascending_vec_of_byte_vec_02x(300, 20, 100));
        let mut stored_vals: Vec<Vec<u8>> = Vec::new();
        let mut stored_slots: Vec<SlotId> = Vec::new();
        let mut has_space = true;
        let mut rng = rand::thread_rng();
        // Load up page until full
        while has_space {
            let bytes = original_vals
                .pop_front()
                .expect("ran out of data -- shouldn't happen");
            let slot = p.add_value(&bytes);
            match slot {
                Some(slot_id) => {
                    stored_vals.push(bytes);
                    stored_slots.push(slot_id);
                }
                None => {
                    // No space for this record, we are done. go ahead and stop. add back value
                    original_vals.push_front(bytes);
                    has_space = false;
                }
            };
        }
        // let (check_vals, check_slots): (Vec<Vec<u8>>, Vec<SlotId>) = p.into_iter().map(|(a, b)| (a, b)).unzip();
        let bytes = p.to_bytes();
        let p_clone = Page::from_bytes(&bytes);
        let mut check_vals: Vec<Vec<u8>> = p_clone.into_iter().map(|(a, _)| a).collect();
        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
        trace!("\n==================\n PAGE LOADED - now going to delete to make room as needed \n =======================");
        // Delete and add remaining values until goes through all. Should result in a lot of random deletes and adds.
        while !original_vals.is_empty() {
            let bytes = original_vals.pop_front().unwrap();
            trace!("Adding new value (left:{}). Need to make space for new record (len:{}).\n - Stored_slots {:?}", original_vals.len(), &bytes.len(), stored_slots);
            let mut added = false;
            while !added {
                let try_slot = p.add_value(&bytes);
                match try_slot {
                    Some(new_slot) => {
                        stored_slots.push(new_slot);
                        stored_vals.push(bytes.clone());
                        let bytes = p.to_bytes();
                        let p_clone = Page::from_bytes(&bytes);
                        check_vals = p_clone.into_iter().map(|(a, _)| a).collect();
                        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
                        trace!("Added new value ({}) {:?}", new_slot, stored_slots);
                        added = true;
                    }
                    None => {
                        //Delete a random value and try again
                        let random_idx = rng.gen_range(0..stored_slots.len());
                        trace!(
                            "Deleting a random val to see if that makes enough space {}",
                            stored_slots[random_idx]
                        );
                        let value_id_to_del = stored_slots.remove(random_idx);
                        stored_vals.remove(random_idx);
                        p.delete_value(value_id_to_del)
                            .expect("Error deleting slot_id");
                        trace!("Stored vals left {}", stored_slots.len());
                    }
                }
            }
        }
    }

}
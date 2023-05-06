use crate::heapfile::HeapFile;
use crate::page::PageIntoIter;
use crate::page::Page;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    hf: Arc<HeapFile>,
    page_id: PageId,
    page_iter: Option<PageIntoIter>
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    #[allow(unused_variables)]
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        HeapFileIterator {
            hf,
            page_id: 0,
            page_iter: None::<PageIntoIter>,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);

    fn next(&mut self) -> Option<Self::Item> {
        // This statement checks if we have iterated all through all pages
        // The maximum page id in a heafile equals the number of pages minus 1
        if self.page_id >= self.hf.num_pages() {
            return None; 
        }

        let page = self.hf.read_page_from_file(self.page_id);

        match page {
            Ok(p) => {
                // Initialize a page_iter if none exists
                // This occurs when we finished the last page
                if let None = self.page_iter{
                    self.page_iter = Some(p.into_iter());
                }

                // Match next item
                match self.page_iter.as_mut().unwrap().next() {
                    // Next item exists
                    Some((vec, slot_id)) => {
                        return Some ((
                            vec, 
                            ValueId {
                                container_id: self.hf.container_id,
                                segment_id: None,
                                page_id: Some(self.page_id),
                                slot_id: Some(slot_id)
                            }));
                    }
                    // Next item doesn't exist
                    None => {
                        // Go to the next page if we have gone through every item on page
                        // Once we have exhausted page_iter, set it back to none
                        self.page_id += 1;
                        self.page_iter = None;
                        return self.next();
                    }
                }        
            },
            _ => panic! ("Something is broken in iter next"),
        }
    }
}


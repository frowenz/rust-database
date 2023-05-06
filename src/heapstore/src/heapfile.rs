#![allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused)]
#[allow(unused_variables)]
use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::fs::canonicalize;
use std::fs::metadata;
use std::io::BufWriter;
use std::io::{Seek, SeekFrom};
use std::ops::Deref;

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    // Track file, file_path and number of pages
    pub file: Arc<RwLock<File>>,
    pub file_path: Arc<RwLock<PathBuf>>,
    pub num_pages: Arc<RwLock<PageId>>,

    // Track this HeapFile's container Id
    pub container_id: ContainerId,

    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct HeapFileMetadata {
    pub file_path: PathBuf,
    pub num_pages: PageId,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        if file_path.as_path().exists(){
            info!("Calling new heapfile on a path that already exists. This will currently cause problems.");
        }

        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        // A note: This is not set up to load in existing heapfiles
        Ok(HeapFile {
            file: Arc::new(RwLock::new(file)),
            file_path: Arc::new(RwLock::new(file_path)),
            container_id,
            num_pages: Arc::new(RwLock::new(0 as PageId)),
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        return *self.num_pages.read().unwrap() as PageId;
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }

        // If page_id exists
        if pid < self.num_pages() {
            // Go to start of page
            let mut file = self.file.write().unwrap();
            let mut buffer = [0; PAGE_SIZE];
            file.seek(SeekFrom::Start(
                (PAGE_SIZE * pid as usize).try_into().unwrap(),
            ))?;

            // Read into buffer
            file.read(&mut buffer)?;

            // Turn buffer into page and return
            return Ok(Page::from_bytes(&buffer));
        } else {
            return Err(CrustyError::CrustyError("Invalid PageID".to_string()));
        }
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        trace!(
            "Writing page {} to file {}",
            page.get_page_id(),
            self.container_id
        );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }

        let pid: PageId = page.get_page_id();
        let mut file = self.file.write().unwrap();

        // Move the cursor to the start of the page in the file
        match file.seek(SeekFrom::Start(
            (PAGE_SIZE * pid as usize).try_into().unwrap(),
        )) {
            Ok(_) => {
                // Try to write the page to this spot in the file
                match file.write(&page.to_bytes()) {
                    Ok(_) => {
                        // If this is a new page, increment num_pages
                        if self.num_pages() as u16 <= pid {
                            let mut num_pages = self.num_pages.write().unwrap();
                            *num_pages += 1;
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(CrustyError::CrustyError(format!("Write Error: {:?}", e)));
                    }
                }
            }
            Err(e) => {
                return Err(CrustyError::CrustyError(format!("Seek Error: {:?}", e)));
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}

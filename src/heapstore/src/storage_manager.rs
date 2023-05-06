use crate::heapfile::{HeapFile, HeapFileMetadata};
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::atomic::AtomicU16;

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: PathBuf,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,

    // A hashmap of all the heapfiles
    #[serde(skip)]
    hf_dict: RwLock<HashMap<ContainerId, Arc<HeapFile>>>,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    // Inspired by memstore's storage manager's load function
    fn load(path: PathBuf) -> Self {
        let mut hf_dict_unlocked = HashMap::new();

        // Get each metadata file
        let entries: Vec<fs::DirEntry> = fs::read_dir(&path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|x| !x.path().is_dir())
            .filter(|x| x.path().extension().unwrap() == "md")
            .collect();

        // For each metadata file, rebuild its heapfile
        for entry in entries {
            let file = OpenOptions::new()
                .read(true)
                .open(entry.path())
                .expect("Failed to read file");

            let hf_metadata: HeapFileMetadata = serde_cbor::from_reader(file).unwrap();

            let hf_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&hf_metadata.file_path)
                .expect("Error reading file");

            // Get container id from the path
            let container_id: ContainerId = entry
                .path()
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string()
                .parse::<ContainerId>()
                .unwrap();

            // I have not set up heapfile to load paths from Heapfile::new()
            let heapfile = HeapFile {
                file: Arc::new(RwLock::new(hf_file)),
                file_path: Arc::new(RwLock::new(hf_metadata.file_path)),
                container_id,
                num_pages: Arc::new(RwLock::new(hf_metadata.num_pages)),
                read_count: AtomicU16::new(0),
                write_count: AtomicU16::new(0),
            };

            // Add heapfile to the hashmap
            hf_dict_unlocked.insert(container_id, Arc::new(heapfile));

            // Clear metadata file
            if let Err(e) = fs::remove_file(entry.path()) {
                panic!("Couldn't heapfile metadata: {}", e);
            }
        }

        return StorageManager {
            storage_path: path,
            is_temp: false,
            hf_dict: RwLock::new(hf_dict_unlocked),
        };
    }

    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        match self.hf_dict.read().unwrap().get(&container_id) {
            Some(hf) => match hf.read_page_from_file(page_id) {
                Ok(page) => Some(page),
                Err(_) => None,
            },
            None => None,
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        match self.hf_dict.write().unwrap().get(&container_id) {
            Some(hf) => return hf.write_page_to_file(page),
            None => {
                return Err(CrustyError::CrustyError(
                    "Could not find heapfile in write_page".to_string(),
                ))
            }
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        match self.hf_dict.read().unwrap().get(&container_id) {
            Some(hf) => return hf.num_pages(),
            None => panic!("Could not find heapfile in get_num_pages"),
        }
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        match self.hf_dict.read().unwrap().get(&container_id) {
            Some(hf) => {
                return (
                    hf.read_count.load(Ordering::Relaxed),
                    hf.write_count.load(Ordering::Relaxed),
                )
            }
            None => return (0, 0),
        }
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }

    /// For testing
    pub fn get_page_bytes(&self, container_id: ContainerId, page_id: PageId) -> Vec<u8> {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => p.to_bytes(),
            None => Vec::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_path for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_path: PathBuf) -> Self {
        if storage_path.exists() {
            info!(
                "Initializing heapstore::storage_manager from path {:?}",
                &storage_path
            );
            return StorageManager::load(storage_path);
        } else {
            info!(
                "Creating new heapstore::storage_manager with path {:?}",
                &storage_path,
            );
            if let Err(_) = std::fs::create_dir_all(&storage_path) {
                panic!("Error creating heapstore::storage_manager");
            }
            return StorageManager {
                storage_path,
                is_temp: false,
                hf_dict: RwLock::new(HashMap::new()),
            };
        };
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_path = gen_random_test_sm_dir();
        debug!("Making new temp storage_manager {:?}", storage_path);

        match fs::create_dir_all(&storage_path) {
            Err(e) => panic!("Failure to create directory: {}", e),
            _ => {}
        }

        return StorageManager {
            storage_path,
            is_temp: true,
            hf_dict: RwLock::new(HashMap::new()),
        };
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    #[allow(unused_variables)]
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        let mut value_id = ValueId::new(container_id);

        // Look for heapfile with matching container_id
        match self.hf_dict.read().unwrap().get(&container_id) {
            // If we find heap file
            Some(hf) => {
                // Iterate through all the pages in heapfile
                for page_id in 0..hf.num_pages() {
                    // Read in each page
                    match hf.read_page_from_file(page_id) {
                        Ok(mut page) => {
                            // See if value can be added
                            match page.add_value(&value) {
                                // If value was succesfully added (i.e. page has space)
                                Some(slot_id) => {
                                    // Overwrite old page with the page containing added value
                                    match hf.write_page_to_file(page) {
                                        Ok(()) => {
                                            value_id.page_id = Some(page_id);
                                            value_id.slot_id = Some(slot_id);
                                            return value_id;
                                        }
                                        Err(e) => panic!("Error in insert_value: {}", e),
                                    };
                                }
                                // If value was not added (most likely because page was full)
                                // We keep iterating
                                None => continue,
                            }
                        }
                        Err(_) => panic!("Something is broken in insert_value"),
                    }
                }

                // If there was no space in heapfile's pages
                // Make a new page
                let mut page = Page::new(hf.num_pages());
                match page.add_value(&value) {
                    // If value was succesfully added (i.e. page has space)
                    Some(slot_id) => {
                        match hf.write_page_to_file(page) {
                            Ok(()) => {
                                // After page is written, num_pages will include this page
                                // EX: If we have 1 page: num_pages() = 1; last_page_id = num_pages() - 1 = 0
                                value_id.page_id = Some(hf.num_pages() - 1);
                                value_id.slot_id = Some(slot_id);
                                return value_id;
                            }
                            Err(e) => panic!("Error in insert_value: {}", e),
                        };
                    }
                    None => panic!("Something is broken in insert_value"),
                }
            }
            None => panic!("Could not find container in insert_value"),
        }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    /// TODO: Is all Ok() really the ideal behavior?
    #[allow(unused_variables)]
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        // Get heapfile
        match self.hf_dict.write().unwrap().get(&id.container_id) {
            Some(hf) => {
                // Get the page that the value is stored in
                match hf.read_page_from_file(id.page_id.unwrap()) {
                    Ok(mut page) => {
                        // Delete value
                        match page.delete_value(id.slot_id.unwrap()) {
                            Some(_) => {
                                hf.write_page_to_file(page)?;
                                return Ok(());
                            }
                            None => Ok(()),
                        }
                    }
                    Err(_) => Ok(()),
                }
            }
            None => Ok(()),
        }
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        // Get heapfile
        match self.hf_dict.write().unwrap().get(&id.container_id) {
            Some(hf) => {
                // Get page value is stored in
                // TODO: Check if it is possible for a valid ValueId to have page of none
                match hf.read_page_from_file(id.page_id.unwrap()) {
                    Ok(mut page) => {
                        // Delete value
                        match page.delete_value(id.slot_id.unwrap()) {
                            Some(_) => {
                                // Try to add the value back into the same page
                                match page.add_value(&value) {
                                    Some(slot_id) => {
                                        // Rewrite the page
                                        hf.write_page_to_file(page)?;
                                        // Return updated valueid
                                        return Ok(ValueId {
                                            container_id: id.container_id,
                                            segment_id: id.segment_id,
                                            page_id: id.page_id,
                                            slot_id: Some(slot_id),
                                        });
                                    }
                                    // If page failed to add value, that means there is no longer room to add value back into same page
                                    // Hence, we must find somewhere else to insert it
                                    None => {
                                        // Still have to update page from which we deleted value
                                        hf.write_page_to_file(page)?;
                                        return Ok(self.insert_value(id.container_id, value, _tid));
                                    }
                                }
                            }
                            None => Err(CrustyError::CrustyError("Slot not found".to_string())),
                        }
                    }
                    Err(_) => Err(CrustyError::CrustyError("Page not found".to_string())),
                }
            }
            None => Err(CrustyError::CrustyError(
                "Could not find heapfile".to_string(),
            )),
        }
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {

        let mut path = self.storage_path.clone();
        path.push(container_id.to_string());
        path.set_extension("hf"); // Used to differentiate from metadata files

        // Make new heapfile
        let new_hf = HeapFile::new(path, container_id)?;

        // Add to dict
        self.hf_dict
            .write()
            .unwrap()
            .insert(container_id, Arc::new(new_hf));

        return Ok(());
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        match self.hf_dict.write().unwrap().remove(&container_id) {
            Some(_) => {
                let mut path = self.storage_path.clone();
                path.push(container_id.to_string());
                path.set_extension("hf");
                match fs::remove_file(path) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(CrustyError::CrustyError(format!("{}", e.to_string()))),
                }
            }
            None => {
                return Err(CrustyError::CrustyError(
                    "Unable to find container to remove".to_string(),
                ))
            }
        }
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let hfs = self.hf_dict.read().unwrap();
        match hfs.get(&container_id) {
            Some(hf) => return HeapFileIterator::new(tid, hf.clone()),
            None => panic!("invalid!"),
        }
    }

    #[allow(unused_variables)]
    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        match self.get_page(
            id.container_id,
            id.page_id.unwrap(),
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(page) => match page.get_value(id.slot_id.unwrap()) {
                Some(val) => Ok(val),
                None => Err(CrustyError::CrustyError("ValueId not in file".to_string())),
            },
            None => return Err(CrustyError::CrustyError("Page not found".to_string())),
        }
    }

    #[allow(unused_variables)]
    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_path.clone())?;
        fs::create_dir_all(self.storage_path.clone()).unwrap();
        self.hf_dict.write().unwrap().clear();
        return Ok(());
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        info!("Shutting down");

        if self.storage_path.to_string_lossy().is_empty() {
            info!("No path found, not persiting");
            return;
        }

        let hf_dict = self.hf_dict.read().unwrap();
        for (&container_id, hf) in hf_dict.iter() {
            let hf_metadata = HeapFileMetadata {
                file_path: hf.file_path.read().unwrap().clone(),
                num_pages: hf.num_pages.read().unwrap().clone(),
            };

            let mut metadata_path = self.storage_path.clone();
            metadata_path.push(container_id.to_string());
            metadata_path.set_extension("md");
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(metadata_path)
                .expect("Failed to create file");
            serde_cbor::to_writer(file, &hf_metadata).expect("Failed on persisting container");
        }
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Err(CrustyError::CrustyError(String::from("TODO")))
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.to_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_path);
            let remove_all = fs::remove_dir_all(self.storage_path.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}

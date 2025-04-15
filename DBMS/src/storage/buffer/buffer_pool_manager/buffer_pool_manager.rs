use crate::common::constants::NO_CORRESPONDING_FRAME_ID_MSG;
use crate::storage::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::storage::disk::disk_manager::{DiskManager, PageId};
use crate::storage::page::{Page, TablePage, TablePageHandle};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, RwLock, RwLockWriteGuard};

pub type FrameId = usize;

#[derive(Copy, Clone, Debug)]
pub struct FrameMetadata {
    frame_id: FrameId,
    pin_count: usize,
}

impl FrameMetadata {
    pub fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            pin_count: 0,
        }
    }

    #[allow(dead_code)]
    pub fn pin_count(&self) -> usize {
        self.pin_count
    }
    pub fn increment_pin_count(&mut self) {
        self.pin_count += 1;
    }
    pub fn decrement_pin_count(&mut self) {
        if self.pin_count == 0 {
            panic!("Pin count already at zero, cannot decrement.");
        }
        self.pin_count -= 1;
    }

    #[allow(dead_code)]
    pub fn frame_id(&self) -> &FrameId {
        &self.frame_id
    }
}

#[derive(Debug)]
pub struct BufferPoolManager {
    /// Number of page in the buffer pool.
    pub(crate) pool_size: usize,
    /// Array of buffer pool page.
    pub(crate) pages: Vec<TablePageHandle>,
    /// HashMap that maps page IDs to frame IDs (offsets in `page`).
    pub(crate) page_table: HashMap<PageId, FrameMetadata>,
    /// Manages reads and writes of page on disk.
    pub(crate) disk_manager: Arc<RwLock<DiskManager>>,
    /// Replacer to find unpinned page for replacement.
    pub(crate) replacer: Arc<RwLock<LRUKReplacer>>,
    /// List of free frames that don't have any page on them.
    pub(crate) free_list: VecDeque<FrameId>,
}

#[derive(Default)]
pub struct BufferPoolManagerBuilder {
    pool_size: Option<usize>,
    replacer_k: Option<usize>,
    disk_manager: Option<Arc<RwLock<DiskManager>>>,
}

impl BufferPoolManagerBuilder {
    pub fn pool_size(&mut self, pool_size: usize) -> &mut Self {
        self.pool_size = Some(pool_size);
        self
    }
    pub fn replacer_k(&mut self, replacer_k: usize) -> &mut Self {
        self.replacer_k = Some(replacer_k);
        self
    }
    pub fn disk_manager(&mut self, disk_manager: Arc<RwLock<DiskManager>>) -> &mut Self {
        self.disk_manager = Some(disk_manager);
        self
    }
    pub fn build(&self) -> BufferPoolManager {
        let pool_size = self
            .pool_size
            .expect("`pool_size` not initialized before build.");
        let replacer_k = self
            .replacer_k
            .expect("`replacer_k` not initialized before build.");
        let disk_manager = self
            .disk_manager
            .clone()
            .expect("`disk_manager` not initialized before build.");

        BufferPoolManager::new(pool_size, replacer_k, disk_manager)
    }

    pub fn build_with_handle(&self) -> Arc<RwLock<BufferPoolManager>> {
        Arc::new(RwLock::new(self.build()))
    }
}

impl BufferPoolManager {
    pub fn new(
        pool_size: usize,
        replacer_k: usize,
        disk_manager: Arc<RwLock<DiskManager>>,
    ) -> Self {
        BufferPoolManager {
            pool_size,
            pages: Vec::with_capacity(pool_size),
            page_table: HashMap::new(),
            disk_manager,
            replacer: Arc::new(RwLock::new(LRUKReplacer::new(pool_size, replacer_k))),
            free_list: (0..pool_size).collect(),
            // Initialize other fields here
        }
    }

    pub fn new_with_handle(
        pool_size: usize,
        replacer_k: usize,
        disk_manager: Arc<RwLock<DiskManager>>,
    ) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new(pool_size, replacer_k, disk_manager)))
    }

    pub fn builder() -> BufferPoolManagerBuilder {
        BufferPoolManagerBuilder::default()
    }

    /// Creates a new page in the buffer pool.
    ///
    /// This method allocates a new page and returns its identifier. If all
    /// frames are in use and cannot be evicted, it returns `None`.
    ///
    /// The frame should be pinned to prevent eviction, and its access history
    /// recorded.
    ///
    /// # Returns
    /// - `Some(PageId)`: The identifier of the newly created page if successful.
    /// - `None`: If no new page could be created due to all frames being in use.
    pub fn new_page(&mut self) -> Option<PageId> {
        if let Some(frame_id) = self.free_list.pop_front() {
            let mut disk_binding = self.disk_manager.write().unwrap();
            let new_page_id = disk_binding.allocate_new_page();
            let new_page = disk_binding.read_page(&new_page_id);
            let new_page_handle = Arc::new(RwLock::new(new_page));

            self.pages.insert(frame_id, new_page_handle);

            let mut frame_metadata = FrameMetadata::new(frame_id);
            frame_metadata.increment_pin_count();
            self.page_table.insert(new_page_id, frame_metadata);

            let mut replacer = self.replacer.write().unwrap();
            replacer.record_access(&frame_id, AccessType::Lookup);
            replacer.set_evictable(&frame_id, false);


            Some(new_page_id)
        } else {
            let mut replacer = self.replacer.write().unwrap();
            let evicted_frame_id = replacer.evict()?;

            drop(replacer);

            // Flush the evicted page if it is dirty
            let evict_page_id = self.pages.get(evicted_frame_id).unwrap().read().unwrap().page_id;
            let is_dirty = self.pages.get(evicted_frame_id).unwrap().read().unwrap().is_dirty;
            if is_dirty {
                self.flush_page(&evict_page_id);
            }

            // Read the new page from disk
            let mut disk_binding = self.disk_manager.write().unwrap();
            let new_page = disk_binding.read_page(&evict_page_id);
            let new_page_handle = Arc::new(RwLock::new(new_page));

            // Update the page table with the new page
            self.page_table.insert(evict_page_id, FrameMetadata::new(evicted_frame_id));
            self.pages.insert(evicted_frame_id, new_page_handle.clone());

            self.free_list.push_back(evicted_frame_id);

            drop(disk_binding);

            self.page_table.remove(&evict_page_id);
            self.new_page()
        }
    }

    /// Fetches a page from the buffer pool.
    ///
    /// This method attempts to retrieve the page identified by `page_id` from
    /// the buffer pool. If the page is not in the pool and all frames are
    /// currently in use and non-evictable (i.e., pinned), it returns `None`.
    ///
    /// The function first searches for the `page_id` in the buffer pool. If
    /// the page is not found, it selects a frame from the free list or, if
    /// empty, from the replacer, reading the page from disk and adding it to
    /// the buffer pool.
    ///
    /// Additionally, eviction is disabled for the frame, and its access history
    /// is recorded similarly to `NewPage`.
    ///
    /// Note: it is undefined behavior to call `fetch_page` on a `page_id` that
    /// does not exist in the page.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be fetched.
    ///
    /// # Returns
    /// - `Some(&mut TablePage)`: A mutable reference to the page if it is
    ///   successfully fetched.
    /// - `None`: If the `page_id` cannot be fetched due to all frames being
    ///   in use and non-evictable.
    pub fn fetch_page(&mut self, page_id: &PageId) -> Option<TablePageHandle> {

        // Check Buffer Pool
        if let Some(frame_metadata) = self.page_table.get(page_id).copied() {
            let frame_id = frame_metadata.frame_id();

            let mut replacer = self.replacer.write().unwrap();
            replacer.record_access(&frame_id, AccessType::Lookup);
            replacer.set_evictable(&frame_id, false);

            drop(replacer);

            {
                self.page_table.get_mut(page_id).unwrap().increment_pin_count();
            }

            let page_handle = self.pages.get(*frame_id).unwrap();
            return Some(Arc::clone(&page_handle));
        }

        // Check Free Frames
        if let Some(free_frame) = self.free_list.pop_front() {
            let page_handle = Arc::new(RwLock::new(TablePage::builder().build()));

            let mut disk_manager = self.disk_manager.write().unwrap();
            disk_manager.read_page(page_id);

            let frame_metadata = FrameMetadata::new(free_frame);
            self.page_table.insert(*page_id, frame_metadata);

            let mut replacer = self.replacer.write().unwrap();
            replacer.record_access(&free_frame, AccessType::Lookup);
            replacer.set_evictable(&free_frame, true);

            drop(replacer);

            {
                self.page_table.get_mut(page_id).unwrap().increment_pin_count();
            }

            self.pages.insert(free_frame, page_handle.clone());

            return Some(page_handle);
        }

        // See if you can evict a page
        let mut replacer = self.replacer.write().unwrap();
        if let Some(evicted_frame_id) = replacer.evict() {
            drop(replacer);

            // Flush the evicted page if it is dirty
            let evict_page_id = self.pages.get(evicted_frame_id).unwrap().read().unwrap().page_id;
            let is_dirty = self.pages.get(evicted_frame_id).unwrap().read().unwrap().is_dirty;
            if is_dirty {
                self.flush_page(&evict_page_id);
            }

            // Read the new page from disk
            let mut disk_binding = self.disk_manager.write().unwrap();
            let new_page = disk_binding.read_page(&page_id);
            let new_page_handle = Arc::new(RwLock::new(new_page));

            // Update the page table with the new page
            self.page_table.insert(*page_id, FrameMetadata::new(evicted_frame_id));
            self.pages.insert(evicted_frame_id, new_page_handle.clone());

            return Some(new_page_handle);
        }

        None
    }


    /// Unpins a page from the buffer pool.
    ///
    /// This method attempts to unpin the page identified by `page_id` from the
    /// buffer pool. If the page is not present in the pool, it should abort; or,
    /// if the page's pin count is already zero, the function returns `false` to
    /// indicate that no action was taken.
    ///
    /// When unpinning a page, the method decrements its pin count. If the pin
    /// count drops to zero, the frame containing the page becomes eligible for
    /// eviction by the replacer. The function also updates the page's dirty flag
    /// based on the `is_dirty` parameter, which indicates whether the page has
    /// been modified.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be unpinned.
    /// - `is_dirty`: A boolean flag that specifies whether the page should be
    ///   marked as dirty (`true`) or clean (`false`).
    ///
    /// # Returns
    /// - `true`: If the page was successfully unpinned (i.e., it was present
    ///   in the buffer pool and its pin count was greater than zero before this
    ///   call).
    /// - `false`: If the page was not in the buffer pool or its pin count was
    ///   zero or less before this call.
    pub fn unpin_page(&mut self, page_id: &PageId, is_dirty: bool) -> bool {
        if let Some(framedata) = self.page_table.get_mut(page_id) {
            return if framedata.pin_count > 0 {
                framedata.decrement_pin_count();
                if framedata.pin_count == 0 {
                    if let Some(page_handle) = self.pages.get(framedata.frame_id) {
                        let mut page = page_handle.write().unwrap();
                        page.is_dirty = is_dirty;

                        let mut replacer = self.replacer.write().unwrap();
                        replacer.set_evictable(&framedata.frame_id, true);
                    }
                }

                true
            } else {
                false
            };
        }

        panic!("Page {:?} not found", page_id);
    }

    /// Flushes a page to disk.
    ///
    /// This method writes the page identified by `page_id` to disk using
    /// the [`crate::storage::disk::disk_manager::DiskManager::write_page`] method.
    /// This operation is performed regardless of the page's dirty flag.
    /// After the page is successfully flushed, its dirty flag is reset to
    /// indicate that the page is now clean.
    ///
    /// If the page corresponding to `page_id` does not exist in the page,
    /// this method should abort.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be flushed.
    pub fn flush_page(&mut self, page_id: &PageId) {
        if let Some(frame_metadata) = self.page_table.get(page_id) {
            if let Some(page_handle) = self.pages.get(frame_metadata.frame_id) {
                let mut page = page_handle.write().unwrap();

                let mut disk_manager = self.disk_manager.write().unwrap();
                disk_manager.write_page((*page).clone());

                page.is_dirty = false;
            } else {
                panic!("Frame ID not found in pages.");
            }
        } else {
            panic!("Attempted to flush a page that does not exist in the buffer pool.");
        }
    }

    /// Flush all the page in the buffer pool to disk.
    pub fn flush_all_pages(&mut self) {
        let page_keys: Vec<PageId> = self.page_table.keys().cloned().collect();
        for page_id in page_keys {
            self.flush_page(&page_id);
        }
    }

    /// If the page identified by `page_id` is not in the buffer pool, this
    /// method aborts. If the page is pinned, it returns `false`. Otherwise,
    /// it deletes the page, updates the frame list,
    /// ([maybe] resets the page's memory and metadata, ) and calls
    /// [`crate::storage::disk::disk_manager::DiskManager::deallocate_page`] to free it
    /// on disk.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be deleted.
    ///
    /// # Returns
    /// - `true`: If the page was successfully deleted.
    /// - `false`: If the page was found but could not be deleted (e.g., it was pinned).
    pub fn delete_page(&mut self, page_id: PageId) -> bool {
        if let Some(frame_metadata) = self.page_table.get(&page_id) {
            if frame_metadata.pin_count > 0 {
                return false;
            }

            let frame_id = frame_metadata.frame_id;

            self.page_table.remove(&page_id);
            if let Some(page_handle) = self.pages.get(frame_id) {
                // reset page's memory and metadata
                let mut page = page_handle.write().unwrap();
                page.data.clear(); // clear the data
                page.tuple_info.clear(); // clear tuple info
                page.tuple_cnt = 0;
                page.deleted_tuple_cnt = 0;
                page.is_dirty = false;

                let mut disk_manager = self.disk_manager.write().unwrap();
                disk_manager.deallocate_page(&page_id);
            }

            self.free_list.push_back(frame_id);
            true
        } else {
            // Page not found
            panic!("Attempted to delete a page that does not exist in the buffer pool.");
        }
    }

    pub fn size(&self) -> usize {
        self.pool_size
    }

    pub(crate) fn get_is_dirty(&self, page_id: &PageId) -> bool {
        let frame_id = self
            .page_table
            .get(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG)
            .frame_id;
        self.pages.get(frame_id).unwrap().read().unwrap().is_dirty
    }

    pub(crate) fn get_pin_count(&self, page_id: &PageId) -> Option<usize> {
        Some(self.page_table.get(&page_id)?.pin_count)
    }

    pub(crate) fn set_is_dirty(&mut self, page_id: &PageId, is_dirty: bool) {
        let frame_id = self
            .page_table
            .get(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG)
            .frame_id;
        self.pages
            .get_mut(frame_id)
            .unwrap()
            .write()
            .unwrap()
            .set_is_dirty(is_dirty);
    }

    pub(crate) fn set_evictable(
        &mut self,
        page_id: &PageId,
        is_evictable: bool,
        replacer: &mut RwLockWriteGuard<LRUKReplacer>,
    ) {
        let frame_id = self
            .page_table
            .get(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG)
            .frame_id;
        replacer.set_evictable(&frame_id, is_evictable);
    }
}

impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        // Code to clean up resources
        println!("BufferPoolManager is being dropped");
    }
}

use crate::storage::buffer::buffer_pool_manager::FrameId;
use std::collections::{HashMap, VecDeque};
use log::Level::Error;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum AccessType {
    Unknown = 0,
    Lookup,
    Scan,
    Index,
}

#[derive(Debug)]
pub struct LRUKNode {
    /// History of last seen k timestamps of this page. Least recent timestamp stored in front.
    pub(crate) history: VecDeque<usize>,
    pub(crate) k: usize,
    pub(crate) is_evictable: bool,
}

impl LRUKNode {
    fn new(k: usize) -> Self {
        Self {
            history: VecDeque::with_capacity(k),
            k,
            is_evictable: false,
        }
    }

    /// # Returns
    /// - the k'th most recent timestamp's distance from the current timestamp if k accesses
    ///   have been recorded, and `usize::MAX` otherwise
    pub(crate) fn get_backwards_k_distance(&self, current_timestamp: usize) -> usize {
        if (self.has_infinite_backwards_k_distance()) {
            usize::MAX
        } else {
            current_timestamp - self.history.front().unwrap()
        }
    }

    pub(crate) fn has_infinite_backwards_k_distance(&self) -> bool {
        self.history.len() != self.k
    }
}

#[derive(Debug)]
pub struct LRUKReplacer {
    pub(crate) node_store: HashMap<FrameId, LRUKNode>,
    pub(crate) current_timestamp: usize,
    // Number of evictable frames in the replacer. Note: this might not be the size of `node_store`!
    pub(crate) curr_size: usize,
    // Maximum number of frames that can be stored in the replacer.
    pub(crate) max_size: usize,
    pub(crate) k: usize,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        Self {
            node_store: HashMap::new(),
            current_timestamp: 0,
            curr_size: 0,
            max_size: num_frames,
            k,
        }
    }

    pub fn builder() -> LRUKReplacerBuilder {
        LRUKReplacerBuilder {
            node_store: HashMap::new(),
            current_timestamp: 0,
            curr_size: 0,
            max_size: None,
            k: None,
        }
    }

    /// Evict the frame with the largest backwards k-distance. If a frame has
    /// not been accessed k times, its backwards k-distance is considered to
    /// be infinite. If there are multiple frames with infinite k-distance,
    /// choose the one to evict based on LRU.
    ///
    /// # Returns
    /// - an Option that is either `Some(frame_id)` if a frame with id `frame_id` was evicted, and
    ///   `None` otherwise
    pub fn evict(&mut self) -> Option<FrameId> {
        let mut largest_k_frame: Option<FrameId> = None;
        let mut largest_k_earliest_timestamp: usize = usize::MAX;
        let mut largest_k_dist: usize = 0;

        for (frame, node) in &mut self.node_store {
            if node.is_evictable {
                let node_k_dist = node.get_backwards_k_distance(self.current_timestamp);

                if node_k_dist > largest_k_dist {
                    largest_k_dist = node_k_dist;
                    largest_k_frame = Some(*frame);
                    largest_k_earliest_timestamp = *node.history.back().unwrap();
                } else if node_k_dist == largest_k_dist {
                    if *node.history.back().unwrap() < largest_k_earliest_timestamp {
                        largest_k_frame = Some(*frame);
                        largest_k_earliest_timestamp = *node.history.back().unwrap();
                    }
                }
            }
        }

        self.remove(&largest_k_frame?);
        largest_k_frame

    }

    /// Record an access to a frame at the current timestamp.
    ///
    /// This method should update the k-history of the frame and increment the current timestamp.
    /// If the given `frame_id` is invalid (i.e. >= `max_size`), this method throws an exception.
    ///
    /// # Parameters
    /// - `frame_id`: The id of the frame that was accessed
    /// - `access_type`: The type of access that occurred (e.g., Lookup, Scan, Index)
    pub fn record_access(&mut self, frame_id: &FrameId, _access_type: AccessType) {
        if *frame_id >= self.max_size {
            panic!("Invalid frame_id");
        }

        if let Some(node) = self.node_store.get_mut(frame_id) {
            if node.history.len() < node.k {
                node.history.push_back(self.current_timestamp);
            } else {
                node.history.pop_front();
                node.history.push_back(self.current_timestamp);
            }
        } else {
            let mut new_node = LRUKNode::new(self.k);
            new_node.history.push_back(self.current_timestamp);
            self.node_store.insert(frame_id.clone(), new_node);
        }
        self.current_timestamp += 1;
    }

    /// Set the evictable status of a frame. Note that replacer's curr_size is equal
    /// to the number of evictable frames.
    ///
    /// If a frame was previously evictable and is set to be non-evictable,
    /// then curr_size should decrement. If a frame was previously non-evictable and
    /// is to be set to evictable, then curr_size should increment. If the frame id is
    /// invalid, throw an exception or abort the process.
    ///
    /// For other scenarios, this function should terminate without modifying anything.
    ///
    /// # Parameters
    /// - `frame_id`: id of the frame whose 'evictable' status will be modified
    /// - `set_evictable`: whether the given frame is evictable or not
    pub fn set_evictable(&mut self, frame_id: &FrameId, set_evictable: bool) {
        if let Some(frame) = self.node_store.get_mut(frame_id) {
            if frame.is_evictable != set_evictable {
                if set_evictable {
                    self.curr_size += 1;
                } else {
                    self.curr_size -=1;
                }
                frame.is_evictable = set_evictable;
            }
        } else {
            panic!("Invalid frame ID provided");
        }
    }

    /// Remove an evictable frame from the replacer, along with its access history.
    /// This function should also decrement replacer's size if removal is successful.
    ///
    /// Note that this is different from evicting a frame, which always removes the frame
    /// with the largest backward k-distance. This function removes the specified frame id,
    /// no matter what its backward k-distance is.
    ///
    /// If `remove` is called on a non-evictable frame, throw an exception or abort the
    /// process.
    ///
    /// If the specified frame is not found, directly return from this function.
    ///
    /// # Parameters
    /// - `frame_id`: id of the frame to be removed
    pub fn remove(&mut self, frame_id: &FrameId) {
        if let Some(frame) = self.node_store.get(frame_id) {
            if frame.is_evictable {
                self.node_store.remove(frame_id);
                self.curr_size -= 1; // Decrement the size since a frame was removed
            } else {
                panic!("Tried to remove a non-evictable frame");
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_full_capacity(&self) -> bool {
        self.curr_size == self.max_size
    }

    // Returns the number of evictable frames in the replacer.
    pub fn size(&self) -> usize {
        self.curr_size
    }

    fn increment_current_size(&mut self) {
        self.curr_size += 1;
    }

    fn decrement_current_size(&mut self) {
        if self.curr_size == 0 {
            panic!("Attempted to decrement current size, which is already 0");
        }
        self.curr_size -= 1;
    }
}

pub struct LRUKReplacerBuilder {
    node_store: HashMap<FrameId, LRUKNode>,
    current_timestamp: usize,
    curr_size: usize,
    max_size: Option<usize>,
    k: Option<usize>,
}

impl LRUKReplacerBuilder {
    pub fn max_size(mut self, num_frames: usize) -> Self {
        assert!(num_frames > 0);
        self.max_size = Some(num_frames);
        self
    }

    pub fn k(mut self, k: usize) -> Self {
        assert!(k > 0);
        self.k = Some(k);
        self
    }

    pub fn build(self) -> LRUKReplacer {
        LRUKReplacer {
            node_store: self.node_store,
            current_timestamp: self.current_timestamp,
            curr_size: self.curr_size,
            max_size: self
                .max_size
                .expect("Replacer size was not specified before build."),
            k: self.k.expect("k was not specified before build."),
        }
    }
}

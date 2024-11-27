//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);  // reserve: 用来保留，预留容量的，并不改变容器的有效元素个数, 如果n
                                 // 小于原来容器的大小，那么白白调用

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */

/*这段注释是C++代码的一部分，它描述了一个名为NewPage的成员函数，该函数的作用是分配一个新的页面到磁盘上。
实现

你需要维护一个线程安全、单调递增的计数器，形式为std::atomic<page_id_t>。有关原子操作的更多信息，请查看原子操作的文档。

同时，确保阅读DeletePage的文档！你可以假设你永远不会用完磁盘空间（通过DiskScheduler::IncreaseDiskSpace），所以这个函数_不能_失败。

一旦你通过计数器分配了新页面，确保调用DiskScheduler::IncreaseDiskSpace，这样你就有足够的磁盘空间了！
待办事项（P1）

添加实现。

    返回值：新分配页面的页面ID。
*/

//找到一个空闲的frame，新分配一个物理页，并将该物理页的内容读取到刚找到的这个frame中
auto BufferPoolManager::NewPage() -> page_id_t {
  std::lock_guard<std::mutex> lock(*bpm_latch_);

  //auto frame_id = replacer_->Evict().value();

  bool use_free = true;
  page_id_t free_frame_id = INVALID_PAGE_ID;        //缓冲池现有frame
  
  // 先看free_list是否有
  if (!free_frames_.empty()) {
    //取出free_frames的一个frame
    free_frame_id = free_frames_.front();
    free_frames_.pop_front();
  }
  else{
    use_free = false;
  }
  //在检查lru_k替换策略是否可行
  if(use_free == false) {
    auto pid = replacer_->Evict();  // LRU——k替换策略的分配id
    // if (!pid) {
    //   //不可行则只能使用DiskScheduler::IncreaseDiskSpace
    //   // disk_scheduler_->IncreaseDiskSpace(num_frames_ + 1);            ///////////////////////存疑
    //   //由于没有可以分配的空闲frame_id
    //   return INVALID_PAGE_ID;
    // }
    free_frame_id = pid.value();
  }

  auto &newpage = frames_[free_frame_id];

  //考虑是否为脏页, 写回disk
  // if (newpage->is_dirty_) {
  //   auto promise = disk_scheduler_->CreatePromise();
  //   auto future = promise.get_future();
  //   disk_scheduler_->Schedule({true, newpage->GetDataMut(), newpage->frame_id_, std::move(promise)});
  //   future.get();
  //   // ! clean
  //   newpage->is_dirty_ = false;
  // }
  page_table_.erase(newpage->frame_id_);                            //  移除关系
  auto allocate_page_id = static_cast<page_id_t>(next_page_id_++);  //新分配给这个帧的数据页的 ID(待返回值)
  disk_scheduler_->IncreaseDiskSpace(allocate_page_id);             //调用看是否需要增加磁盘空间
  page_table_.emplace(allocate_page_id, free_frame_id);             //建立新关系

  // 把新page的参数更新下
  // newpage->frame_id_ = allocate_page_id;

  newpage->Reset();  //清空frame数据
  // 更新replacer_
  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);
  //newpage->pin_count_++;                                    /////////////////
  return allocate_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */

/**
 这段注释是C++代码的一部分，它描述了一个名为`DeletePage`的成员函数，该函数的作用是从数据库中删除一个页面，
 包括磁盘上的页面和内存中的页面。如果页面在缓冲池中被固定（pinned），则该函数不会执行任何操作，并返回`false`。否则，该函数会从磁盘和内存中删除页面（如果它仍然在缓冲池中），并返回`true`。

### 实现
实现这个函数时，你需要考虑页面或其元数据可能存在的所有位置，并使用这些信息来指导实现。你可能希望在实现`CheckedReadPage`和`CheckedWritePage`之后实现这个函数。

理想情况下，我们希望确保磁盘上的所有空间都被有效利用。这意味着被删除的页面在磁盘上占用的空间应该以某种方式变得可供`NewPage`分配的新页面使用。

如果你想尝试这样做，可以自由地进行。但是，对于这个实现，你被允许假设你不会用完磁盘空间，并且只需在`NewPage`中继续向上分配磁盘空间。

为了（非存在的）风格分数，你仍然可以调用`DeallocatePage`，以防你想在未来实现稍微更空间高效的东西。

### 待办事项（P1）
添加实现。

* `page_id_t page_id`：要删除的页面的页面ID。
* 返回值：`false`如果页面存在但不能被删除，`true`如果页面不存在或删除成功。

 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }
  std::scoped_lock lock(*bpm_latch_);
  // 如果页面存在
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    auto &page = frames_[frame_id];
    // 如果页面pin
    if (page->pin_count_ > 0) {
      return false;
    }
    // 删除页面
    page_table_.erase(page_id);
    free_frames_.push_back(frame_id);
    replacer_->Remove(frame_id);
    // 更新参数
    page->Reset();
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  }
  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */

/**
 * @brief 获取一个可选的写锁保护的页面数据。用户可以根据需要指定 `AccessType`。
 *
 * 如果无法将该页的数据加载到内存中，则该函数将返回 `std::nullopt`。
 *
 * 页面数据只能通过页面保护器访问。`BufferPoolManager` 的用户应该根据他们想要访问数据的模式来获取 `ReadPageGuard` 或
 * `WritePageGuard`，以确保任何数据访问都是线程安全的。
 *
 * 任何时候只能有一个 `WritePageGuard` 来读写一个页面。这允许数据访问既是不可变的又是可变的，这意味着拥有
 * `WritePageGuard` 的线程可以随意操作页面的数据。如果用户希望多个线程同时读取该页面，他们必须使用 `CheckedReadPage`
 * 获取 `ReadPageGuard`。
 *
 * ### 实现
 *
 * 你需要实现三种主要情况。前两种相对简单：一种是有足够可用内存的情况，另一种是不需要执行任何额外 I/O
 * 的情况。考虑一下这两种情况具体需要做什么。
 *
 * 第三种情况是最棘手的，当我们没有容易获得的可用内存时。缓冲池的任务是找到可用于加载页面数据的内存，使用之前实现的替换算法找到候选帧进行驱逐。
 *
 * 一旦缓冲池确定了要驱逐的帧，可能需要执行多个 I/O 操作来将我们想要的数据页加载到该帧中。
 *
 * 与 `CheckedReadPage` 可能有很多共享代码，因此创建辅助函数可能很有用。
 *
 * 这两个函数是这个项目的核心，所以我们不会给你更多的提示。祝你好运！
 *
 * TODO(P1): 添加实现。
 *
 * @param page_id 要写入的页面的 ID。
 * @param access_type 页面访问类型。
 * @return std::optional<WritePageGuard> 一个可选的锁保护器，如果没有更多的空闲帧（内存不足），则返回
 * `std::nullopt`，否则返回一个 `WritePageGuard`，确保对页面数据的独占和可变访问。
 */

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  //UNIMPLEMENTED("TODO(P1): Add implementation.");



  ////////还需要添加并发控制

  
  //BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is invalid");
  if(page_id == INVALID_PAGE_ID) {return std::nullopt;}
  //检查page_id是否存在
  auto iter = page_table_.find(page_id);
  //如果在内存中，则直接构造guard
  if(iter != page_table_.end()) {
    auto frame_id = iter->second;
    auto &frame = frames_[frame_id];
    frame->pin_count_++;                                  //////////////////////////
    auto write_guard = WritePageGuard(page_id, frame, replacer_, bpm_latch_);                     //如果在构造函数内增加pin_count则每创建一个构造函数就会增1
    write_guard.is_valid_  = true;
    
    return std::make_optional<WritePageGuard>(std::move(write_guard));
  }

  
  // explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
  //                         std::shared_ptr<std::mutex> bpm_latch);
  
  //auto write_guard_ = std::optional<WritePageGuard>(write_guard);



  std::lock_guard<std::mutex> lock(*bpm_latch_);

  //auto frame_id = replacer_->Evict().value();

  bool use_free = true;
  page_id_t free_frame_id;        //缓冲池现有frame
  auto pid = replacer_->Evict();  // LRU——k替换策略的分配id
  // 先看free_list是否有
  if (!free_frames_.empty()) {
    //取出free_frames的一个frame
    free_frame_id = free_frames_.front();
    free_frames_.pop_front();
  }
  //在检查lru_k替换策略是否可行
  else {
    use_free = false;
    if (!pid) {
      //不可行则只能使用DiskScheduler::IncreaseDiskSpace
      // disk_scheduler_->IncreaseDiskSpace(num_frames_ + 1);            ///////////////////////存疑
      //由于没有可以分配的空闲frame_id
      return std::nullopt;
    }
  }
  if (!use_free) {
    free_frame_id = pid.value();
  }
  auto &newpage = frames_[free_frame_id];

  
  // if (newpage->is_dirty_) {
  //   auto promise = disk_scheduler_->CreatePromise();
  //   auto future = promise.get_future();
  //   disk_scheduler_->Schedule({true, newpage->GetDataMut(), newpage->frame_id_, std::move(promise)});
  //   future.get();
  //   // ! clean
  //   newpage->is_dirty_ = false;
  // }

  //考虑是否为脏页, 写回disk
  if(newpage->is_dirty_){
    FlushPage(page_id);
  }
  page_table_.erase(newpage->frame_id_);                            //  移除关系
  // auto allocate_page_id = static_cast<page_id_t>(next_page_id_++);  //新分配给这个帧的数据页的 ID(待返回值)
  // disk_scheduler_->IncreaseDiskSpace(allocate_page_id);             //调用看是否需要增加磁盘空间
  page_table_.emplace(page_id, free_frame_id);             //建立新关系

  // 把新page的参数更新下
  // newpage->frame_id_ = allocate_page_id;

  newpage->Reset();  //清空frame数据
  // 更新replacer_
  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);
  

  auto write_guard = WritePageGuard(page_id, newpage, replacer_, bpm_latch_);
  write_guard.is_valid_  = true;                                    ////////////////////获得有效 ReadPageGuard 的唯一方法是通过缓冲池管理器！！！！！！！！！！！！！！！！！
  newpage->pin_count_++;                                    ///////////////增加pin放在内部的函数中
  return std::make_optional<WritePageGuard>(std::move(write_guard));

}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */

/**
 * @brief 获取对一页数据的可选读锁保护。用户可以根据需要指定 `AccessType`。
 *
 * 如果无法将该页数据加载到内存中，此函数将返回 `std::nullopt`。
 *
 * 页面数据**只能**通过页面保护对象访问。`BufferPoolManager` 的用户应根据其访问数据的方式获取 `ReadPageGuard` 或
 * `WritePageGuard`，以确保所有数据访问都是线程安全的。
 *
 * 任何数量的 `ReadPageGuard`
 * 都可以在不同线程中同时读取同一页数据。但是，所有数据访问必须是不可变的。如果用户想要修改页面的数据，他们必须改为使用
 * `CheckedWritePage` 获取 `WritePageGuard`。
 *
 * ### 实现
 *
 * 请参阅 `CheckedWritePage` 的实现细节。
 *
 * TODO(P1): 添加实现。
 *
 * @param page_id 要读取的页面的 ID。
 * @param access_type 页面访问的类型。
 * @return std::optional<ReadPageGuard> 一个可选的闩锁保护对象，如果不再有空闲帧（内存不足），则返回
 * `std::nullopt`，否则返回一个 `ReadPageGuard`，确保对页面数据的共享和只读访问。
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  //UNIMPLEMENTED("TODO(P1): Add implementation.");
  //BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is invalid");
  if(page_id == INVALID_PAGE_ID){return std::nullopt;}
  //检查page_id是否存在
  auto iter = page_table_.find(page_id);
  //如果在内存中，则直接构造guard
  if(iter != page_table_.end()) {
    auto frame_id = iter->second;
    auto &frame = frames_[frame_id];
    frame->pin_count_++;
    auto read_guard = ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
    read_guard.is_valid_ = true;
    return std::make_optional<ReadPageGuard>(std::move(read_guard));
  }

  
  // explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
  //                         std::shared_ptr<std::mutex> bpm_latch);
  
  //auto write_guard_ = std::optional<WritePageGuard>(write_guard);



  std::lock_guard<std::mutex> lock(*bpm_latch_);

  //auto frame_id = replacer_->Evict().value();

  bool use_free = true;
  page_id_t free_frame_id;        //缓冲池现有frame
  auto pid = replacer_->Evict();  // LRU——k替换策略的分配id
  // 先看free_list是否有
  if (!free_frames_.empty()) {
    //取出free_frames的一个frame
    free_frame_id = free_frames_.front();
    free_frames_.pop_front();
  }
  //在检查lru_k替换策略是否可行
  else {
    use_free = false;
    if (!pid) {
      //不可行则只能使用DiskScheduler::IncreaseDiskSpace
      // disk_scheduler_->IncreaseDiskSpace(num_frames_ + 1);            ///////////////////////存疑
      //由于没有可以分配的空闲frame_id
      return std::nullopt;
    }
  }
  if (!use_free) {
    free_frame_id = pid.value();
  }
  auto &newpage = frames_[free_frame_id];

  //考虑是否为脏页, 写回disk
  if (newpage->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, newpage->GetDataMut(), newpage->frame_id_, std::move(promise)});
    future.get();
    // ! clean
    newpage->is_dirty_ = false;
  }
  page_table_.erase(newpage->frame_id_);                            //  移除关系
  // auto allocate_page_id = static_cast<page_id_t>(next_page_id_++);  //新分配给这个帧的数据页的 ID(待返回值)
  // disk_scheduler_->IncreaseDiskSpace(allocate_page_id);             //调用看是否需要增加磁盘空间
  page_table_.emplace(page_id, free_frame_id);             //建立新关系

  // 把新page的参数更新下
  // newpage->frame_id_ = allocate_page_id;

  newpage->Reset();  //清空frame数据
  // 更新replacer_
  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);
  

  auto read_guard = ReadPageGuard(page_id, newpage, replacer_, bpm_latch_);
  read_guard.is_valid_ = true;
  newpage->pin_count_++;
  return std::make_optional<ReadPageGuard>(std::move(read_guard));
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);
  //guard_opt->is_valid_ = true;
  //guard_opt.value().frame_->pin_count_++;                                   ////////////////////
  if (!guard_opt.has_value()) {
    std::cerr << fmt::format("\n`CheckedPageWrite` failed to bring in page {}\n\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */

/**
 * @brief 包装 `CheckedReadPage` 函数，如果存在内部值，则对其进行解包。
 *
 * 如果 `CheckedReadPage` 返回 `std::nullopt`，**此函数将终止整个进程**。
 *
 * 此函数**仅**应用于测试和提高使用体验。如果缓冲池管理器可能耗尽内存，请使用 `CheckedPageWrite` 来处理这种情况。
 *
 * 有关实现的更多信息，请参阅 `CheckedReadPage` 的文档。
 *
 * @param page_id 要读取的页面的 ID。
 * @param access_type 页面访问的类型。
 * @return ReadPageGuard 页面保护对象，确保对页面数据的共享和只读访问。
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);
  //guard_opt.value().frame_->pin_count_++;
  if (!guard_opt.has_value()) {
    std::cerr << fmt::format("\n`CheckedPageRead` failed to bring in page {}\n\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  //写回脏页

  std::scoped_lock lock(*bpm_latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is invalid");
  // 如果页面存在
  auto iter = page_table_.find(page_id);
  // If the given page is not in memory, this function will return `false`
  if (iter == page_table_.end()) {
    return false;
  }

  auto frame_id = iter->second;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, frames_[frame_id]->GetDataMut(), page_id, std::move(promise)});
  future.get();
  frames_[frame_id]->is_dirty_ = false;
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock(*bpm_latch_);
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  for (auto &[page_id, frame_id] : page_table_) {
    BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is invalid");
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, frames_[frame_id]->GetDataMut(), page_id, std::move(promise)});
    future.get();
    frames_[frame_id]->is_dirty_ = false;
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */

/**
 * @brief 获取页面的引用计数。如果页面不存在内存中，则返回 `std::nullopt`。
 *
 * 此函数是线程安全的。调用者可以在访问同一页面的多线程环境中调用此函数。
 *
 * 此函数用于测试目的。如果此函数实现不正确，肯定会导致测试套件和自动评分器出现问题。
 *
 * # 实现
 *
 * 我们将使用此函数来测试您的缓冲池管理器是否正确管理引用计数。由于 `FrameHeader` 中的 `pin_count_`
 * 字段是一个原子类型，因此无需获取持有我们想要查看的页面的框架的闩锁。 相反，您可以简单地使用原子 `load`
 * 安全地加载存储的值。但是，您仍然需要获取缓冲池闩锁。
 *
 * 同样，如果您不熟悉原子类型，请参阅 C++ 官方文档 [此处](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): 添加实现
 *
 * @param page_id 要获取引用计数的页面的 ID。
 * @return std::optional<size_t> 如果页面存在，则返回引用计数，否则返回 `std::nullopt`。
 */

auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::scoped_lock lock(*bpm_latch_);
  // auto count = std::make_optional<size_t>();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return std::nullopt;
  }
  auto frame_id = iter->second;
  auto pin_count = frames_[frame_id]->pin_count_.load();
  return std::make_optional(pin_count);
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */

/**

@brief 唯一用于创建有效 RAII ReadPageGuard 的构造函数。
请注意，只有缓冲池管理器可以调用此构造函数。
TODO(P1): 添加实现。
@param page_id 要读取的页面的 ID。
@param frame 指向持有要保护的页面的帧的共享指针。
@param replacer 指向缓冲池管理器的替换器的共享指针。
@param bpm_latch 指向缓冲池管理器的锁的共享指针。 */

ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  //add write authority, point to process the frame
  // 因而我们需要在构造函数中修改 pin_count_和rwlatch_的状态（加锁），同时还要在 replacer_中标记这一 frame的访问状态和可逐出性。
  std::shared_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock，读可以有多个线程
  //frame_->pin_count_++;                                                              ////////////////////
  replacer_->RecordAccess(frame_->frame_id_);               //标记可访问性
  replacer_->SetEvictable(frame_->frame_id_, false);
  is_valid_ = true;

}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */

/**

@brief ReadPageGuard 的移动构造函数。
实现
如果您不熟悉移动语义，请在线学习相关资料。有很多优秀的资源（包括文章、微软教程、YouTube 视频）可以深入解释。
确保使另一个守卫失效，否则可能会遇到双重释放问题！对于两个对象，您需要更新至少 5 个字段。
TODO(P1): 添加实现。
@param that 另一个页面守卫。 */

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept 
{
  //注意：需要将原来对象Drop
  this->Drop();
  if(that.is_valid_){               //检查原对象是否有效
    page_id_ = that.GetPageId();
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    is_valid_ = true;

    //that.Drop();              //直接清除

  // 因而我们需要在构造函数中修改 pin_count_和rwlatch_的状态（加锁），同时还要在 replacer_中标记这一 frame的访问状态和可逐出性。
    std::shared_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock，读可以有多个线程
    //frame_->pin_count_++;                                   //////////
    replacer_->RecordAccess(frame_->frame_id_);               //标记可访问性
    replacer_->SetEvictable(frame_->frame_id_, false);
  }
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
  this->Drop();
  if(this != &that && that.is_valid_){
    page_id_ = that.GetPageId();
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    is_valid_ = true;
    that.is_valid_ = false;

  // 因而我们需要在构造函数中修改 pin_count_和rwlatch_的状态（加锁），同时还要在 replacer_中标记这一 frame的访问状态和可逐出性。
    std::shared_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock，读可以有多个线程
    //frame_->pin_count_++;                               //////////////
    replacer_->RecordAccess(frame_->frame_id_);               //标记可访问性
    replacer_->SetEvictable(frame_->frame_id_, false);
  }  
  return *this; 
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */

/**
 * @brief 手动丢弃一个有效的 `ReadPageGuard` 的数据。如果这个保护对象无效，这个函数什么也不做。
 *
 * ### 实现
 *
 * 确保你不会重复释放内存！同时，非常非常仔细地考虑你拥有哪些资源以及释放这些资源的顺序。
 * 如果你搞错了顺序，你很可能会在后续的 Gradescope 测试中失败。你可能还想在非常特定的情况下获取缓冲池管理器的锁...
 *
 * TODO(P1): 添加实现。
 */
//需要对Guard内部的Page进行**Unpin操作**，但是需要先检查Guard中是否保存了合法的Page。如果Page为nullptr，那么表明该Guard已经被初始化了，无需进行Drop操作
void ReadPageGuard::Drop() {  
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  if(!is_valid_){return;}
  //在这个方法中需要释放掉所有的指针
  //保证不能重复释放
  if(frame_ && frame_->pin_count_ <= 0) {return;}
  //if(!is_valid_) {return;}

  //std::shared_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock，读可以有多个线程
  

  if(frame_ && --frame_->pin_count_ == 0){                              //进行pin--
    replacer_->SetEvictable(frame_->frame_id_, true);        //如果无pin,则设置可以evit
  }
  
  //清除所有的指针
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;

  is_valid_ = false;

}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  //UNIMPLEMENTED("TODO(P1): Add implementation.");

  //add write authority, point to process the frame
  // 因而我们需要在构造函数中修改 pin_count_和rwlatch_的状态（加锁），同时还要在 replacer_中标记这一 frame的访问状态和可逐出性。
  std::unique_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock,写 时只有一个线程可以
  //frame_->pin_count_++;                                     //
  replacer_->RecordAccess(frame_->frame_id_);               //标记可访问性
  replacer_->SetEvictable(frame_->frame_id_, false);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept 
{
  if(that.is_valid_){               //检查原对象是否有效
    page_id_ = that.GetPageId();
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    is_valid_ = true;
    that.is_valid_ = false;                     //将原来的对象设置为无效

  // 因而我们需要在构造函数中修改 pin_count_和rwlatch_的状态（加锁），同时还要在 replacer_中标记这一 frame的访问状态和可逐出性。
    std::unique_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock，读可以有多个线程
    //frame_->pin_count_++;                               ////////////////////
    replacer_->RecordAccess(frame_->frame_id_);               //标记可访问性
    replacer_->SetEvictable(frame_->frame_id_, false);
  }
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
  if(this != &that && that.is_valid_){
    page_id_ = that.GetPageId();
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    is_valid_ = true;
    that.is_valid_ = false;

  // 因而我们需要在构造函数中修改 pin_count_和rwlatch_的状态（加锁），同时还要在 replacer_中标记这一 frame的访问状态和可逐出性。
    std::unique_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock，读可以有多个线程
    //frame_->pin_count_++;                                                 ////////
    replacer_->RecordAccess(frame_->frame_id_);               //标记可访问性
    replacer_->SetEvictable(frame_->frame_id_, false);
  }  
  return *this; 
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() { 
  //UNIMPLEMENTED("TODO(P1): Add implementation."); 
  

  if(!is_valid_){return;}
  //在这个方法中需要释放掉所有的指针
  //保证不能重复释放
  if(frame_ && frame_->pin_count_ == 0) {return;}

  //要注意写回，否则会丢失修改                                                ?????????
  // if(IsDirty()){
  //   BufferPoolManager& bmp = 
  // }
  //std::shared_lock<std::shared_mutex>(frame_->rwlatch_);          //add lock，读可以有多个线程
  if(--frame_->pin_count_ == 0){
    replacer_->SetEvictable(frame_->frame_id_, true);                     //如果pin = 0,则设置为evictable
  }
  //if(frame_->is_dirty_)
  
  //清除所有的指针
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;

  is_valid_ = false;

}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub

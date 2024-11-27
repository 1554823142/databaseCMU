//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <memory>
#include <utility>
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  bool evict_success = false;
  bool is_inf = false;
  size_t max_delta_time = 0;

  // if (Size() == 0) {
  //   return std::nullopt;
  // }
  frame_id_t ans = INVALID_PAGE_ID;;
  for (auto &[f_id, node] : node_store_) {
    //先检查是否可以evict
    if (!node.is_evictable_) {
      continue;
    }

    evict_success = true;
    //检查特殊情况
    if (node.history_.empty()) {
      ans = f_id;
      break;
    }

    //如果是
    if (node.history_.size() == k_ && !is_inf) {
      auto time_delta = current_timestamp_ - node.history_.front();
      if (max_delta_time < time_delta) {
        max_delta_time = time_delta;
        ans = f_id;
      }
    } else if (node.history_.size() < k_) {
      is_inf = true;
      auto time_delta = current_timestamp_ - node.history_.front();
      if (time_delta > max_delta_time) {
        max_delta_time = time_delta;
        ans = f_id;
      }
    }
  }
  if (evict_success) {
    curr_size_--;
    node_store_[ans].history_.clear();
    node_store_.erase(ans);
  }
  if(ans == INVALID_PAGE_ID) {return std::nullopt;}
  auto ret = std::optional<frame_id_t>(ans);
  return ret;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto helper = static_cast<size_t>(frame_id);
  BUSTUB_ASSERT(helper <= replacer_size_, "invalid frame_id");

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    // Fail to find out the LRUKNode.
    auto new_node_ptr = std::make_unique<LRUKNode>();
    if (access_type != AccessType::Scan) {
      new_node_ptr->history_.push_back(current_timestamp_++);
    }
    node_store_.insert(std::make_pair(frame_id, *new_node_ptr));
  } else {
    auto &node = iter->second;
    if (access_type != AccessType::Scan) {
      if (node.history_.size() == k_) {
        node.history_.pop_front();
      }
      node.history_.push_back(current_timestamp_++);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto helper = static_cast<size_t>(frame_id);
  BUSTUB_ASSERT(helper <= replacer_size_, "invalid frame_id");

  std::unique_ptr<LRUKNode> new_node_ptr;
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    // Fail to find out the LRUKNode.
    new_node_ptr = std::make_unique<LRUKNode>();
    node_store_.insert(std::make_pair(frame_id, *new_node_ptr));
  }
  auto &node = (iter == node_store_.end()) ? *new_node_ptr : iter->second;
  if (set_evictable && !node.is_evictable_) {
    node.is_evictable_ = set_evictable;
    curr_size_++;
  }
  if (!set_evictable && node.is_evictable_) {
    node.is_evictable_ = set_evictable;
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  auto &node = iter->second;
  BUSTUB_ASSERT(node.is_evictable_, "Called on a non-evictable frame.");

  node.history_.clear();
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
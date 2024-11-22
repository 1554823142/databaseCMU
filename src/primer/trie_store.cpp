#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  throw NotImplementedException("TrieStore::Get is not implemented.");

  root_lock_.lock();
  auto cur_thread_root_ = root_;
  root_lock_.unlock();
  // std::shared_lock(root_lock_);   // lock the threads

  auto value = cur_thread_root_.Get<T>(key);  // use the get func completed before is ok!
  if (value == nullptr) {
    // root_lock_.unlock();
    return std::nullopt;
  }

  return {ValueGuard<T>(std::move(cur_thread_root_), *value)};
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  throw NotImplementedException("TrieStore::Put is not implemented.");

  //声明一个局部的std::lock_guard对象，在其构造函数中进行加锁，在其析构函数中进行解锁。最终的结果就是：创建即加锁，作用域结束自动解锁
  std::lock_guard<std::mutex> lk(write_lock_);
  // write_lock_.lock();           //ensure there is only one writer at a time
  // root_lock_.lock();
  // auto cur_thread_root_ = root_;
  // root_lock_.unlock();

  // std::shared_ptr<TrieNode> new_root;
  auto new_root = root_.Put<T>(key, std::move(value));
  root_lock_.lock();
  root_ = new_root;
  root_lock_.unlock();
  // if (root_ != nullptr) {
  //   new_root = root_->Clone();
  // } else {
  //   new_root = std::make_shared<TrieNode>();
  // }

  // auto parent_node = new_root;
  // std::shared_ptr<TrieNode> new_node;
  // auto it = key.begin();
  // for (; it != key.end() && std::next(it) != key.end(); ++it) {
  //   auto iter = parent_node->children_.find(*it);
  //   //if (!(parent_node->HasChild(*it))) {
  //   if(iter == parent_node->children_.end()){
  //     new_node = std::make_shared<TrieNode>();
  //   } else {
  //     //new_node = parent_node->GetChildNode(*it)->Clone();
  //     new_node = iter->second->Clone();
  //   }
  //   parent_node->children_[*it] = new_node;
  //   parent_node = new_node;
  // }

  // auto value_ptr = std::make_shared<T>(std::move(value));

  // auto iter = parent_node->children_.find(*it);

  //  //if (key.empty() || !parent_node->HasChild(*it)) {
  //  if(key.empty() || iter == parent_node->children_.end()){
  //   parent_node->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
  // } else {
  //   //auto child_node = parent_node->GetChildNode(*it);
  //   auto child_node = iter->second;
  //   parent_node->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(child_node->children_, value_ptr);
  // }

  // // auto new_trie = std::make_shared<Trie>();
  // // new_trie->root_ = new_root;
  // root_ = new_root;

  // write_lock_.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  throw NotImplementedException("TrieStore::Remove is not implemented.");

  std::lock_guard<std::mutex> lk(write_lock_);
  auto new_root = root_.Remove(key);
  root_lock_.lock();
  root_ = new_root;
  root_lock_.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub

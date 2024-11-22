#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {
/*
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  throw NotImplementedException("Trie::Get is not implemented.");

  //参数key代表一条路径上的字符组合

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if(root_ == nullptr) return nullptr;
  const TrieNode* cur = root_.get();          //使用get获取ptr指向的指针
  for(char ch : key){
    auto iter = cur->children_.find(ch);
    if(iter == cur->children_.end()){
      return nullptr;
    }
    cur = iter->second.get();
  }


  //////////
  //尝试将节点转换为TrieNodeWithValue<T>类型
  const TrieNodeWithValue<T> * value_node = dynamic_cast<const TrieNodeWithValue<T> * >(cur);
  if(value_node && cur->children_.size() == 0)
    return value_node->value_.get();
  return nullptr;
}
*/

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // 你应该遍历 Trie 来找到与 key 对应的节点。如果节点不存在，则返回 nullptr。找到节点后，你应该使用 `dynamic_cast`
  // 将其转换为 `const TrieNodeWithValue<T> *`。如果 dynamic_cast 返回 `nullptr`，则表示值的类型不匹配，应该返回
  // nullptr。否则，返回值。

  if (root_ == nullptr) {
    return nullptr;
  }

  auto cur_node = root_;
  for (auto &c : key) {
    auto iter = cur_node->children_.find(c);
    // if (!(cur_node->HasChild(c))) {
    if (iter == cur_node->children_.end()) {
      return nullptr;
    }

    // cur_node = cur_node->GetChildNode(c);
    cur_node = iter->second;
    if (cur_node == nullptr) {
      return nullptr;
    }
  }
  if (key.empty()) {
    // cur_node = cur_node->GetChildNode(*key.begin());
    auto iter = cur_node->children_.find(*key.begin());
    if (iter != cur_node->children_.end()) {
      cur_node = iter->second;
    }
  }

  if (!cur_node->is_value_node_) {
    return nullptr;
  }

  auto cur_node_with_value = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur_node);
  if (cur_node_with_value == nullptr) {
    return nullptr;
  }

  return cur_node_with_value->value_.get();
}
/*
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  //在搜索的路径上不断CLone（），从而得到非const节点对childern进行操作
  std::shared_ptr<TrieNode> new_root;
  //if(key == nullptr && root_ == nullptr) return nullptr;
  if(root_ == nullptr){
    new_root = std::make_shared<TrieNode>();
  }
  else new_root = root_->Clone();
  //std::shared_prt<TrieNode>cur = std::shared_ptr<TrieNode>(root_->Clone());
  auto cur = new_root;
  if(root_ != nullptr)
    new_root = root_->Clone();
  std::shared_ptr<TrieNode> new_node;
  for(char ch : key){
    auto iter = cur->children_.find(ch);
    if(iter == cur->children_.end()){
      // if not exist ,then create a new TrieNode
      new_node = std::make_shared<TrieNode>();
    }
    //如果存在则直接Clone
    else new_node = iter->second->Clone();

    // notion that the node which cloned is new node, so keep link with their parent
    cur->children_[ch] = new_node;

    cur = new_node;
    //cur = iter->second.get();
    }


  //`T` might be a non-copyable type
  auto node_value = std::make_shared<T>(std::move(value));

  // if the root is single, and the value is null, assign the value to the root
  auto iter = cur.get()->children_.find(key[key.size() - 1]);
  if(key.size() >= 1){
    if(key.empty() || iter == cur.get()->children_.end()){
    cur.get()->children_[key[key.size() - 1]] = std::make_shared<TrieNodeWithValue<T>>(node_value);
    }
    else{
    //auto children_node = iter->second().get();
    cur.get()->children_[key[key.size() - 1]] = std::make_shared<TrieNodeWithValue<T>>(cur->children_, node_value);
    }
  }


  //create a new tire
  auto new_trie = std::make_shared<Trie>();

  new_trie->root_ = new_root;
  return *new_trie;

  // std::shared_ptr<T> insert_node = std::move(value);
}
*/

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // 注意，`T` 可能是不可复制的类型。在创建该值的 `shared_ptr` 时始终使用 `std::move`。

  // 你应该遍历 Trie，并在必要时创建新节点。如果与 key 对应的节点已存在，则应创建新的 `TrieNodeWithValue`。

  std::shared_ptr<TrieNode> new_root;
  if (root_ != nullptr) {
    new_root = root_->Clone();
  } else {
    new_root = std::make_shared<TrieNode>();
  }

  auto parent_node = new_root;
  std::shared_ptr<TrieNode> new_node;
  auto it = key.begin();
  for (; it != key.end() && std::next(it) != key.end(); ++it) {
    auto iter = parent_node->children_.find(*it);
    // if (!(parent_node->HasChild(*it))) {
    if (iter == parent_node->children_.end()) {
      new_node = std::make_shared<TrieNode>();
    } else {
      // new_node = parent_node->GetChildNode(*it)->Clone();
      new_node = iter->second->Clone();
    }
    parent_node->children_[*it] = new_node;
    parent_node = new_node;
  }

  auto value_ptr = std::make_shared<T>(std::move(value));

  auto iter = parent_node->children_.find(*it);

  // if (key.empty() || !parent_node->HasChild(*it)) {
  if (key.empty() || iter == parent_node->children_.end()) {
    parent_node->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
  } else {
    // auto child_node = parent_node->GetChildNode(*it);
    auto child_node = iter->second;
    parent_node->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(child_node->children_, value_ptr);
  }

  auto new_trie = std::make_shared<Trie>();
  new_trie->root_ = new_root;
  return *new_trie;
}

/*
auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<TrieNode> new_root;
  //if(key == nullptr && root_ == nullptr) return nullptr;
  if(root_ == nullptr){
    new_root = std::make_shared<TrieNode>();
  }
  else new_root = root_->Clone();
  //std::shared_prt<TrieNode>cur = std::shared_ptr<TrieNode>(root_->Clone());
  auto cur = new_root;
  std::shared_ptr<TrieNode> new_node;
  auto new_trie = std::make_shared<Trie>();
  std::vector<std::shared_ptr<TrieNode> > road;         //记录路径上的节点， 便于回溯清理删除不必要存在的节点
  road.emplace_back(cur);
  for(auto ch : key){
    auto iter = cur->children_.find(ch);
    if(iter == cur->children_.end()){
      new_trie->root_ = root_;
      return *new_trie;               //若没有该键，直接返回原来的树
    }

    cur.get()->children_[ch] = new_node;
    road.emplace_back(new_node);            //store the node along the road
    cur = new_node;
  }

  if(cur->is_value_node_ == false){
    new_trie->root_ = root_;
    return *new_trie;
  }

// 进行回溯删除
  auto trace_cur_node = std::make_shared<TrieNode>(cur->children_);
  for(int i = key.size() - 1; i >= 0; i--){
    auto trace_parent_node = road.back()->Clone();
    if(trace_parent_node->children_.empty())
      trace_parent_node->children_.erase(key[i]);             //删除
    else
      trace_parent_node->children_[key[i]] = trace_cur_node;

    //进行上层的回溯
    trace_cur_node = std::move(trace_parent_node);
    road.pop_back();
  }

  return Trie(trace_cur_node);

}

*/

auto Trie::Remove(std::string_view key) const -> Trie {
  // 你应该遍历 Trie，并在必要时移除节点。如果节点不再包含值，则应将其转换为
  // `TrieNode`。如果节点不再有子节点，则应将其移除。

  if (root_ == nullptr) {
    return {};
  }

  std::stack<std::shared_ptr<const TrieNode>> node_stack;

  auto cur_node = root_;
  for (auto &c : key) {
    node_stack.push(cur_node);
    auto iter = cur_node->children_.find(c);
    if (iter == cur_node->children_.end()) {  // no such road, then can't find such node, so return raw root
                                              // if (!(cur_node->HasChild(c))) {
      return Trie(root_);
    }
    cur_node = iter->second;
    // cur_node = cur_node->GetChildNode(c);
    if (cur_node == nullptr) {
      return Trie(root_);
    }
  }
  if (key.empty()) {
    auto iter = cur_node->children_.find(*key.begin());
    cur_node = iter->second;
    // cur_node = cur_node->GetChildNode(*key.begin());
  }

  if (!cur_node->is_value_node_) {
    return Trie(root_);
  }

  auto new_node = std::make_shared<const TrieNode>(cur_node->children_);
  for (auto rit = key.rbegin(); rit != key.rend(); ++rit) {
    auto new_parent = node_stack.top()->Clone();
    if (new_node->children_.empty() && !new_node->is_value_node_) {
      new_parent->children_.erase(*rit);
    } else {
      new_parent->children_[*rit] = new_node;
    }
    new_node = std::move(new_parent);
    node_stack.pop();
  }

  if (new_node->children_.empty()) {
    return {};
  }
  return Trie(new_node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

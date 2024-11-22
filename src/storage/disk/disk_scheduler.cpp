//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  //启动了一个线程，用于接收BufferPoolManager发来的读写磁盘请求，并将其放入一个请求队列（request_queue_）中

  //然后启动一个新线程(background_thread_)，不断从请求队列中获取请求，根据请求类型调用对应DiskManager的读写函数进行磁盘读写

  // emplace方法直接在std::thread对象内部构造线程，而不需要先创建一个线程对象然后再赋值给std::thread。它接受与构造函数相同的参数，并直接用于初始化线程对象
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

//接收请求并放入请求队列

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional<DiskRequest>(std::move(r))); }

/**
 * @brief 后台工作线程函数，处理已安排的请求。
 *
 * 后台线程需要在DiskScheduler存在时处理请求，即此函数不应返回，直到调用~DiskScheduler()为止。
 * 在那时，您需要确保函数返回。
 */
//线程函数，从请求队列中获取新请求，并根据请求类型调用磁盘读写函数
void DiskScheduler::StartWorkerThread() {
  std::optional<DiskRequest> request;
  //从队列中拿出请求
  while ((request = request_queue_.Get()) && request.has_value()) {
    if (request->is_write_) {
      disk_manager_->WritePage(request->page_id_, request->data_);
    } else {
      disk_manager_->ReadPage(request->page_id_, request->data_);
    }

    /// Callback used to signal to the request issuer when the request has been completed.
    request->callback_.set_value(true);
  }
}

}  // namespace bustub

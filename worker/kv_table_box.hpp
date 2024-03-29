#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"

#include "worker/kvpairs.hpp"
#include "worker/simple_range_manager.hpp"

#include "glog/logging.h"

#include <algorithm>
#include <vector>

namespace flexps {

/*
 * KVTableBox contains serveral operations shared by different KVTable.
 */
template <typename Val>
class KVTableBox {
 public:
  KVTableBox(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
             const AbstractPartitionManager* const partition_manager);
  KVTableBox(const KVTableBox&) = delete;
  KVTableBox& operator=(const KVTableBox&) = delete;
  KVTableBox(KVTableBox&& other) = delete;
  KVTableBox& operator=(KVTableBox&& other) = delete;

  using SlicedKVs = AbstractPartitionManager::SlicedKVs;

  void Clock();
  void Send(const SlicedKVs& sliced, bool is_add);
  void SendChunk(const SlicedKVs& sliced, bool is_add);
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals);
  void AddChunk(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals);
  SlicedKVs Slice(const KVPairs<char>& send);
  SlicedKVs SliceChunk(const KVPairs<char>& send);
  
  void HandleMsg(Message& msg);
  template <typename C>
  void HandleFinish(const third_party::SArray<Key>& keys, C* vals);
  template <typename C>
  void HandleChunkFinish(const third_party::SArray<Key>& keys, std::vector<C*> &vals);
  
  uint32_t app_thread_id_;
  uint32_t model_id_;

 private:
  // Not owned.
  ThreadsafeQueue<Message>* const send_queue_;
  // Not owned.
  const AbstractPartitionManager* const partition_manager_;

  std::vector<KVPairs<Val>> recv_kvs_;
};

template <typename Val>
KVTableBox<Val>::KVTableBox(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                            const AbstractPartitionManager* const partition_manager)
    : app_thread_id_(app_thread_id),
      model_id_(model_id),
      send_queue_(send_queue),
      partition_manager_(partition_manager) {}

// SArray version Add
template <typename Val>
void KVTableBox<Val>::Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
  KVPairs<char> kvs;
  kvs.keys = keys;
  kvs.vals = vals;
  CHECK_NOTNULL(partition_manager_);
  SlicedKVs sliced = partition_manager_->Slice(kvs);
  Send(sliced, true);
}

template <typename Val>
void KVTableBox<Val>::AddChunk(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
  KVPairs<char> kvs;
  kvs.keys = keys;
  kvs.vals = vals;
  CHECK_NOTNULL(partition_manager_);
  SlicedKVs sliced = partition_manager_->SliceChunk(kvs);
  SendChunk(sliced, true);
}


template <typename Val>
typename KVTableBox<Val>::SlicedKVs KVTableBox<Val>::Slice(const KVPairs<char>& send) {
  CHECK_NOTNULL(partition_manager_);
  return partition_manager_->Slice(send);
}

template <typename Val>
void KVTableBox<Val>::Send(const SlicedKVs& sliced, bool is_add) {
  CHECK_NOTNULL(partition_manager_);
  for (size_t i = 0; i < sliced.size(); ++i) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = sliced[i].first;
    msg.meta.model_id = model_id_;
    msg.meta.flag = is_add ? Flag::kAdd : Flag::kGet;
    const auto& kvs = sliced[i].second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      if (is_add) {
        msg.AddData(kvs.vals);
      }
    }
    send_queue_->Push(std::move(msg));
  }
}

template <typename Val>
typename KVTableBox<Val>::SlicedKVs KVTableBox<Val>::SliceChunk(const KVPairs<char>& send) {
  CHECK_NOTNULL(partition_manager_);
  return partition_manager_->SliceChunk(send);
}

template <typename Val>
void KVTableBox<Val>::SendChunk(const SlicedKVs& sliced, bool is_add) {
  CHECK_NOTNULL(partition_manager_);
  for (size_t i = 0; i < sliced.size(); ++i) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = sliced[i].first;
    msg.meta.model_id = model_id_;
    msg.meta.flag = is_add ? Flag::kAddChunk : Flag::kGetChunk;
    const auto& kvs = sliced[i].second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      if (is_add) {
        msg.AddData(kvs.vals);
      }
    }
    send_queue_->Push(std::move(msg));
  }
}


template <typename Val>
void KVTableBox<Val>::Clock() {
  CHECK_NOTNULL(partition_manager_);
  const auto& server_thread_ids = partition_manager_->GetServerThreadIds();
  for (uint32_t server_id : server_thread_ids) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = server_id;
    msg.meta.model_id = model_id_;
    msg.meta.flag = Flag::kClock;
    send_queue_->Push(std::move(msg));
  }
}

template <typename Val>
void KVTableBox<Val>::HandleMsg(Message& msg) {
  CHECK_EQ(msg.data.size(), 2);
  KVPairs<Val> kvs;
  kvs.keys = msg.data[0];
  kvs.vals = msg.data[1];
  recv_kvs_.push_back(kvs);
}

template <typename Val>
template <typename C>
void KVTableBox<Val>::HandleFinish(const third_party::SArray<Key>& keys, C* vals) {
  size_t total_key = 0, total_val = 0;
  for (const auto& s : recv_kvs_) {
    third_party::Range range = third_party::FindRange(keys, s.keys.front(), s.keys.back() + 1);
    CHECK_EQ(range.size(), s.keys.size()) << "unmatched keys size from one server";
    total_key += s.keys.size();
    total_val += s.vals.size();
  }
  CHECK_EQ(total_key, keys.size()) << "lost some servers?";
  std::sort(recv_kvs_.begin(), recv_kvs_.end(),
            [](const KVPairs<Val>& a, const KVPairs<Val>& b) { return a.keys.front() < b.keys.front(); });
  CHECK_NOTNULL(vals);
  vals->resize(total_val);
  Val* p_vals = vals->data();
  for (const auto& s : recv_kvs_) {
    memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
    p_vals += s.vals.size();
  }
  recv_kvs_.clear();
}

template <typename Val>
template <typename C>
void KVTableBox<Val>::HandleChunkFinish(const third_party::SArray<Key>& keys, std::vector<C*>& vals) {
  size_t total_key = 0, total_val = 0;
  for (const auto& s : recv_kvs_) {
    third_party::Range range = third_party::FindRange(keys, s.keys.front(), s.keys.back() + 1);
    CHECK_EQ(range.size(), s.keys.size()) << "unmatched keys size from one server";
    total_key += s.keys.size();
    total_val += s.vals.size();
  }
  size_t chunk_size = total_val / total_key;
  CHECK_EQ(total_key, keys.size()) << "lost some servers?";
  int idx = 0;
  for (const auto& s : recv_kvs_) {
    int start = 0;
    for (int i = 0; i < s.keys.size(); ++ i) {
      vals[idx]->resize(chunk_size);
      memcpy(vals[idx]->data(), s.vals.data()+start, chunk_size*sizeof(Val));
      start += chunk_size;
      idx += 1;
    }
  }
  recv_kvs_.clear();
}


}  // namespace flexps

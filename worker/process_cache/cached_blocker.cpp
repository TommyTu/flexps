#include "app_blocker.hpp"

#include "glog/logging.h"

namespace flexps {

void CachedBlocker::UpdateProcessCache(Message& msg, uint32_t model_id){
  KVPairs<Val> kvs;
  kvs.keys = msg.data[0];
  kvs.vals = msg.data[1];
  int clock = msg.version;
  for (auto chunk_id: kvs.keys){
    std::vector<int> app_thread_id;
    chunk_requst_mgr_.InformReply(model_id, chunk_id, clock, &app_thread_id);
  }
  ps_cache_.insert(kvs.keys, kvs.vals);
}


void CachedBlocker::RegisterSenderqueue(ThreadsafeQueue<Message>* const send_queue) {
  send_queue_ = send_queue;
}

void CachedBlocker::AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) {
  if (msg.flag == Flag::kGetReply) {
    bool recv_finish = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      recv_finish =
          tracker_[app_thread_id][model_id].first == tracker_[app_thread_id][model_id].second + 1 ? true : false;
    }
    UpdateProcessCache(msg, model_id);
    recv_handle_[app_thread_id][model_id](msg);
    if (recv_finish) {
      recv_finish_handle_[app_thread_id][model_id]();
    }
    {
      std::lock_guard<std::mutex> lk(mu_);
      tracker_[app_thread_id][model_id].second += 1;
      if (recv_finish) {
        cond_.notify_all();
      }
    }
  }
  else if (msg.flag == Flag::kGetChunkReply)
  {
    bool recv_finish = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      recv_finish =
          tracker_[app_thread_id][model_id].first == tracker_[app_thread_id][model_id].second + 1 ? true : false;
    }
    UpdateProcessCache(msg, model_id);
    recv_handle_[app_thread_id][model_id](msg);
    
    if (recv_finish) {
      recv_finish_handle_[app_thread_id][model_id]();
    }
    {
      std::lock_guard<std::mutex> lk(mu_);
      tracker_[app_thread_id][model_id].second += 1;
      if (recv_finish) {
        cond_.notify_all();
      }
    }
  }
  else if (msg.flag == Flag::kGetChunk) {
    KVPairs<Val> kvs;
    kvs.keys = msg.data[0];
    // TODO compute the clock in workerside
    int min_clock = ...;
    std::vector<size_t> chunks_not_in_cache = ps_cache_.FindChunkToUpdate(kvs.keys, min_clock);
    ChunkRequestInfo chunk_request;
    chunk_request.app_thread_id = app_thread_id;
    chunk_request.clock = min_clock;
    chunk_request.sent = false;
    std::vector<size_t> chunks_to_request;
    for (auto chunk_id : chunks_not_in_cache) {
      bool send = chunk_requst_mgr_.AddChunkRequest(chunk_request, model_id, chunk_id);
      if (send)
        chunks_to_request.push_back(chunk_id);
    }
    Message msg_to_send;
    msg_to_send.meta.sender = msg.meta.sender;
    msg_to_send.meta.recver = msg.meta.recver;
    msg_to_send.meta.model_id = msg.meta.model_id;
    msg_to_send.meta.flag = msg.meta.flag;
    msg_to_send.AddData(chunks_to_request);
    send_queue_.push(msg_to_send);
  }
  // just send message to server as normal
  else {
    send_queue_.push(msg);
  }
}

}  // namespace flexps

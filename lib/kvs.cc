#include "cloudlab/kvs.hh"

#include "rocksdb/db.h"

namespace cloudlab {

auto KVS::open() -> bool {
  rocksdb::Options options;
  options.create_if_missing = true;
  return rocksdb::DB::Open(options, path.string(), &db).ok();
}

auto KVS::get(const std::string& key, std::string& result) -> bool {
  std::shared_lock<std::shared_timed_mutex> lck(mtx);
  if (!kvs_open) kvs_open = open();
  return db && db->Get(rocksdb::ReadOptions(), key, &result).ok();
}

auto KVS::get_all(std::vector<std::pair<std::string, std::string>>& buffer)
    -> bool {
  std::shared_lock<std::shared_timed_mutex> lck(mtx);
  if (!kvs_open) kvs_open = open();

  auto* it = db->NewIterator(rocksdb::ReadOptions());
  it->SeekToFirst();

  while (it->Valid()) {
    buffer.emplace_back(it->key().data(), it->value().data());
    it->Next();
  }

  return true;
}

auto KVS::put(const std::string& key, const std::string& value) -> bool {
  std::lock_guard<std::shared_timed_mutex> lck(mtx);
  if (!kvs_open) kvs_open = open();
  return db && db->Put(rocksdb::WriteOptions(), key, value).ok();
}

auto KVS::remove(const std::string& key) -> bool {
  std::lock_guard<std::shared_timed_mutex> lck(mtx);
  if (!kvs_open) kvs_open = open();
  return db && db->Delete(rocksdb::WriteOptions(), key).ok();
}

auto KVS::clear() -> bool {
  std::lock_guard<std::shared_timed_mutex> lck(mtx);
  return rocksdb::DestroyDB(path.string(), {}).ok();
}

}  // namespace cloudlab
#ifndef CLOUDLAB_KVS_HH
#define CLOUDLAB_KVS_HH

#include <filesystem>
#include <shared_mutex>
#include <vector>

namespace rocksdb {
class DB;
}

namespace cloudlab {

/**
 * The key-value store. We use rocksdb for the actual key-value operations.
 */
class KVS {
 public:
  KVS(const std::string& path, bool open = false)
      : path{std::filesystem::path(path)}, kvs_open{open} {
    if (open) kvs_open = this->open();
  }

  auto open() -> bool;

  auto get(const std::string& key, std::string& result) -> bool;

  auto get_all(std::vector<std::pair<std::string, std::string>>& buffer)
      -> bool;

  auto put(const std::string& key, const std::string& value) -> bool;

  auto remove(const std::string& key) -> bool;

  auto clear() -> bool;

 private:
  std::filesystem::path path;
  rocksdb::DB* db{};
  bool kvs_open;

  // we use a readers-writer lock s.t. multiple threads may read at the same
  // time while only one thread may modify data in the KVS
  std::shared_timed_mutex mtx{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_KVS_HH

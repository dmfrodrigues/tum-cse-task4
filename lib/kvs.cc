#include "cloudlab/kvs.hh"

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include <iostream>
#include <fmt/core.h>

namespace cloudlab {

KVS::KVS(const std::string& _path) : path{std::filesystem::path(_path)} {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::TransactionDB* _db;
  rocksdb::TransactionDBOptions txOptions{};
  txOptions.default_lock_timeout = 0;
  txOptions.transaction_lock_timeout = 0;
  bool ok =
      rocksdb::TransactionDB::Open(options, txOptions, path.string(), &_db)
          .ok();
  assert(ok);
  db = std::unique_ptr<rocksdb::TransactionDB>(_db);
}
KVS::~KVS() = default;

auto KVS::get(const std::string& key, std::string& result) -> bool {
  std::shared_lock<std::shared_timed_mutex> lck(mtx);
  assert(db);
  return db->Get(rocksdb::ReadOptions(), key, &result).ok();
}

auto KVS::get_all(std::vector<std::pair<std::string, std::string>>& buffer)
    -> bool {
  std::shared_lock<std::shared_timed_mutex> lck(mtx);
  assert(db);

  auto* it = db->NewIterator(rocksdb::ReadOptions());
  it->SeekToFirst();
  while (it->Valid()) {
    buffer.emplace_back(it->key().ToString(), it->value().ToString());
    it->Next();
  }

  return true;
}

auto KVS::put(const std::string& key, const std::string& value) -> bool {
  std::lock_guard<std::shared_timed_mutex> lck(mtx);
  assert(db);
  return db->Put(rocksdb::WriteOptions(), key, value).ok();
}

auto KVS::remove(const std::string& key) -> bool {
  std::lock_guard<std::shared_timed_mutex> lck(mtx);
  assert(db);
  return db->Delete(rocksdb::WriteOptions(), key).ok();
}

auto KVS::clear() -> bool {
  std::lock_guard<std::shared_timed_mutex> lck(mtx);
  assert(db);
  return rocksdb::DestroyDB(path.string(), {}).ok();
}

auto KVS::tx_begin(const std::string& txId) -> std::tuple<bool, std::string> {
  if(transactions.count(txId)) return {false, "ERROR"};
  rocksdb::WriteOptions options;
  rocksdb::TransactionOptions txOptions{};
  auto tx = db->BeginTransaction(rocksdb::WriteOptions(), txOptions);
  if(!tx) return {false, "ERROR"};
  transactions[txId] = tx;
  return {true, "OK"};
}

auto KVS::tx_commit(const std::string& txId) -> std::tuple<bool, std::string> {
  if(!transactions.count(txId)) return {false, "ERROR"};
  std::tuple<bool, std::string> ret = {true, "OK"};
  if(!transactions.at(txId)->Commit().ok())
    ret = {false, "ERROR"};
  delete transactions[txId];
  transactions.erase(txId);
  return ret;
}
auto KVS::tx_abort(const std::string& txId) -> std::tuple<bool, std::string> {
  if(!transactions.count(txId)) return {false, "ERROR"};
  std::tuple<bool, std::string> ret = {true, "OK"};
  if(!transactions.at(txId)->Rollback().ok())
    ret = {false, "ERROR"};
  delete transactions.at(txId);
  transactions.erase(txId);
  return ret;
}
auto KVS::tx_get(const std::string& txId, const std::string& key,
                 std::string& result) -> std::tuple<bool, std::string> {
  if(!transactions.count(txId)){
    std::cerr << "GET: Transaction " << txId << ", transaction object is gone" << std::endl;
    return {false, "ERROR"};
  }
  std::tuple<bool, std::string> ret = {true, "OK"};
  auto transaction = transactions.at(txId);
  rocksdb::Status status = transaction->GetForUpdate(rocksdb::ReadOptions(), key, &result);
  if(!status.ok()){
    if(status == rocksdb::Status::NotFound()){
      result = "ERROR";
      return {true, "OK"};
    }
    std::cerr << "Status is not ok nor NotFound, but it is: " << status.ToString() << std::endl;
    ret = {false, "ERROR"};
    transaction->Rollback();
    delete transaction;
    transactions.erase(txId);
  }
  return ret;
}
auto KVS::tx_put(const std::string& txId, const std::string& key,
                 const std::string& value) -> std::tuple<bool, std::string> {
  if(!transactions.count(txId)) return {false, "ERROR"};
  std::tuple<bool, std::string> ret = {true, "OK"};
  auto transaction = transactions.at(txId);
  if(!transaction->Put(key, value).ok()){
    ret = {false, "ERROR"};
    transaction->Rollback();
    delete transaction;
    transactions.erase(txId);
  }
  return ret;
}
auto KVS::tx_del(const std::string& txId, const std::string& key)
    -> std::tuple<bool, std::string> {
  if(!transactions.count(txId)) return {false, "ERROR"};
  std::tuple<bool, std::string> ret = {true, "OK"};
  auto transaction = transactions.at(txId);
  if(!transaction->Delete(key).ok()){
    ret = {false, "ERROR"};
    transaction->Rollback();
    delete transaction;
    transactions.erase(txId);
  }
  return ret;
}

}  // namespace cloudlab

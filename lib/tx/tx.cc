#include "cloudlab/tx/tx.hh"
#include "cloudlab/message/message_helper.hh"
#include "cloudlab/network/connection.hh"
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <fmt/core.h>
#include <list>

using namespace std;

namespace cloudlab {

auto TXManager::GetTransactionPairs(const MessageHelper& msg,
                                    MessageHelper& resHelper) -> PairVec {
  // TODO(you)
  return {};
}

auto TXManager::HandleOpCoordinator(const cloud::CloudMessage_Operation& op,
                                    const cloud::CloudMessage_Operation& peerOp,
                                    const cloud::CloudMessage& request,
                                    cloud::CloudMessage& response) -> void {
  // TODO(you)
}

auto TXManager::HandleOpParticipant(const cloud::CloudMessage_Operation& op,
                                    const cloud::CloudMessage& request,
                                    cloud::CloudMessage& response) -> void {
  // TODO(you)
}

auto TXManager::HandleMessage(const cloud::CloudMessage& request,
                              cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleMessage\n";

  assert(request.type() == cloud::CloudMessage_Type_REQUEST);
  switch (request.operation()) {
    case cloud::CloudMessage_Operation_TX_CLT_BEGIN:
      assert(IsCoordinator());
      HandleBeginCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_COMMIT:
      assert(IsCoordinator());
      HandleCommitCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_ABORT:
      assert(IsCoordinator());
      HandleAbortCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_GET:
      assert(IsCoordinator());
      HandleGetCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_PUT:
      assert(IsCoordinator());
      HandlePutCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_DELETE:
      assert(IsCoordinator());
      HandleDeleteCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_BEGIN:
      assert(!IsCoordinator());
      HandleBeginParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_COMMIT:
      assert(!IsCoordinator());
      HandleCommitParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_ABORT:
      assert(!IsCoordinator());
      HandleAbortParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_GET:
      assert(!IsCoordinator());
      HandleGetParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_PUT:
      assert(!IsCoordinator());
      HandlePutParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_DELETE:
      assert(!IsCoordinator());
      HandleDeleteParticipant(request, response);
      break;
    default:
      std::cout << "TXManager::HandleMessage default" << std::endl;
      response.set_type(cloud::CloudMessage_Type_RESPONSE);
      response.set_operation(request.operation());
      response.set_success(false);
      response.set_message("TX Operation not supported");
      assert(false);
      break;
  }
}

auto TXManager::HandleBeginCoordinator(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleBeginCoordinator\n";

  const string tx_id = request.tx_id();

  assert(transactionKeysMap[tx_id].size() == 0);

  map<SocketAddress, list<string>> keysPerPeer;
  for(const auto &kvp: request.kvp()){
    SocketAddress peerAddress = routing->find_peer(kvp.key()).value();
    keysPerPeer[peerAddress].push_back(kvp.key());
    transactionKeysMap[tx_id].insert(kvp.key());
  }
  bool success = true;
  for(const auto &p: keysPerPeer){
    const SocketAddress &peerAddress = p.first;
    
    cloud::CloudMessage msg{};
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_TX_BEGIN);
    msg.set_tx_id(tx_id);

    for(const string &key: p.second){
      auto *tmp = msg.add_kvp();
      tmp->set_key(key);
    }

    Connection con{peerAddress};
    con.send(msg);

    cloud::CloudMessage response{};
    con.receive(response);

    success &= response.success();
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleCommitCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleCommitCoordinator\n";

  const string tx_id = request.tx_id();

  bool success = true;

  if(transactionKeysMap.at(tx_id).size() == 0){
    success = false;
  } else {
    set<SocketAddress> peers;
    for(const string &key: transactionKeysMap.at(tx_id)){
      peers.insert(routing->find_peer(key).value());
    }
    for(const SocketAddress &peerAddress: peers){    
      cloud::CloudMessage msg{};
      msg.set_type(cloud::CloudMessage_Type_REQUEST);
      msg.set_operation(cloud::CloudMessage_Operation_TX_COMMIT);
      msg.set_tx_id(tx_id);

      Connection con{peerAddress};
      con.send(msg);

      cloud::CloudMessage response{};
      con.receive(response);

      success &= response.success();
    }
  }

  if(!success) cerr << "COMMIT FAILED" << endl;

  transactionKeysMap.erase(tx_id);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleAbortCoordinator(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleAbortCoordinator\n";

  const string tx_id = request.tx_id();

  bool success = true;

  if(transactionKeysMap.at(tx_id).size() == 0){
    success = false;
  } else {

    set<SocketAddress> peers;
    for(const string &key: transactionKeysMap.at(tx_id)){
      peers.insert(routing->find_peer(key).value());
    }
    
    for(const SocketAddress &peerAddress: peers){    
      cloud::CloudMessage msg{};
      msg.set_type(cloud::CloudMessage_Type_REQUEST);
      msg.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
      msg.set_tx_id(tx_id);

      Connection con{peerAddress};
      con.send(msg);

      cloud::CloudMessage response{};
      con.receive(response);

      success &= response.success();
    }
  }

  if(!success) cerr << "ABORT FAILED" << endl;

  transactionKeysMap.erase(tx_id);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleGetCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleGetCoordinator\n";

  const string tx_id = request.tx_id();

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(true);
  response.set_message("OK");

  for(const auto &kvp: request.kvp()){
    const string &key = kvp.key();
    SocketAddress peerAddress = routing->find_peer(key).value();

    cloud::CloudMessage msg{};
    msg.set_type(request.type());
    msg.set_operation(cloud::CloudMessage_Operation_TX_GET);
    msg.set_tx_id(tx_id);
    msg.add_kvp()->set_key(key);

    Connection con{peerAddress};
    con.send(msg);

    cloud::CloudMessage resp{};
    con.receive(resp);

    if(!resp.success()){
      auto *tmp = response.add_kvp();
      tmp->set_key(key);
      tmp->set_value("0");

      cerr << "GET: Transaction " << tx_id << " (GET " << key << ") is set to ZERO!" << endl;

      transactionKeysMap.at(tx_id).clear();
    } else {
      for(const auto &kvp2: resp.kvp()){
        auto *tmp = response.add_kvp();
        tmp->set_key(kvp2.key());
        tmp->set_value(kvp2.value());
      }
    }
  }
}
auto TXManager::HandlePutCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandlePutCoordinator\n";

  const string tx_id = request.tx_id();

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(true);
  response.set_message("OK");

  for(const auto &kvp: request.kvp()){
    const string &key = kvp.key();
    SocketAddress peerAddress = routing->find_peer(key).value();

    const string &value = kvp.value();

    cloud::CloudMessage msg{};
    msg.set_type(request.type());
    msg.set_operation(cloud::CloudMessage_Operation_TX_PUT);
    msg.set_tx_id(tx_id);
    auto tmp = msg.add_kvp();
    tmp->set_key(key);
    tmp->set_value(value);

    Connection con{peerAddress};
    con.send(msg);

    cloud::CloudMessage resp{};
    con.receive(resp);

    if(!resp.success())
      transactionKeysMap.at(tx_id).clear();

    for(const auto &kvp2: resp.kvp()){
      auto tmp = response.add_kvp();
      tmp->set_key(kvp2.key());
      tmp->set_value("OK");
    }
  }
}
auto TXManager::HandleDeleteCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleDeleteCoordinator\n";

  const string tx_id = request.tx_id();

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(true);
  response.set_message("OK");

  for(const auto &kvp: request.kvp()){
    const string &key = kvp.key();
    SocketAddress peerAddress = routing->find_peer(key).value();

    cloud::CloudMessage msg{};
    msg.set_type(request.type());
    msg.set_operation(cloud::CloudMessage_Operation_TX_DELETE);
    msg.set_tx_id(tx_id);
    auto tmp = msg.add_kvp();
    tmp->set_key(key);

    Connection con{peerAddress};
    con.send(msg);

    cloud::CloudMessage resp{};
    con.receive(resp);

    if(!resp.success())
      transactionKeysMap.at(tx_id).clear();

    for(const auto &kvp2: resp.kvp()){
      auto tmp = response.add_kvp();
      tmp->set_key(kvp2.key());
      tmp->set_value("OK");
    }
  }
}
auto TXManager::HandleBeginParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleBeginParticipant\n";

  const string tx_id = request.tx_id();

  assert(transactionKeysMap.count(tx_id) == 0);

  set<uint32_t> selectedPartitions;
  for(const auto &p: request.kvp()){
    const string key = p.key();
    transactionKeysMap[tx_id].insert(key);

    selectedPartitions.insert(routing->get_partition(key));
  }
  bool success = true;
  for(const int &partitionId: selectedPartitions){
    KVS &kvs = *partitions->at(partitionId).get();
    auto ret = kvs.tx_begin(tx_id);
    success &= get<0>(ret);
  }

  if(!success){
    transactionKeysMap.at(tx_id).clear();

    cerr << "BEGIN: Transaction " << tx_id << " failed" << endl;
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleCommitParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleCommitParticipant\n";

  const string tx_id = request.tx_id();

  if(transactionKeysMap.count(tx_id) == 0){
    response.set_type(cloud::CloudMessage_Type_RESPONSE);
    response.set_operation(request.operation());
    response.set_success(false);
    response.set_message("ERROR");
    return;
  }
  
  bool success = true;

  if(transactionKeysMap.count(tx_id) == 0){
    success = false;
  } else {
    set<uint32_t> selectedPartitions;
    for(const string &key: transactionKeysMap.at(tx_id)){
      selectedPartitions.insert(routing->get_partition(key));
    }
    
    for(const int &partitionId: selectedPartitions){
      KVS &kvs = *partitions->at(partitionId).get();
      auto ret = kvs.tx_commit(tx_id);
      success &= get<0>(ret);
    }
  }

  transactionKeysMap.erase(tx_id);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleAbortParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleAbortParticipant\n";

  const string tx_id = request.tx_id();

  bool success = true;

  if(transactionKeysMap.count(tx_id) == 0){
    success = false;
  } else {
    set<uint32_t> selectedPartitions;
    for(const string &key: transactionKeysMap.at(tx_id)){
      selectedPartitions.insert(routing->get_partition(key));
    }
    for(const int &partitionId: selectedPartitions){
      KVS &kvs = *partitions->at(partitionId).get();
      auto ret = kvs.tx_abort(tx_id);
      success &= get<0>(ret);
    }
  }

  transactionKeysMap.erase(tx_id);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleGetParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleGetParticipant\n";

  const string tx_id = request.tx_id();

  if(transactionKeysMap.at(tx_id).size() == 0){
    response.set_type(cloud::CloudMessage_Type_RESPONSE);
    response.set_operation(request.operation());
    response.set_success(false);
    response.set_message("ERROR");
    return;
  }

  bool success = true;

  for(const auto &p: request.kvp()){
    const string key = p.key();

    KVS &kvs = *partitions->at(routing->get_partition(key));

    string value;
    auto ret = kvs.tx_get(tx_id, key, value);
    if(!get<0>(ret)){
      success = false;
      value = "ERROR";
    }
    auto response_kvp = response.add_kvp();
    response_kvp->set_key(key);
    response_kvp->set_value(value);
  }

  if(!success){
    for(const string &key: transactionKeysMap.at(tx_id))
      partitions->at(routing->get_partition(key))->tx_abort(tx_id);
    transactionKeysMap.at(tx_id).clear();

    cerr << "GET: Transaction " << tx_id << " failed" << endl;
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandlePutParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandlePutParticipant\n";

  const string tx_id = request.tx_id();

  if(transactionKeysMap.at(tx_id).size() == 0){
    response.set_type(cloud::CloudMessage_Type_RESPONSE);
    response.set_operation(request.operation());
    response.set_success(false);
    response.set_message("ERROR");
    return;
  }

  bool success = true;

  for(const auto &p: request.kvp()){
    const string key = p.key();
    const string value = p.value();

    KVS &kvs = *partitions->at(routing->get_partition(key));

    auto ret = kvs.tx_put(tx_id, key, value);
    auto response_kvp = response.add_kvp();
    response_kvp->set_key(key);
    if(!get<0>(ret)){
      success = false;
    }
    response_kvp->set_value(get<1>(ret));
  }

  if(!success){
    for(const string &key: transactionKeysMap.at(tx_id))
      partitions->at(routing->get_partition(key))->tx_abort(tx_id);
    transactionKeysMap.at(tx_id).clear();

    cerr << "PUT: Transaction " << tx_id << " failed" << endl;
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleDeleteParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleDeleteParticipant\n";

  const string tx_id = request.tx_id();

  if(transactionKeysMap.at(tx_id).size() == 0){
    response.set_type(cloud::CloudMessage_Type_RESPONSE);
    response.set_operation(request.operation());
    response.set_success(false);
    response.set_message("ERROR");
    return;
  }

  bool success = true;

  for(const auto &p: request.kvp()){
    const string key = p.key();
    const string value = p.value();

    KVS &kvs = *partitions->at(routing->get_partition(key));

    auto ret = kvs.tx_del(tx_id, key);
    auto response_kvp = response.add_kvp();
    response_kvp->set_key(key);
    if(!get<0>(ret)){
      success = false;
    }
    response_kvp->set_value(get<1>(ret));
  }

  if(!success){
    for(const string &key: transactionKeysMap.at(tx_id))
      partitions->at(routing->get_partition(key))->tx_abort(tx_id);
    transactionKeysMap.at(tx_id).clear();

    cerr << "DELETE: Transaction " << tx_id << " failed" << endl;
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}

}  // namespace cloudlab

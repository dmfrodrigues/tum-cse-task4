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

  // Send BEGIN TX to all involved peers
  map<SocketAddress, list<string>> keysPerPeer;
  for(const auto &kvp: request.kvp()){
    SocketAddress peerAddress = routing->find_peer(kvp.key()).value();
    keysPerPeer[peerAddress].push_back(kvp.key());
  }
  for(const auto &p: keysPerPeer){
    const SocketAddress &peerAddress = p.first;
    
    cloud::CloudMessage msg{};
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_TX_BEGIN);

    for(const string &key: p.second){
      auto *tmp = msg.add_kvp();
      tmp->set_key(key);
    }

    Connection con{peerAddress};
    con.send(msg);

    cloud::CloudMessage response{};
    con.receive(response);

    assert(response.success());
    assert(response.message() == "OK");
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(true);
  response.set_message("OK");
}
auto TXManager::HandleCommitCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleCommitCoordinator\n";

  const string tx_id = request.tx_id();
  KVS &kvs = *(*partitions)[0];

  auto ret = kvs.tx_commit(tx_id);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(get<0>(ret));
  response.set_message(get<1>(ret));
}
auto TXManager::HandleAbortCoordinator(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleAbortCoordinator\n";

  const string tx_id = request.tx_id();
  KVS &kvs = *(*partitions)[0];

  auto ret = kvs.tx_abort(tx_id);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(get<0>(ret));
  response.set_message(get<1>(ret));
}
auto TXManager::HandleGetCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleGetCoordinator\n";

  const string tx_id = request.tx_id();
  KVS &kvs = *(*partitions)[0];

  bool success = true;

  for(const auto &p: request.kvp()){
    const string key = p.key();
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

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandlePutCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandlePutCoordinator\n";

  const string tx_id = request.tx_id();
  KVS &kvs = *(*partitions)[0];

  bool success = true;

  for(const auto &p: request.kvp()){
    const string key = p.key();
    const string value = p.value();
    string result = "OK";
    auto ret = kvs.tx_put(tx_id, key, value);
    if(!get<0>(ret)){
      success = false;
      result = "ERROR";
    }
    auto response_kvp = response.add_kvp();
    response_kvp->set_key(key);
    response_kvp->set_value(result);
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleDeleteCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleDeleteCoordinator\n";

  const string tx_id = request.tx_id();
  KVS &kvs = *(*partitions)[0];

  bool success = true;

  for(const auto &p: request.kvp()){
    const string key = p.key();
    string result = "OK";
    auto ret = kvs.tx_del(tx_id, key);
    if(!get<0>(ret)){
      success = false;
      result = "ERROR";
    }
    auto response_kvp = response.add_kvp();
    response_kvp->set_key(key);
    response_kvp->set_value(result);
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(success);
  response.set_message(success ? "OK" : "ERROR");
}
auto TXManager::HandleBeginParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleBeginParticipant\n";

  const string tx_id = request.tx_id();
  KVS &kvs = *(*partitions)[0];

  auto ret = kvs.tx_begin(tx_id);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());
  response.set_success(get<0>(ret));
  response.set_message(get<1>(ret));
}
auto TXManager::HandleCommitParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleCommitParticipant\n";
}
auto TXManager::HandleAbortParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleAbortParticipant\n";
}
auto TXManager::HandleGetParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleGetParticipant\n";
}
auto TXManager::HandlePutParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandlePutParticipant\n";
}
auto TXManager::HandleDeleteParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleDeleteParticipant\n";
}

}  // namespace cloudlab

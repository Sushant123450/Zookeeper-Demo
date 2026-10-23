#include <chrono>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "kv_store.h"
#include "node.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

KVStore kv;

class ZooKeeperClient
{
private:
    std::string target;
    std::unique_ptr<cluster::CoordinatorService::Stub> stub;

public:
    explicit ZooKeeperClient(const std::string &coordinatorAddress)
        : target(coordinatorAddress)
    {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        stub = cluster::CoordinatorService::NewStub(channel);
    }

    bool registerNode(const std::string &nodeId, const std::string &nodeAddress)
    {
        cluster::NodeRegistrationRequest request;
        request.set_node_id(nodeId);
        request.set_node_address(nodeAddress);

        cluster::NodeRegistrationReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

        auto status = stub->RegisterNode(&context, request, &reply);
        return status.ok();
    }

    cluster::LeaderQueryReply getLeader()
    {
        cluster::LeaderQueryRequest request;
        cluster::LeaderQueryReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

        stub->GetLeader(&context, request, &reply);
        return reply;
    }
};

class NodeServiceImpl final : public cluster::NodeService::Service
{
private:
    std::string nodeId;
    std::string nodeAddress;
    std::vector<std::string> peerNodes;
    ZooKeeperClient coordinator;

public:
    NodeServiceImpl(const std::string &id,
                    const std::string &address,
                    const std::string &coordinatorAddress,
                    const std::vector<std::string> &peers)
        : nodeId(id), nodeAddress(address), peerNodes(peers), coordinator(coordinatorAddress)
    {
        coordinator.registerNode(nodeId, nodeAddress);
    }

    cluster::LeaderQueryReply fetchLeader()
    {
        return coordinator.getLeader();
    }

    bool isLeaderNode(const cluster::LeaderQueryReply &leaderInfo) const
    {
        return !leaderInfo.leader_node_id().empty() &&
               leaderInfo.leader_node_id() == nodeId;
    }

    void replicateToPeer(const std::string &target, const std::string &msg)
    {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        auto stub = cluster::NodeService::NewStub(channel);

        cluster::MessageRequest request;
        request.set_from("peer");
        request.set_message(msg);

        cluster::MessageReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        stub->SendMessage(&context, request, &reply);
    }

    Status SendMessage(ServerContext *context,
                       const cluster::MessageRequest *request,
                       cluster::MessageReply *reply) override
    {
        (void)context;

        std::string msg = request->message();
        std::istringstream iss(msg);
        std::string cmd, key;
        iss >> cmd >> key;

        if (cmd == "PUT")
        {
            std::string value;
            std::getline(iss, value);
            if (!value.empty() && value[0] == ' ')
            {
                value.erase(0, 1);
            }

            if (key.empty() || value.empty())
            {
                reply->set_status("ERROR: PUT <key> <value>");
                return Status::OK;
            }

            if (request->from() == "peer")
            {
                kv.put(key, value);
                reply->set_status("REPLICATED");
                return Status::OK;
            }

            cluster::LeaderQueryReply leaderInfo = fetchLeader();
            if (!isLeaderNode(leaderInfo))
            {
                std::string leaderAddress = leaderInfo.leader_node_address();
                if (leaderAddress.empty())
                {
                    leaderAddress = "unknown";
                }
                reply->set_status("ERROR: Not Leader. Leader is " + leaderAddress);
                return Status::OK;
            }

            kv.put(key, value);
            for (const auto &peer : peerNodes)
            {
                replicateToPeer(peer, msg);
            }

            reply->set_status("STORED");
            return Status::OK;
        }

        if (cmd == "GET")
        {
            if (key.empty())
            {
                reply->set_status("ERROR: GET <key>");
                return Status::OK;
            }

            auto value = kv.get(key);
            if (!value)
            {
                reply->set_status("ERROR: NOT FOUND");
            }
            else
            {
                reply->set_status("VALUE: " + *value);
            }
            return Status::OK;
        }

        if (cmd == "WHO_IS_LEADER")
        {
            cluster::LeaderQueryReply leaderInfo = fetchLeader();
            reply->set_status("LEADER " + leaderInfo.leader_node_address());
            return Status::OK;
        }

        reply->set_status("ERROR: UNKNOWN COMMAND");
        return Status::OK;
    }
};

void RunServer(const std::string &port,
               const std::string &coordinatorAddress,
               const std::vector<std::string> &peers)
{
    const std::string bindAddress = "0.0.0.0:" + port;
    const std::string nodeAddress = "localhost:" + port;

    NodeServiceImpl service(port, nodeAddress, coordinatorAddress, peers);

    ServerBuilder builder;
    builder.AddListeningPort(bindAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server)
    {
        std::cerr << "Failed to start node on " << bindAddress << std::endl;
        return;
    }

    std::cout << "[Node " << port << "] running on " << bindAddress
              << " with ZooKeeper " << coordinatorAddress << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        std::cout << "Usage: node <port> <zookeeper-host:port> [peer1 peer2 ...]\n";
        return 1;
    }

    std::vector<std::string> peers;
    for (int i = 3; i < argc; i++)
    {
        peers.push_back(argv[i]);
    }

    RunServer(argv[1], argv[2], peers);
    return 0;
}

#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <grpcpp/grpcpp.h>
#include "node.grpc.pb.h"
#include "kv_store.h"

// Thread-safe global store
KVStore kv;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

enum class NodeState
{
    FOLLOWER,
    CANDIDATE,
    LEADER
};

class NodeServiceImpl final : public cluster::NodeService::Service
{
private:
    std::vector<std::string> peers;
    std::string nodeId;
    NodeState state = NodeState::FOLLOWER;
    int currentTerm = 0;
    std::string votedFor = "";

public:
    NodeServiceImpl(const std::vector<std::string> &p, const std::string &id)
        : peers(p), nodeId(id)
    {
        if (nodeId == "5001")
        {
            state = NodeState::LEADER;
        }
        else
        {
            state = NodeState::FOLLOWER;
        }

        std::cout << "[Node " << nodeId << "] State: "
                  << (state == NodeState::LEADER ? "LEADER" : "FOLLOWER")
                  << std::endl;
    }

public:
    void sendToPeer(const std::string &target, const std::string &msg)
    {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        auto stub = cluster::NodeService::NewStub(channel);

        cluster::MessageRequest request;
        request.set_from("peer");
        request.set_message(msg);

        cluster::MessageReply reply;
        grpc::ClientContext context;

        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

        auto status = stub->SendMessage(&context, request, &reply);

        if (!status.ok())
        {
            std::cout << "[Node " << nodeId << "][WARN] Failed to send to "
                      << target << " (" << status.error_message() << ")\n";
        }
        else
        {
            std::cout << "[Node " << nodeId << "][ACK] from " << target << "\n";
        }
    }

    Status SendMessage(ServerContext *context,
                       const cluster::MessageRequest *request,
                       cluster::MessageReply *reply) override
    {

        std::string msg = request->message();
        std::istringstream iss(msg);
        std::string cmd, key;

        iss >> cmd >> key;

        if (cmd == "PUT")
        {
            std::string value;
            std::getline(iss, value);

            if (!value.empty() && value[0] == ' ')
                value.erase(0, 1);

            if (key.empty() || value.empty())
            {
                reply->set_status("ERROR: PUT <key> <value>");
                return Status::OK;
            }

            if (request->from() == "Client" && state != NodeState::LEADER)
            {
                std::cout << "[Node " << nodeId << "][REJECT] Not leader\n";
                reply->set_status("ERROR: Not Leader. Leader is 5001");
                return Status::OK;
            }

            // ✅ Peer request → only apply, no broadcast
            if (request->from() == "peer")
            {
                kv.put(key, value);

                std::cout << "[Node " << nodeId << "][REPL] from peer -> key: "
                          << key << ", value: " << value << std::endl;

                reply->set_status("REPLICATED");
                return Status::OK;
            }
            kv.put(key, value);

            std::cout << "[Node " << nodeId << "][LOCAL PUT] key: "
                      << key << ", value: " << value << std::endl;

            for (const auto &peer : peers)
            {
                std::cout << "[Node " << nodeId << "][SEND] to " << peer
                          << " -> " << msg << std::endl;
                sendToPeer(peer, msg);
            }

            reply->set_status("STORED");
        }
        else if (cmd == "GET")
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
        }
        else
        {
            reply->set_status("ERROR: UNKNOWN COMMAND");
        }

        std::cout << "[Node " << nodeId << "][REQ] "
                  << request->from() << " -> " << msg << std::endl;
        return Status::OK;
    }
};

void RunServer(std::string port, std::vector<std::string> peers)
{
    std::string address = "0.0.0.0:" + port;

    NodeServiceImpl service(peers, port); // ✅ pass port as nodeId

    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server)
    {
        std::cerr << "Failed to start node on " << address << std::endl;
        return;
    }

    std::cout << "[Node " << port << "] running on " << address << std::endl;

    server->Wait();
}
int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cout << "Usage: node <port> [peer1 peer2 ...]\n";
        return 1;
    }

    std::vector<std::string> peers;

    for (int i = 2; i < argc; i++)
    {
        peers.push_back(argv[i]);
    }

    RunServer(argv[1], peers);
    return 0;
}
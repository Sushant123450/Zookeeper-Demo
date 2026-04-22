#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <grpcpp/grpcpp.h>
#include "node.grpc.pb.h"
#include "kv_store.h"
#include <thread>
#include <chrono>
#include <atomic>

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
    std::atomic<bool> running{true};
    std::atomic<long long> lastHeartbeatTime{0};

public:
    NodeServiceImpl(const std::vector<std::string> &p, const std::string &id)
        : peers(p), nodeId(id)
    {
        lastHeartbeatTime = currentTimeMillis(); // 🔥 important

        startElectionTimer();

        std::cout << "[Node " << nodeId << "] State: "
                  << (state == NodeState::LEADER ? "LEADER" : "FOLLOWER")
                  << std::endl;
    }

    long long currentTimeMillis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    void resetElectionTimer()
    {
        lastHeartbeatTime = currentTimeMillis();
    }

    void startHeartbeat()
    {
        std::thread([this]()
                    {
        while (running)
        {
            if (state == NodeState::LEADER)
            {
                for (const auto& peer : peers)
                {
                    sendHeartbeat(peer);
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        } })
            .detach();
    }

    void sendHeartbeat(const std::string &target)
    {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        auto stub = cluster::NodeService::NewStub(channel);

        cluster::MessageRequest request;
        request.set_from(nodeId);
        request.set_message("HEARTBEAT " + std::to_string(currentTerm));

        cluster::MessageReply reply;
        grpc::ClientContext context;

        auto status = stub->SendMessage(&context, request, &reply);
    }

    bool requestVote(const std::string &target)
    {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        auto stub = cluster::NodeService::NewStub(channel);

        cluster::MessageRequest request;
        request.set_from(nodeId);
        request.set_message("VOTE " + std::to_string(currentTerm));

        cluster::MessageReply reply;
        grpc::ClientContext context;

        auto status = stub->SendMessage(&context, request, &reply);

        return status.ok() && reply.status() == "VOTE_GRANTED";
    }

    void startElection()
    {
        state = NodeState::CANDIDATE;
        currentTerm++;
        votedFor = nodeId;

        int votes = 1; // vote for self

        std::cout << "[Node " << nodeId << "] Starting election for term "
                  << currentTerm << std::endl;

        for (const auto &peer : peers)
        {
            if (requestVote(peer))
            {
                votes++;
            }
        }

        if (votes > (peers.size() + 1) / 2)
        {
            state = NodeState::LEADER;
            std::cout << "[Node " << nodeId << "] Became LEADER\n";

            startHeartbeat();
        }
        else
        {
            state = NodeState::FOLLOWER;
            std::cout << "[Node " << nodeId << "] Election lost\n";
        }
    }

    void startElectionTimer()
    {
        std::thread([this]()
                    {
        while (running)
        {
            int timeout = 1500 + (rand() % 1500);
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout));

            long long now = currentTimeMillis();

if (state == NodeState::FOLLOWER &&
    now - lastHeartbeatTime > timeout)
{
    startElection();
}
        } })
            .detach();
    }

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

    Status SendMessage(ServerContext *context, const cluster::MessageRequest *request, cluster::MessageReply *reply) override
    {

        std::string msg = request->message();
        std::istringstream iss(msg);
        std::string cmd, key;

        iss >> cmd >> key;

        if (msg.rfind("HEARTBEAT", 0) == 0)
        {
            std::istringstream hbStream(msg);
            std::string cmd;
            int term;

            hbStream >> cmd >> term;

            if (term >= currentTerm)
            {
                currentTerm = term;
                state = NodeState::FOLLOWER;
                votedFor = "";

                std::cout << "[Node " << nodeId << "] Received heartbeat\n";

                resetElectionTimer(); // 🔥 important
            }

            reply->set_status("OK");
            return Status::OK;
        }

        if (msg.rfind("VOTE", 0) == 0)
        {
            std::istringstream voteStream(msg);
            std::string cmd;
            int term;
            voteStream >> cmd >> term;

            std::string candidateId = request->from();

            if (term < currentTerm)
            {
                reply->set_status("VOTE_DENIED");
                return Status::OK;
            }

            if (term > currentTerm)
            {
                currentTerm = term;
                votedFor = "";
                state = NodeState::FOLLOWER;
            }

            if (votedFor.empty() || votedFor == candidateId)
            {
                votedFor = candidateId;
                reply->set_status("VOTE_GRANTED");

                std::cout << "[Node " << nodeId << "] Voted for "
                          << candidateId << std::endl;
            }
            else
            {
                reply->set_status("VOTE_DENIED");
            }

            return Status::OK;
        }

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
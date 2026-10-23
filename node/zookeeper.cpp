#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "kv_store.h"
#include "node.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

enum class CoordinatorState
{
    FOLLOWER,
    CANDIDATE,
    LEADER
};

const char *ToString(CoordinatorState state)
{
    switch (state)
    {
    case CoordinatorState::FOLLOWER:
        return "FOLLOWER";
    case CoordinatorState::CANDIDATE:
        return "CANDIDATE";
    case CoordinatorState::LEADER:
        return "LEADER";
    }

    return "UNKNOWN";
}

class ZooKeeperServiceImpl final : public cluster::CoordinatorService::Service,
                                   public cluster::NodeService::Service
{
private:
    std::string selfAddress;
    std::vector<std::string> peers;
    std::vector<std::string> allServers;

    KVStore kv;

    std::mutex stateMutex;
    CoordinatorState state = CoordinatorState::FOLLOWER;
    int currentTerm = 0;
    std::string votedFor;
    std::string leaderAddress;

    std::atomic<bool> running{true};
    std::atomic<long long> lastHeartbeatTime{0};
    int electionTimeoutMs = 2500;

public:
    ZooKeeperServiceImpl(const std::string &address,
                         const std::vector<std::string> &peerAddresses)
        : selfAddress(address), peers(peerAddresses)
    {
        allServers.push_back(selfAddress);
        for (const auto &peer : peers)
        {
            allServers.push_back(peer);
        }

        std::mt19937 rng(static_cast<unsigned int>(
            std::chrono::steady_clock::now().time_since_epoch().count() ^
            std::hash<std::string>{}(selfAddress)));
        std::uniform_int_distribution<int> dist(1800, 3200);
        electionTimeoutMs = dist(rng);

        resetElectionTimer();
        startHeartbeatLoop();
        startElectionLoop();
    }

    long long currentTimeMillis() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    void resetElectionTimer()
    {
        lastHeartbeatTime = currentTimeMillis();

        std::mt19937 rng(static_cast<unsigned int>(
            std::chrono::steady_clock::now().time_since_epoch().count() ^
            std::hash<std::string>{}(selfAddress)));

        std::uniform_int_distribution<int> dist(1800, 3200);
        electionTimeoutMs = dist(rng); // 🔥 re-randomize every time
    }

    void startHeartbeatLoop()
    {
        std::thread([this]()
                    {
            while (running)
            {
                bool sendNow = false;
                int termSnapshot = 0;
                {
                    std::lock_guard<std::mutex> lock(stateMutex);
                    sendNow = (state == CoordinatorState::LEADER);
                    termSnapshot = currentTerm;
                }

                if (sendNow)
                {
                    for (const auto &peer : peers)
                    {
                        sendCoordinatorHeartbeat(peer, termSnapshot);
                    }
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            } })
            .detach();
    }

    void sendCoordinatorHeartbeat(const std::string &target, int term)
    {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        auto stub = cluster::CoordinatorService::NewStub(channel);

        cluster::CoordinatorHeartbeatRequest request;
        request.set_coordinator_id(selfAddress);
        request.set_term(term);

        cluster::CoordinatorHeartbeatReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));

        auto status = stub->Heartbeat(&context, request, &reply);
        if (status.ok() && reply.term() > term)
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            currentTerm = reply.term();
            state = CoordinatorState::FOLLOWER;
            votedFor.clear();
            leaderAddress.clear();
            resetElectionTimer();
        }
    }

    bool requestVoteFromPeer(const std::string &target, int term)
    {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        auto stub = cluster::CoordinatorService::NewStub(channel);

        cluster::CoordinatorVoteRequest request;
        request.set_candidate_id(selfAddress);
        request.set_term(term);

        cluster::CoordinatorVoteReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));

        auto status = stub->RequestVote(&context, request, &reply);
        if (!status.ok())
        {
            return false;
        }

        if (reply.term() > term)
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            currentTerm = reply.term();
            state = CoordinatorState::FOLLOWER;
            votedFor.clear();
            leaderAddress.clear();
            resetElectionTimer();
            return false;
        }

        return reply.status() == "VOTE_GRANTED";
    }

    void startElection()
    {
        if (state == CoordinatorState::LEADER)
            return;
        int electionTerm = 0;
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            state = CoordinatorState::CANDIDATE;
            currentTerm++;
            electionTerm = currentTerm;
            votedFor = selfAddress;
            leaderAddress.clear();
        }
        resetElectionTimer();

        int votes = 1;
        for (const auto &peer : peers)
        {
            if (requestVoteFromPeer(peer, electionTerm))
            {
                votes++;
            }
        }

        std::lock_guard<std::mutex> lock(stateMutex);
        if (state != CoordinatorState::CANDIDATE || currentTerm != electionTerm)
        {
            return;
        }

        if (votes > static_cast<int>(peers.size() + 1) / 2)
        {
            state = CoordinatorState::LEADER;
            leaderAddress = selfAddress;
            std::cout << "[ZooKeeper " << selfAddress << "] Became leader for term "
                      << currentTerm << std::endl;
        }
        else
        {
            state = CoordinatorState::FOLLOWER;
        }
    }

    void startElectionLoop()
    {
        std::thread([this]()
                    {
            while (running)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));

                long long elapsed = currentTimeMillis() - lastHeartbeatTime.load();
                bool shouldStartElection = false;
                {
                    std::lock_guard<std::mutex> lock(stateMutex);
                    if (state != CoordinatorState::LEADER && elapsed > electionTimeoutMs)
                    {
                        shouldStartElection = true;
                    }
                }

                if (shouldStartElection)
                {
                    startElection();
                }
            } })
            .detach();
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
        auto status = stub->SendMessage(&context, request, &reply);

        if (!status.ok())
        {
            std::cout << "[ZooKeeper " << selfAddress << "][REPL-FAIL] peer=" << target
                      << " code=" << status.error_code()
                      << " reason=" << status.error_message() << std::endl;
            return;
        }

        if (reply.status() != "REPLICATED")
        {
            std::cout << "[ZooKeeper " << selfAddress << "][REPL-WARN] peer=" << target
                      << " response=" << reply.status() << std::endl;
        }
    }

    Status RegisterNode(ServerContext *context,
                        const cluster::NodeRegistrationRequest *request,
                        cluster::NodeRegistrationReply *reply) override
    {
        (void)context;
        (void)request;
        reply->set_status("NOT_USED");
        return Status::OK;
    }

    Status GetLeader(ServerContext *context,
                     const cluster::LeaderQueryRequest *request,
                     cluster::LeaderQueryReply *reply) override
    {
        (void)context;
        (void)request;

        std::lock_guard<std::mutex> lock(stateMutex);
        reply->set_leader_node_id(leaderAddress);
        reply->set_leader_node_address(leaderAddress);
        if (state == CoordinatorState::LEADER)
        {
            reply->set_coordinator_state("LEADER");
        }
        else if (state == CoordinatorState::CANDIDATE)
        {
            reply->set_coordinator_state("CANDIDATE");
        }
        else
        {
            reply->set_coordinator_state("FOLLOWER");
        }
        return Status::OK;
    }

    Status Heartbeat(ServerContext *context,
                     const cluster::CoordinatorHeartbeatRequest *request,
                     cluster::CoordinatorHeartbeatReply *reply) override
    {
        (void)context;

        std::lock_guard<std::mutex> lock(stateMutex);
        if (request->term() > currentTerm)
        {
            currentTerm = request->term();
            votedFor.clear();
            state = CoordinatorState::FOLLOWER;
            leaderAddress = request->coordinator_id();
        }
        else if (request->term() == currentTerm)
        {
            if (leaderAddress.empty())
            {
                leaderAddress = request->coordinator_id();
            }
            else if (leaderAddress != request->coordinator_id())
            {
                // 🔥 ignore conflicting leader
                return Status::OK;
            }
        }

        reply->set_status("OK");
        reply->set_term(currentTerm);

        resetElectionTimer();
        return Status::OK;
    }

    Status RequestVote(ServerContext *context,
                       const cluster::CoordinatorVoteRequest *request,
                       cluster::CoordinatorVoteReply *reply) override
    {
        (void)context;

        std::lock_guard<std::mutex> lock(stateMutex);
        if (request->term() < currentTerm)
        {
            reply->set_status("VOTE_DENIED");
            reply->set_term(currentTerm);
            return Status::OK;
        }

        if (request->term() > currentTerm)
        {
            currentTerm = request->term();
            state = CoordinatorState::FOLLOWER;
            votedFor.clear();
            leaderAddress.clear();
        }

        if (votedFor.empty())
        {
            votedFor = request->candidate_id();
            reply->set_status("VOTE_GRANTED");
        }
        else
        {
            reply->set_status("VOTE_DENIED");
        }

        reply->set_term(currentTerm);
        return Status::OK;
    }

    Status SendMessage(ServerContext *context,
                       const cluster::MessageRequest *request,
                       cluster::MessageReply *reply) override
    {
        (void)context;

        std::string msg = request->message();
        std::istringstream iss(msg);
        std::string cmd;
        iss >> cmd;

        if (cmd == "CLUSTER")
        {
            std::ostringstream out;
            out << "NODES";
            for (const auto &server : allServers)
            {
                out << " " << server;
            }
            reply->set_status(out.str());
            return Status::OK;
        }

        if (cmd == "PUT")
        {
            std::string key;
            iss >> key;
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

            std::string currentLeader;
            bool amLeader = false;
            CoordinatorState currentState;
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                currentLeader = leaderAddress;
                amLeader = (state == CoordinatorState::LEADER);
                currentState = state;
            }

            if (!amLeader)
            {
                std::string reason;
                if (currentLeader.empty())
                {
                    currentLeader = "unknown";
                    reason = currentState == CoordinatorState::CANDIDATE
                                 ? "leader election is in progress"
                                 : "leader has not been established yet";
                }
                else
                {
                    reason = std::string("this server is in state ") + ToString(currentState);
                }

                std::cout << "[ZooKeeper " << selfAddress << "][REDIRECT] cmd=PUT"
                          << " leader=" << currentLeader
                          << " reason=" << reason << std::endl;

                reply->set_status("ERROR: Not Leader. Leader is " + currentLeader +
                                  ". Reason: " + reason);
                return Status::OK;
            }

            kv.put(key, value);
            for (const auto &peer : peers)
            {
                replicateToPeer(peer, msg);
            }

            reply->set_status("STORED");
            return Status::OK;
        }

        if (cmd == "GET")
        {
            std::string key;
            iss >> key;
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

        reply->set_status("ERROR: UNKNOWN COMMAND");
        return Status::OK;
    }
};

void RunZooKeeper(const std::string &port, const std::vector<std::string> &peers)
{
    const std::string bindAddress = "0.0.0.0:" + port;
    const std::string selfAddress = "localhost:" + port;
    ZooKeeperServiceImpl service(selfAddress, peers);

    ServerBuilder builder;
    builder.AddListeningPort(bindAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(
        static_cast<cluster::CoordinatorService::Service *>(&service));
    builder.RegisterService(
        static_cast<cluster::NodeService::Service *>(&service));

    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server)
    {
        std::cerr << "Failed to start ZooKeeper on " << bindAddress << std::endl;
        return;
    }

    std::cout << "[ZooKeeper " << selfAddress << "] running on " << bindAddress
              << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cout << "Usage: zookeeper <port> [peer1 peer2 ...]\n";
        return 1;
    }

    std::vector<std::string> peers;
    for (int i = 2; i < argc; i++)
    {
        peers.push_back(argv[i]);
    }

    RunZooKeeper(argv[1], peers);
    return 0;
}

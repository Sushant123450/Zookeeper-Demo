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

class NodeServiceImpl final : public cluster::NodeService::Service
{
public:
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

            kv.put(key, value);
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

        std::cout << "[REQ] " << request->from() << " -> " << msg << std::endl;
        return Status::OK;
    }
};

void RunServer(const std::string &port)
{
    std::string address = "0.0.0.0:" + port;
    NodeServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server)
    {
        std::cerr << "Failed to start node on " << address << std::endl;
        return;
    }

    std::cout << "Node running on " << address << std::endl;

    server->Wait();
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cout << "Usage: node <port>\n";
        return 1;
    }
    RunServer(argv[1]);
    return 0;
}

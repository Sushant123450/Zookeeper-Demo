#include <chrono>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "node.grpc.pb.h"

std::unique_ptr<cluster::NodeService::Stub> createStub(const std::string &target)
{
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    return cluster::NodeService::NewStub(channel);
}

std::vector<std::string> fetchClusterNodes(std::unique_ptr<cluster::NodeService::Stub> &stub)
{
    std::vector<std::string> discovered;

    cluster::MessageRequest request;
    request.set_from("Client");
    request.set_message("CLUSTER");

    cluster::MessageReply reply;
    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds(2));

    auto status = stub->SendMessage(&context, request, &reply);

    if (!status.ok())
    {
        std::cout << "[Client] Failed to fetch cluster: code="
                  << status.error_code()
                  << " reason=" << status.error_message() << "\n";
        return discovered;
    }

    std::string resp = reply.status();

    if (resp.rfind("NODES ", 0) == 0)
    {
        std::string nodesStr = resp.substr(6);

        std::istringstream iss(nodesStr);
        std::string node;

        while (iss >> node)
        {
            discovered.push_back(node);
        }
    }

    return discovered;
}

bool sendMessage(std::unique_ptr<cluster::NodeService::Stub> &stub, std::string &target, const std::string &message, const std::vector<std::string> &nodes)
{
    cluster::MessageRequest request;
    request.set_from("Client");
    request.set_message(message);

    cluster::MessageReply reply;
    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds(2));

    auto status = stub->SendMessage(&context, request, &reply);

    if (!status.ok())
    {
        std::cout << "[Client] Node failed: " << target
                  << " code=" << status.error_code()
                  << " reason=" << status.error_message() << "\n";

        // try another node
        for (const auto &node : nodes)
        {
            if (node == target)
                continue;

            std::cout << "[Client] Trying fallback: " << node
                      << " because " << target << " is unavailable\n";

            target = node;
            stub = createStub(target);
            return false; // retry
        }

        std::cout << "[Client] No nodes available\n";
        return true;
    }

    std::string resp = reply.status();

    // 🔥 HANDLE REDIRECT
    if (resp.rfind("ERROR: Not Leader", 0) == 0)
    {
        std::string prefix = "Leader is ";
        auto pos = resp.find(prefix);

        if (pos != std::string::npos)
        {
            std::string leaderAddr = resp.substr(pos + prefix.length());
            std::string redirectReason;
            auto reasonPos = leaderAddr.find(". Reason: ");
            if (reasonPos != std::string::npos)
            {
                redirectReason = leaderAddr.substr(reasonPos + 10);
                leaderAddr = leaderAddr.substr(0, reasonPos);
            }

            // 🔥 CASE 1: Known leader
            if (!leaderAddr.empty() && leaderAddr != "unknown")
            {
                if (leaderAddr != target)
                {
                    std::cout << "[Client] Redirect → " << leaderAddr << "\n";

                    if (!redirectReason.empty())
                    {
                        std::cout << "[Client] Redirect reason=" << redirectReason << "\n";
                    }
                    target = leaderAddr;
                    stub = createStub(target);
                }
                return false; // retry
            }

            // 🔥 CASE 2: Unknown leader
            std::cout << "[Client] Leader unknown → trying other nodes\n";

            if (redirectReason.empty())
            {
                auto fullReasonPos = resp.find(". Reason: ");
                if (fullReasonPos != std::string::npos)
                {
                    redirectReason = resp.substr(fullReasonPos + 10);
                }
            }
            if (!redirectReason.empty())
            {
                std::cout << "[Client] Leader resolution reason=" << redirectReason << "\n";
            }

            for (const auto &node : nodes)
            {
                if (node == target)
                    continue;

                std::cout << "[Client] Trying: " << node
                          << " because current node could not provide a usable leader\n";

                target = node;
                stub = createStub(target);
                return false; // retry
            }
        }

        std::cout << "[Client] Could not resolve leader from response: "
                  << resp << "\n";
        return true;
    }

    std::cout << "Response: " << resp << std::endl;
    return true;
}

void printHelp()
{
    std::cout << "\nCommands:\n";
    std::cout << "  PUT <key> <value>\n";
    std::cout << "  GET <key>\n";
    std::cout << "  REFRESH\n";
    std::cout << "  help\n";
    std::cout << "  exit\n\n";
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cout << "Usage: client <host:port> [more-host:port...]\n";
        return 1;
    }

    std::vector<std::string> nodes;
    for (int i = 1; i < argc; i++)
    {
        nodes.push_back(argv[i]);
        std::cout << "[Client] Added node: " << argv[i] << "\n";
    }

    std::string target = nodes[0];
    auto stub = createStub(target);

    // 🔥 Fetch cluster dynamically
    auto discovered = fetchClusterNodes(stub);

    if (!discovered.empty())
    {
        nodes = discovered;

        std::cout << "[Client] Cluster discovered: ";
        for (auto &n : nodes)
            std::cout << n << " ";
        std::cout << "\n";
    }
    else
    {
        std::cout << "[Client] Using bootstrap node only\n";
    }

    while (true)
    {
        std::cout << "zk> ";
        std::string input;
        std::getline(std::cin, input);

        if (input.empty())
            continue;

        if (input == "exit")
            break;

        if (input == "help")
        {
            printHelp();
            continue;
        }

        // Parse command
        std::istringstream iss(input);
        std::string cmd, key, value;

        iss >> cmd;

        if (cmd == "REFRESH")
        {
            auto discovered = fetchClusterNodes(stub);

            if (!discovered.empty())
            {
                nodes = discovered;
                std::cout << "[Client] Cluster updated\n";
            }
            continue;
        }

        if (cmd == "PUT")
        {
            iss >> key;
            std::getline(iss, value);

            if (!value.empty() && value[0] == ' ')
            {
                value.erase(0, 1);
            }

            if (key.empty() || value.empty())
            {
                std::cout << "Usage: PUT <key> <value>\n";
                continue;
            }

            while (!sendMessage(stub, target, input, nodes))
            {
            }
        }
        else if (cmd == "GET")
        {
            iss >> key;

            if (key.empty())
            {
                std::cout << "Usage: GET <key>\n";
                continue;
            }

            while (!sendMessage(stub, target, input, nodes))
            {
            }
        }
        else
        {
            std::cout << "Unknown command. Type 'help'\n";
        }
    }

    std::cout << "Exiting...\n";
    return 0;
}

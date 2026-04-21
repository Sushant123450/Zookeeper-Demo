#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "node.grpc.pb.h"

void sendMessage(std::string target, std::string message)
{
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = cluster::NodeService::NewStub(channel);

    cluster::MessageRequest request;
    request.set_from("Client");
    request.set_message(message);

    cluster::MessageReply reply;
    grpc::ClientContext context;

    auto status = stub->SendMessage(&context, request, &reply);

    if (status.ok())
    {
        std::cout << "Response: " << reply.status() << std::endl;
    }
    else
    {
        std::cout << "RPC failed: (" << status.error_code() << ") "
                  << status.error_message() << std::endl;
    }
}

void printHelp()
{
    std::cout << "\nCommands:\n";
    std::cout << "  PUT <key> <value>\n";
    std::cout << "  GET <key>\n";
    std::cout << "  help\n";
    std::cout << "  exit\n\n";
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cout << "Usage: client <host:port>\n";
        return 1;
    }

    std::string target = argv[1];

    std::cout << "Connected to " << target << "\n";
    std::cout << "Type 'help' for commands\n";

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

            sendMessage(target, input);
        }
        else if (cmd == "GET")
        {
            iss >> key;

            if (key.empty())
            {
                std::cout << "Usage: GET <key>\n";
                continue;
            }

            sendMessage(target, input);
        }
        else
        {
            std::cout << "Unknown command. Type 'help'\n";
        }
    }

    std::cout << "Exiting...\n";
    return 0;
}

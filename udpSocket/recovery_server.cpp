#include "protocol.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

constexpr int RECOVERY_PORT = 9001;

//The key is the sequence number, 
//and the value is the original encoded message
using ReplayBuffer = std::map<std::uint64_t, Message>;

// Send all requested bytes, even if send() writes only part.
bool send_all(int fd, const void* data, std::size_t size){
    const auto* bytes = static_cast<const char*>(data);
    std::size_t sent = 0;

    while(sent < size){
        ssize_t n = send(
            fd, bytes + sent, size - sent, MSG_NOSIGNAL
        );

        if (n < 0 && errno == EINTR){
            continue;
        }
        if (n <= 0){
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

// Read one newline-terminated request, with a size limit.
bool read_line(int fd, std::string& line) {
    line.clear();

    while (line.size() < 128) {
        char ch;
        ssize_t n = recv(fd, &ch, 1, 0);

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n <= 0) {
            return false;
        }

        if (ch == '\n') {
            return true;
        }

        line.push_back(ch);
    }

    return false;
}

void handle_client(int client_fd, const ReplayBuffer& replay) {
    std::string request;

    if (!read_line(client_fd, request)) {
        std::cerr << "Failed to read request\n";
        return;
    }

    std::istringstream input(request);
    std::string command;
    std::uint64_t sequence;
    std::string extra;

    if (!(input >> command >> sequence) ||
        command != "REQUEST" ||
        (input >> extra)) {
        const std::string response = "BAD_REQUEST\n";
        send_all(client_fd, response.data(), response.size());
        return;
    }

    auto it = replay.find(sequence);

    if (it == replay.end()) {
        const std::string response = "NOT_FOUND\n";
        send_all(client_fd, response.data(), response.size());

        std::cout << "Sequence " << sequence
                  << " not found in replay buffer\n";
        return;
    }

    const std::string response = "OK\n";

    if (!send_all(client_fd, response.data(), response.size())) {
        return;
    }

    if (!send_all(
            client_fd,
            it->second.data(),
            it->second.size())) {
        std::cerr << "Failed to send recovered message\n";
        return;
    }

    std::cout << "Retransmitted sequence "
              << sequence << '\n';
}

int main() {
    // Temporary test data: populate the replay buffer.
    ReplayBuffer replay;

    for (std::uint64_t seq = 5001; seq <= 5010; ++seq) {
        MarketData update{
            seq,
            101,
            245025,
            245050,
            200
        };

        replay.emplace(seq, encode(update));
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        std::cerr << "socket() failed: "
                  << std::strerror(errno) << '\n';
        return 1;
    }

    int reuse = 1;
    setsockopt(
        server_fd, SOL_SOCKET, SO_REUSEADDR,
        &reuse, sizeof(reuse)
    );

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(RECOVERY_PORT);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(
            server_fd,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)) < 0) {
        std::cerr << "bind() failed: "
                  << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 10) < 0) {
        std::cerr << "listen() failed: "
                  << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

    std::cout << "TCP recovery server listening on "
              << "127.0.0.1:" << RECOVERY_PORT << '\n';

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);

        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }

            std::cerr << "accept() failed: "
                      << std::strerror(errno) << '\n';
            continue;
        }

        handle_client(client_fd, replay);
        close(client_fd);
    }

    close(server_fd);
    return 0;
}
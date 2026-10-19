#include "protocol.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>

using ReplayBuffer = std::map<std::uint64_t, Message>;

constexpr int UDP_PORT = 9000;
constexpr int TCP_PORT = 9001;
constexpr std::size_t REPLAY_CAPACITY = 1000;

constexpr std::uint64_t FIRST_SEQUENCE = 5001;
constexpr std::uint64_t LAST_SEQUENCE = 5010;

//fault-injection
//packet loss
const std::uint64_t DROP_SEQUNECE = 0;
//duplicate
constexpr std::uint64_t DUPLICATE_SEQUENCE = 0;
//out of order
constexpr std::uint64_t REORDER_FIRST = 5003;
constexpr std::uint64_t REORDER_SECOND = 5004;

ReplayBuffer replay;
std::mutex replay_mutex;
std::atomic<bool> stopping{false};

// Send all bytes, handling partial writes.
bool send_all(int fd, const void* data, std::size_t size) {
    const auto* bytes = static_cast<const char*>(data);
    std::size_t done = 0;

    while (done < size) {
        ssize_t n = send(
            fd, bytes + done, size - done, MSG_NOSIGNAL
        );

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n <= 0) {
            return false;
        }

        done += static_cast<std::size_t>(n);
    }

    return true;
}

// Read a bounded, newline-terminated TCP request.
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

void handle_client(int client_fd) {
    std::string request;

    if (!read_line(client_fd, request)) {
        std::cerr << "Failed to read recovery request\n";
        return;
    }

    std::istringstream input(request);
    std::string command;
    std::uint64_t sequence{};
    std::string extra;

    if (!(input >> command >> sequence) ||
        command != "REQUEST" ||
        (input >> extra)) {
        const std::string response = "BAD_REQUEST\n";
        send_all(client_fd, response.data(), response.size());
        return;
    }

    // Copy the message while holding the lock.
    // Do not hold the mutex during network transmission.
    Message packet{};
    bool found = false;

    {
        std::lock_guard<std::mutex> lock(replay_mutex);

        auto it = replay.find(sequence);

        if (it != replay.end()) {
            packet = it->second;
            found = true;
        }
    }

    if (!found) {
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

    if (!send_all(client_fd, packet.data(), packet.size())) {
        std::cerr << "Failed to retransmit sequence "
                  << sequence << '\n';
        return;
    }

    std::cout << "[TCP RECOVERY] Retransmitted sequence "
              << sequence << '\n';
}

void recovery_server(int server_fd) {
    while (!stopping.load()) {
        int client_fd = accept(
            server_fd, nullptr, nullptr
        );

        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (stopping.load()) {
                break;
            }

            std::cerr << "accept() failed: "
                      << std::strerror(errno) << '\n';
            continue;
        }

        handle_client(client_fd);
        close(client_fd);
    }
}

int main() {
    // ---------------- TCP RECOVERY SERVER ----------------

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        std::cerr << "TCP socket() failed: "
                  << std::strerror(errno) << '\n';
        return 1;
    }

    int reuse = 1;

    setsockopt(
        server_fd,
        SOL_SOCKET,
        SO_REUSEADDR,
        &reuse,
        sizeof(reuse)
    );

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(TCP_PORT);
    server_address.sin_addr.s_addr =
        htonl(INADDR_LOOPBACK);

    if (bind(
            server_fd,
            reinterpret_cast<sockaddr*>(&server_address),
            sizeof(server_address)) < 0) {
        std::cerr << "TCP bind() failed: "
                  << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 10) < 0) {
        std::cerr << "TCP listen() failed: "
                  << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

    std::thread tcp_thread(recovery_server, server_fd);

    std::cout << "TCP recovery server listening on "
              << "127.0.0.1:" << TCP_PORT << '\n';

    // ---------------- UDP PUBLISHER ----------------

    int udp_fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (udp_fd < 0) {
        std::cerr << "UDP socket() failed: "
                  << std::strerror(errno) << '\n';

        stopping.store(true);
        shutdown(server_fd, SHUT_RDWR);
        tcp_thread.join();
        close(server_fd);
        return 1;
    }

    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(UDP_PORT);
    destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    for (std::uint64_t seq = FIRST_SEQUENCE; seq <= LAST_SEQUENCE; ++seq) {
        MarketData update{
            seq,
            101,
            245025,
            245050,
            200
        };

        Message packet = encode(update);

        // Store EVERY message, including sequence 5005.
        {
            std::lock_guard<std::mutex> lock(replay_mutex);
            replay[seq] = packet;
            while (replay.size() > REPLAY_CAPACITY){
                replay.erase(replay.begin());
            }
        }

        std::cout << "Stored sequence " << seq << " in replay buffer\n";

        // Fault injection
        if (seq == DROP_SEQUNECE) {
            std::cout << "[FAULT INJECTION] Skipping UDP " << "sequence 5005\n";
            continue;
        }

        if (seq == REORDER_FIRST) {
            std::cout << "[FAULT INJECTION] Delaying UDP sequence "<< seq << '\n';
            continue;
        }
        ssize_t sent = sendto(
            udp_fd,
            packet.data(),
            packet.size(),
            0,
            reinterpret_cast<sockaddr*>(&destination),
            sizeof(destination)
        );


        if (sent < 0) {
            std::cerr << "UDP sendto() failed: "
                      << std::strerror(errno) << '\n';
            close(udp_fd);
            stopping.store(true);
            shutdown(server_fd, SHUT_RDWR);
            tcp_thread.join();
            close(server_fd);
            return 1;
        }

        if (sent != static_cast<ssize_t>(packet.size())) {
            std::cerr << "Unexpected UDP message size\n";
            close(udp_fd);
            stopping.store(true);
            shutdown(server_fd, SHUT_RDWR);
            tcp_thread.join();
            close(server_fd);
            return 1;
        }

        std::cout << "[UDP] Sent " << sent << " bytes, sequence " << seq << '\n';
        
        if (seq == REORDER_SECOND) {
            Message delayed_packet{};
            bool found = false;

            {
                std::lock_guard<std::mutex> lock(replay_mutex);
                auto it = replay.find(REORDER_FIRST);

                if (it != replay.end()) {
                    delayed_packet = it->second;
                    found = true;
                }
            }

            if (found) {
                ssize_t delayed_sent = sendto(
                    udp_fd,
                    delayed_packet.data(),
                    delayed_packet.size(),
                    0,
                    reinterpret_cast<sockaddr*>(&destination),
                    sizeof(destination)
                );

                if (delayed_sent ==
                    static_cast<ssize_t>(delayed_packet.size())) {
                    std::cout << "[FAULT INJECTION] Sent delayed sequence "
                            << REORDER_FIRST << " after "
                            << REORDER_SECOND << '\n';
                }
            }
        }

        if (seq == DUPLICATE_SEQUENCE) {
            ssize_t duplicate_sent = sendto(
                udp_fd,
                packet.data(),
                packet.size(),
                0,
                reinterpret_cast<sockaddr*>(&destination),
                sizeof(destination)
            );

            if (duplicate_sent == static_cast<ssize_t>(packet.size())) {
                std::cout << "[FAULT INJECTION] Duplicated UDP sequence "
                        << seq << '\n';
            } else {
                std::cerr << "Failed to send duplicate sequence "
                        << seq << '\n';
            }
        }
        // Keep this demonstration easy to observe.
        std::this_thread::sleep_for(
            std::chrono::milliseconds(20)
        );
    }

    close(udp_fd);

    std::cout << "\nPublisher finished sending.\n";
    std::cout << "TCP recovery remains available.\n";
    std::cout << "Press Enter to stop the server.\n";

    std::cin.get();

    stopping.store(true);
    shutdown(server_fd, SHUT_RDWR);
    tcp_thread.join();
    close(server_fd);

    std::cout << "TickWire publisher stopped.\n";
    return 0;
}


//g++ -std=c++20 -Wall -Wextra -Wpedantic -O2 -pthread receiver.cpp -o receiver
//g++ -std=c++20 -Wall -Wextra -Wpedantic -O2 -pthread publisher.cpp -o publisher
#include "protocol.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <cstdint>
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
#include <cstdlib>
#include <cstddef>
#include <algorithm>
#include <vector>
#include <poll.h>
#include <sys/time.h>


using ReplayBuffer = std::map<std::uint64_t, Message>;

constexpr int UDP_PORT = 9000; // Feed A multicast port
constexpr int UDP_PORT_B = 9002; // Feed B multicast port
constexpr int TCP_PORT = 9001;
constexpr const char* FEED_A_GROUP = "239.10.10.1";
constexpr const char* FEED_B_GROUP = "239.10.10.2";
constexpr int TCP_CLIENT_REQUEST_TIMEOUT_MS = 1000;
constexpr std::size_t REPLAY_CAPACITY = 20000;

constexpr std::uint64_t FIRST_SEQUENCE = 5001;
constexpr std::uint64_t LAST_SEQUENCE = 15000;

//fault-injection
//packet loss
// Runtime-configurable fault injection.
std::uint64_t DROP_FIRST_SEQUENCE = 999999;
std::uint64_t DROP_INTERVAL = 1000;

std::uint64_t DROP_FEED_A_SEQUENCE = 0;
std::uint64_t DROP_FEED_B_SEQUENCE = 0;

std::uint64_t DUPLICATE_SEQUENCE = 0;

std::uint64_t REORDER_FIRST = 0;
std::uint64_t REORDER_SECOND = 0;

ReplayBuffer replay;
std::mutex replay_mutex;
std::atomic<bool> stopping{false};
std::atomic<bool> publishing_finished{false};
bool verbose_logging = true;

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

bool send_udp_to_feed(
    int fd,
    const sockaddr_in& destination,
    const Message& packet,
    std::uint64_t sequence,
    const char* feed_name
) {
    const ssize_t sent = sendto(
        fd,
        packet.data(),
        packet.size(),
        0,
        reinterpret_cast<const sockaddr*>(&destination),
        sizeof(destination)
    );

    if (sent < 0) {
        std::cerr << "[" << feed_name << "] sendto() failed for sequence "
                  << sequence << ": "
                  << std::strerror(errno) << '\n';
        return false;
    }

    if (sent != static_cast<ssize_t>(packet.size())) {
        std::cerr << "[" << feed_name
                  << "] Unexpected UDP message size\n";
        return false;
    }

    if (verbose_logging) {
        std::cout << "[" << feed_name << "] Sent "
                  << sent << " bytes, sequence "
                  << sequence << '\n';
    }

    return true;
}

// Read a bounded, newline-terminated TCP request with one total deadline.
// This prevents an idle or slow-drip client from blocking the serial server.
bool read_line(int fd, std::string& line) {
    line.clear();
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(TCP_CLIENT_REQUEST_TIMEOUT_MS);

    while (line.size() < 128) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            errno = ETIMEDOUT;
            return false;
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - now
        ).count();
        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLIN;
        const int ready = poll(&pfd, 1, static_cast<int>(std::max<long long>(1, remaining)));
        if (ready < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (ready == 0) {
            errno = ETIMEDOUT;
            return false;
        }
        if (pfd.revents & (POLLERR | POLLNVAL)) {
            errno = ECONNRESET;
            return false;
        }

        char ch;
        const ssize_t n = recv(fd, &ch, 1, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        if (ch == '\n') return true;
        line.push_back(ch);
    }
    errno = EMSGSIZE;
    return false;
}

void configure_accepted_client(int client_fd) {
    // Bound blocked sends as well; read_line enforces a total request deadline.
    timeval send_timeout{};
    send_timeout.tv_sec = 2;
    send_timeout.tv_usec = 0;
    (void)setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO,
                     &send_timeout, sizeof(send_timeout));
}

void handle_client(int client_fd) {
    std::string request;

    if (!read_line(client_fd, request)) {
        if (verbose_logging) {
            std::cerr << "Failed to read recovery request\n";
        }
        return;
    }

    std::istringstream input(request);
    std::string command;
    std::string extra;

    if (!(input >> command)) {
        const std::string response = "BAD_REQUEST\n";
        send_all(client_fd, response.data(), response.size());
        return;
    }

    // A status query lets the receiver distinguish a quiet feed from EOF.
    if (command == "STATUS") {
        if (input >> extra) {
            const std::string response = "BAD_REQUEST\n";
            send_all(client_fd, response.data(), response.size());
            return;
        }

        const std::string response = publishing_finished.load()
            ? "END " + std::to_string(LAST_SEQUENCE) + "\n"
            : "RUNNING\n";
        send_all(client_fd, response.data(), response.size());
        return;
    }

    // Recover a contiguous range in one TCP connection. This prevents a
    // burst-induced loss of thousands of UDP packets from creating thousands
    // of sequential TCP connection setups.
    if (command == "RANGE") {
        std::uint64_t first{};
        std::uint64_t last{};
        if (!(input >> first >> last) || (input >> extra) ||
            first < FIRST_SEQUENCE || first > last || last > LAST_SEQUENCE) {
            const std::string response = "BAD_REQUEST\n";
            send_all(client_fd, response.data(), response.size());
            return;
        }

        std::vector<std::byte> payload;
        const auto expected_count = static_cast<std::size_t>(last - first + 1);
        payload.reserve(expected_count * Message{}.size());
        std::uint64_t missing_sequence = 0;

        {
            std::lock_guard<std::mutex> lock(replay_mutex);
            for (std::uint64_t sequence = first;; ++sequence) {
                const auto it = replay.find(sequence);
                if (it == replay.end()) {
                    missing_sequence = sequence;
                    break;
                }
                payload.insert(payload.end(), it->second.begin(), it->second.end());
                if (sequence == last) {
                    break;
                }
            }
        }

        if (missing_sequence != 0) {
            const std::string response =
                "NOT_FOUND " + std::to_string(missing_sequence) + "\n";
            send_all(client_fd, response.data(), response.size());
            return;
        }

        const std::string response = "OK " + std::to_string(expected_count) + "\n";
        if (!send_all(client_fd, response.data(), response.size())) {
            return;
        }
        if (!payload.empty() && !send_all(client_fd, payload.data(), payload.size())) {
            std::cerr << "Failed to send recovered range "
                      << first << "-" << last << '\n';
            return;
        }

        if (verbose_logging) {
            std::cout << "[TCP RANGE] Sent sequences " << first
                      << "-" << last << " (" << expected_count << " messages)\n";
        }
        return;
    }

    if (command != "REQUEST") {
        const std::string response = "BAD_REQUEST\n";
        send_all(client_fd, response.data(), response.size());
        return;
    }

    std::uint64_t sequence{};
    if (!(input >> sequence) || (input >> extra)) {
        const std::string response = "BAD_REQUEST\n";
        send_all(client_fd, response.data(), response.size());
        return;
    }

    Message packet{};
    bool found = false;
    {
        std::lock_guard<std::mutex> lock(replay_mutex);
        const auto it = replay.find(sequence);
        if (it != replay.end()) {
            packet = it->second;
            found = true;
        }
    }

    if (!found) {
        const std::string response = "NOT_FOUND\n";
        send_all(client_fd, response.data(), response.size());
        if (verbose_logging) {
            std::cout << "Sequence " << sequence
                      << " not found in replay buffer\n";
        }
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

    if (verbose_logging) {
        std::cout << "[TCP RECOVERY] Retransmitted sequence "
                  << sequence << '\n';
    }
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

        configure_accepted_client(client_fd);
        handle_client(client_fd);
        close(client_fd);
    }
}

int main(int argc, char* argv[]) {
        const std::string scenario =
        (argc > 1) ? argv[1] : "normal";

    if (argc > 2) {
        std::cerr << "Usage: ./publisher [scenario]\n";
        return 1;
    }

    if (scenario == "normal") {
        // All faults remain disabled.
    } else if (scenario == "drop-a") {
        DROP_FEED_A_SEQUENCE = 5005;
    } else if (scenario == "drop-b") {
        DROP_FEED_B_SEQUENCE = 5005;
    } else if (scenario == "drop-both") {
        DROP_FIRST_SEQUENCE = 5005;
        DROP_INTERVAL = 1000;
    } else if (scenario == "drop-tail") {
        // Deliberately drop the final 251 sequences on both UDP feeds. This
        // validates end-of-stream detection and TCP range recovery for losses
        // that would otherwise be invisible because no later UDP packet arrives.
        DROP_FIRST_SEQUENCE = LAST_SEQUENCE - 250;
        DROP_INTERVAL = 1;
    } else if (scenario == "duplicate") {
        DUPLICATE_SEQUENCE = 5005;
    } else if (scenario == "reorder") {
        REORDER_FIRST = 5005;
        REORDER_SECOND = 5006;
    } else {
        std::cerr
            << "Unknown scenario: " << scenario << '\n'
            << "Use: normal, drop-a, drop-b, drop-both, drop-tail, "
            << "duplicate, reorder\n";
        return 1;
    }
    unsigned long send_interval_ms = 20;

    if (const char* env = std::getenv("TICKWIRE_SEND_INTERVAL_MS")) {
        try {
            send_interval_ms = std::stoul(env);
        } catch (const std::exception&) {
            std::cerr << "Invalid TICKWIRE_SEND_INTERVAL_MS value\n";
            return 1;
        }

        if (send_interval_ms > 1000) {
            std::cerr << "Send interval must be between 0 and 1000 ms\n";
            return 1;
        }
    }

    if (const char* env = std::getenv("TICKWIRE_VERBOSE")) {
        verbose_logging = std::string(env) != "0";
    }

    std::cout << "[SCENARIO] " << scenario << '\n';
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
    const char* tcp_bind_ip = std::getenv("TICKWIRE_TCP_BIND");
    if (tcp_bind_ip == nullptr) tcp_bind_ip = "127.0.0.1";
    if (inet_pton(AF_INET, tcp_bind_ip, &server_address.sin_addr) != 1) {
        std::cerr << "Invalid TICKWIRE_TCP_BIND IPv4 address: "
                  << tcp_bind_ip << '\n';
        close(server_fd);
        return 1;
    }

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
              << tcp_bind_ip << ':' << TCP_PORT << '\n';

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

    // Each redundant feed is sent to its own IPv4 multicast group.
    // All receivers that join a group/port can receive the same datagrams.
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(UDP_PORT);
    if (inet_pton(AF_INET, FEED_A_GROUP, &destination.sin_addr) != 1) {
        std::cerr << "Invalid Feed A multicast group\n";
        close(udp_fd);
        stopping.store(true);
        shutdown(server_fd, SHUT_RDWR);
        tcp_thread.join();
        close(server_fd);
        return 1;
    }

    sockaddr_in destination_b{};
    destination_b.sin_family = AF_INET;
    destination_b.sin_port = htons(UDP_PORT_B);
    if (inet_pton(AF_INET, FEED_B_GROUP, &destination_b.sin_addr) != 1) {
        std::cerr << "Invalid Feed B multicast group\n";
        close(udp_fd); stopping.store(true); shutdown(server_fd, SHUT_RDWR);
        tcp_thread.join(); close(server_fd);
        return 1;
    }

    // Keep multicast on the local network only and enable local loopback so
    // receivers on this machine can test the feeds too.
    unsigned char multicast_ttl = 1;
    unsigned char multicast_loop = 1;
    if (setsockopt(udp_fd, IPPROTO_IP, IP_MULTICAST_TTL,
                   &multicast_ttl, sizeof(multicast_ttl)) < 0 ||
        setsockopt(udp_fd, IPPROTO_IP, IP_MULTICAST_LOOP,
                   &multicast_loop, sizeof(multicast_loop)) < 0) {
        std::cerr << "Could not configure UDP multicast: "
                  << std::strerror(errno) << '\n';
        close(udp_fd); stopping.store(true); shutdown(server_fd, SHUT_RDWR);
        tcp_thread.join(); close(server_fd);
        return 1;
    }

    // Optional: choose the same local interface address in publisher and
    // receiver via TICKWIRE_MULTICAST_IF (for example, 192.168.1.20).
    if (const char* interface_ip = std::getenv("TICKWIRE_MULTICAST_IF")) {
        in_addr interface_address{};
        if (inet_pton(AF_INET, interface_ip, &interface_address) != 1 ||
            setsockopt(udp_fd, IPPROTO_IP, IP_MULTICAST_IF,
                       &interface_address, sizeof(interface_address)) < 0) {
            std::cerr << "Invalid/unusable TICKWIRE_MULTICAST_IF: "
                      << interface_ip << '\n';
            close(udp_fd); stopping.store(true); shutdown(server_fd, SHUT_RDWR);
            tcp_thread.join(); close(server_fd);
            return 1;
        }
    }

    Message delayed_packet{};
    bool has_delayed_packet = false;

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

        if (verbose_logging) {
            std::cout << "Stored sequence " << seq << " in replay buffer\n";
        }

        // Fault injection
        if (
            seq >= DROP_FIRST_SEQUENCE
            && (seq - DROP_FIRST_SEQUENCE) % DROP_INTERVAL == 0
        ) {
            if (verbose_logging) {
                std::cout << "[FAULT INJECTION] Skipping UDP sequence "
                          << seq << '\n';
            }
            continue;
        }

        if (seq == REORDER_FIRST) {
            delayed_packet = packet;
            has_delayed_packet = true;

            std::cout << "[FAULT INJECTION] Delaying UDP sequence "
                    << seq << '\n';
            continue;
        }
        // Send the current message on both UDP feeds.
       // Simulate packet loss on Feed A only.
        // Feed A: optionally drop this sequence.
        if (seq == DROP_FEED_A_SEQUENCE) {
            std::cout
                << "[FAULT INJECTION] Dropping sequence "
                << seq << " on Feed A only\n";
        } else {
            send_udp_to_feed(
                udp_fd, destination, packet, seq, "Feed A"
            );
        }

        // Feed B: optionally drop this sequence.
        if (seq == DROP_FEED_B_SEQUENCE) {
            std::cout
                << "[FAULT INJECTION] Dropping sequence "
                << seq << " on Feed B only\n";
        } else {
            send_udp_to_feed(
                udp_fd, destination_b, packet, seq, "Feed B"
            );
        }

        // Feed B always sends the current message.
        // send_udp_to_feed(
        //     udp_fd, destination_b, packet, seq, "Feed B"
        // );

        // If this is the second sequence in a reordering test,
        // release the delayed packet on both feeds.
        if (seq == REORDER_SECOND && has_delayed_packet) {
            const bool delayed_a = send_udp_to_feed(
                udp_fd,
                destination,
                delayed_packet,
                REORDER_FIRST,
                "Feed A"
            );

            const bool delayed_b = send_udp_to_feed(
                udp_fd,
                destination_b,
                delayed_packet,
                REORDER_FIRST,
                "Feed B"
            );

            if (delayed_a || delayed_b) {
                std::cout
                    << "[FAULT INJECTION] Sent delayed UDP sequence "
                    << REORDER_FIRST << " after "
                    << REORDER_SECOND << '\n';
            } else {
                std::cerr << "Failed to send delayed sequence "
                        << REORDER_FIRST << " on both feeds\n";
            }

            has_delayed_packet = false;
        }
        // ssize_t sent = sendto(
        //     udp_fd,
        //     packet.data(),
        //     packet.size(),
        //     0,
        //     reinterpret_cast<sockaddr*>(&destination),
        //     sizeof(destination)
        // );

        // if (seq == REORDER_SECOND && has_delayed_packet) {
         
        //     ssize_t delayed_sent = sendto(
        //         udp_fd,
        //         delayed_packet.data(),
        //         delayed_packet.size(),
        //         0,
        //         reinterpret_cast<sockaddr*>(&destination),
        //         sizeof(destination)
        //     );

        //     if (delayed_sent ==
        //         static_cast<ssize_t>(delayed_packet.size())) {
        //         std::cout << "[FAULT INJECTION] Sent delayed UDP sequence "
        //                 << REORDER_FIRST << " after "
        //                 << REORDER_SECOND << '\n';
        //         has_delayed_packet = false;
        //     } else {
        //         std::cerr << "Failed to send delayed UDP sequence "
        //                 << REORDER_FIRST << '\n';
        //     }
        // }

        // if (sent < 0) {
        //     std::cerr << "UDP sendto() failed: "
        //               << std::strerror(errno) << '\n';
        //     close(udp_fd);
        //     stopping.store(true);
        //     shutdown(server_fd, SHUT_RDWR);
        //     tcp_thread.join();
        //     close(server_fd);
        //     return 1;
        // }

        // if (sent != static_cast<ssize_t>(packet.size())) {
        //     std::cerr << "Unexpected UDP message size\n";
        //     close(udp_fd);
        //     stopping.store(true);
        //     shutdown(server_fd, SHUT_RDWR);
        //     tcp_thread.join();
        //     close(server_fd);
        //     return 1;
        // }

        // std::cout << "[UDP] Sent " << sent << " bytes, sequence " << seq << '\n';
        

        if (seq == DUPLICATE_SEQUENCE) {
            const bool duplicate_a = send_udp_to_feed(
                udp_fd,
                destination,
                packet,
                seq,
                "Feed A"
            );

            const bool duplicate_b = send_udp_to_feed(
                udp_fd,
                destination_b,
                packet,
                seq,
                "Feed B"
            );

            if (duplicate_a || duplicate_b) {
                std::cout << "[FAULT INJECTION] Duplicated UDP sequence "
                        << seq << " on the feeds\n";
            } else {
                std::cerr << "Failed to send duplicate sequence "
                        << seq << " on both feeds\n";
            }
        }

        // Keep this demonstration easy to observe.
        std::this_thread::sleep_for(
            std::chrono::milliseconds(send_interval_ms)
        );
    }

    // Publish an authoritative end-of-stream state over TCP. Unlike a UDP
    // end marker, this cannot be lost in the same overloaded receive queues.
    publishing_finished.store(true);
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
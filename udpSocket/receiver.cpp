#include "protocol.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <map>
#include <mutex>
#include <optional>
#include <poll.h>
#include <csignal>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <atomic>
#include <unistd.h>
#include <utility>
#include <set>
#include <vector>
#include <algorithm>
#include <cmath>
#include <iomanip>


// ---------------- CONFIGURATION ----------------

constexpr int UDP_PORT = 9000; // Feed A multicast port
constexpr int UDP_PORT_B = 9002; // Feed B multicast port
constexpr int TCP_PORT = 9001;
constexpr const char* FEED_A_GROUP = "239.10.10.1";
constexpr const char* FEED_B_GROUP = "239.10.10.2";
constexpr std::uint64_t FIRST_SEQUENCE = 5001;
constexpr std::uint64_t LAST_SEQUENCE = 15000;
constexpr int UDP_RECEIVE_BUFFER_BYTES = 4 * 1024 * 1024;

constexpr int POLL_TIMEOUT_MS = 1;
constexpr int TCP_TIMEOUT_MS = 500;
constexpr int RETRY_DELAY_MS = 200;
constexpr int MAX_RECOVERY_ATTEMPTS = 3;
constexpr std::uint64_t MAX_GAP_RECOVERY = 1000;
constexpr auto REORDER_GRACE_PERIOD = std::chrono::milliseconds(2);
constexpr auto END_STATUS_IDLE_PERIOD = std::chrono::milliseconds(200);
constexpr auto END_STATUS_CHECK_INTERVAL = std::chrono::milliseconds(250);
constexpr auto RANGE_RETRY_INTERVAL = std::chrono::milliseconds(250);

bool verbose_logging = true;
std::atomic<bool> pause_recovery_worker{false};
volatile std::sig_atomic_t stop_requested = 0;

void handle_signal(int) {
    stop_requested = 1;
}

void configure_udp_receive_buffer(int fd, const char* feed_name) {
    const int requested = UDP_RECEIVE_BUFFER_BYTES;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &requested, sizeof(requested)) < 0) {
        std::cerr << "Could not set " << feed_name << " SO_RCVBUF: "
                  << std::strerror(errno) << '\n';
    }

    int actual = 0;
    socklen_t length = sizeof(actual);
    if (getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &actual, &length) == 0) {
        std::cout << "[SO_RCVBUF] " << feed_name << " effective socket value: "
                  << actual << " bytes\n";
    }
}

void set_tcp_client_timeouts(int fd) {
    timeval timeout{};
    timeout.tv_sec = TCP_TIMEOUT_MS / 1000;
    timeout.tv_usec = (TCP_TIMEOUT_MS % 1000) * 1000;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

// Bind to INADDR_ANY and join the selected multicast group. SO_REUSEADDR
// permits multiple local receiver processes to subscribe to the same feed.
bool configure_multicast_receiver(int fd, int port, const char* group_ip,
                                  const char* feed_name) {
    int reuse = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << feed_name << " SO_REUSEADDR failed: "
                  << std::strerror(errno) << '\n';
        return false;
    }

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(static_cast<std::uint16_t>(port));
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(fd, reinterpret_cast<sockaddr*>(&local), sizeof(local)) < 0) {
        std::cerr << feed_name << " bind() failed: "
                  << std::strerror(errno) << '\n';
        return false;
    }

    ip_mreq membership{};
    if (inet_pton(AF_INET, group_ip, &membership.imr_multiaddr) != 1) {
        std::cerr << "Invalid " << feed_name << " multicast group: "
                  << group_ip << '\n';
        return false;
    }
    membership.imr_interface.s_addr = htonl(INADDR_ANY);
    if (const char* interface_ip = std::getenv("TICKWIRE_MULTICAST_IF")) {
        if (inet_pton(AF_INET, interface_ip, &membership.imr_interface) != 1) {
            std::cerr << "Invalid TICKWIRE_MULTICAST_IF: "
                      << interface_ip << '\n';
            return false;
        }
    }

    if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                   &membership, sizeof(membership)) < 0) {
        std::cerr << feed_name << " IP_ADD_MEMBERSHIP failed for "
                  << group_ip << ": " << std::strerror(errno) << '\n';
        return false;
    }

    std::cout << feed_name << " joined multicast group " << group_ip
              << " on port " << port << '\n';
    return true;
}

// ---------------- TCP HELPERS ----------------

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

bool recv_all(int fd, void* data, std::size_t size) {
    auto* bytes = static_cast<char*>(data);
    std::size_t done = 0;

    while (done < size) {
        ssize_t n = recv(
            fd, bytes + done, size - done, 0
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

bool recv_line(int fd, std::string& line) {
    line.clear();

    while (line.size() < 128) {
        char ch;

        if (!recv_all(fd, &ch, 1)) {
            return false;
        }

        if (ch == '\n') {
            return true;
        }

        line.push_back(ch);
    }

    return false;
}

// Connect without allowing connect() to block indefinitely.
bool connect_with_timeout(
    int fd,
    sockaddr* address,
    socklen_t address_length,
    int timeout_ms
) {
    int old_flags = fcntl(fd, F_GETFL, 0);

    if (old_flags < 0) {
        return false;
    }

    if (fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) < 0) {
        return false;
    }

    int result = connect(fd, address, address_length);

    if (result < 0 && errno != EINPROGRESS) {
        int saved_errno = errno;
        fcntl(fd, F_SETFL, old_flags);
        errno = saved_errno;
        return false;
    }

    if (result < 0) {
        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLOUT;

        do {
            result = poll(&pfd, 1, timeout_ms);
        } while (result < 0 && errno == EINTR);

        if (result <= 0) {
            int saved_errno = result == 0 ? ETIMEDOUT : errno;
            fcntl(fd, F_SETFL, old_flags);
            errno = saved_errno;
            return false;
        }

        int socket_error = 0;
        socklen_t error_length = sizeof(socket_error);

        if (getsockopt(
                fd,
                SOL_SOCKET,
                SO_ERROR,
                &socket_error,
                &error_length
            ) < 0) {
            int saved_errno = errno;
            fcntl(fd, F_SETFL, old_flags);
            errno = saved_errno;
            return false;
        }

        if (socket_error != 0) {
            fcntl(fd, F_SETFL, old_flags);
            errno = socket_error;
            return false;
        }
    }

    if (fcntl(fd, F_SETFL, old_flags) < 0) {
        return false;
    }

    return true;
}

// ---------------- TCP RECOVERY CLIENT ----------------

std::optional<MarketData> recover_sequence(std::uint64_t sequence) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        std::cerr << "Recovery Socket failed: "
                  << std::strerror(errno) << '\n';
        return std::nullopt;
    }

    timeval timeout{};
    timeout.tv_sec = TCP_TIMEOUT_MS / 1000;
    timeout.tv_usec = (TCP_TIMEOUT_MS % 1000) * 1000;

    setsockopt(
        fd, SOL_SOCKET, SO_RCVTIMEO,
        &timeout, sizeof(timeout)
    );

    setsockopt(
        fd, SOL_SOCKET, SO_SNDTIMEO,
        &timeout, sizeof(timeout)
    );

    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(TCP_PORT);
    const char* recovery_host = std::getenv("TICKWIRE_RECOVERY_HOST");
    if (recovery_host == nullptr) recovery_host = "127.0.0.1";
    if (inet_pton(AF_INET, recovery_host, &server.sin_addr) != 1) {
        std::cerr << "Invalid TICKWIRE_RECOVERY_HOST IPv4 address: "
                  << recovery_host << '\n';
        close(fd);
        return std::nullopt;
    }

    if (!connect_with_timeout(
            fd,
            reinterpret_cast<sockaddr*>(&server),
            sizeof(server),
            TCP_TIMEOUT_MS
        )) {
        std::cerr << "Recovery connection failed: "
                  << std::strerror(errno) << '\n';
        close(fd);
        return std::nullopt;
    }

    const std::string request =
        "REQUEST " + std::to_string(sequence) + "\n";

    if (!send_all(fd, request.data(), request.size())) {
        std::cerr << "Failed to send recovery request\n";
        close(fd);
        return std::nullopt;
    }

    std::string response;

    if (!recv_line(fd, response)) {
        std::cerr << "Failed to read recovery response\n";
        close(fd);
        return std::nullopt;
    }

    if (response != "OK") {
        std::cerr << "Recovery unavailable for sequence "
                  << sequence << ": " << response << '\n';
        close(fd);
        return std::nullopt;
    }

    Message packet{};

    if (!recv_all(fd, packet.data(), packet.size())) {
        std::cerr << "Incomplete recovered message\n";
        close(fd);
        return std::nullopt;
    }

    close(fd);

    auto update = decode(packet);

    if (!update || update->sequence != sequence) {
        std::cerr << "Invalid recovered sequence\n";
        return std::nullopt;
    }

    return update;
}

// Query the publisher over TCP. std::nullopt means RUNNING or a retryable
// query failure; an END response supplies the authoritative final sequence.
std::optional<std::uint64_t> query_publisher_final_sequence() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }
    set_tcp_client_timeouts(fd);

    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(TCP_PORT);
    const char* recovery_host = std::getenv("TICKWIRE_RECOVERY_HOST");
    if (recovery_host == nullptr) recovery_host = "127.0.0.1";
    if (inet_pton(AF_INET, recovery_host, &server.sin_addr) != 1) {
        std::cerr << "Invalid TICKWIRE_RECOVERY_HOST IPv4 address: "
                  << recovery_host << '\n';
        close(fd);
        return std::nullopt;
    }
    if (!connect_with_timeout(fd, reinterpret_cast<sockaddr*>(&server),
                              sizeof(server), TCP_TIMEOUT_MS)) {
        close(fd);
        return std::nullopt;
    }

    const std::string request = "STATUS\n";
    if (!send_all(fd, request.data(), request.size())) {
        close(fd);
        return std::nullopt;
    }

    std::string response;
    if (!recv_line(fd, response)) {
        close(fd);
        return std::nullopt;
    }
    close(fd);

    std::istringstream input(response);
    std::string status;
    std::uint64_t final_sequence{};
    std::string extra;
    if (!(input >> status) || status != "END" ||
        !(input >> final_sequence) || (input >> extra) ||
        final_sequence < FIRST_SEQUENCE || final_sequence > LAST_SEQUENCE) {
        return std::nullopt;
    }
    return final_sequence;
}

// Retrieve a contiguous sequence interval in one TCP request / connection.
// The publisher responds with "OK <count>\n" followed by count binary packets.
std::optional<std::vector<MarketData>> recover_range(
    std::uint64_t first, std::uint64_t last) {
    if (first > last) {
        return std::vector<MarketData>{};
    }
    if (first < FIRST_SEQUENCE || last > LAST_SEQUENCE) {
        return std::nullopt;
    }

    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }
    set_tcp_client_timeouts(fd);

    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(TCP_PORT);
    const char* recovery_host = std::getenv("TICKWIRE_RECOVERY_HOST");
    if (recovery_host == nullptr) recovery_host = "127.0.0.1";
    if (inet_pton(AF_INET, recovery_host, &server.sin_addr) != 1) {
        std::cerr << "Invalid TICKWIRE_RECOVERY_HOST IPv4 address: "
                  << recovery_host << '\n';
        close(fd);
        return std::nullopt;
    }
    if (!connect_with_timeout(fd, reinterpret_cast<sockaddr*>(&server),
                              sizeof(server), TCP_TIMEOUT_MS)) {
        close(fd);
        return std::nullopt;
    }

    const std::string request =
        "RANGE " + std::to_string(first) + " " + std::to_string(last) + "\n";
    if (!send_all(fd, request.data(), request.size())) {
        close(fd);
        return std::nullopt;
    }

    std::string response;
    if (!recv_line(fd, response)) {
        close(fd);
        return std::nullopt;
    }

    std::istringstream input(response);
    std::string status;
    std::size_t count{};
    std::string extra;
    const auto expected_count = static_cast<std::size_t>(last - first + 1);
    if (!(input >> status >> count) || status != "OK" ||
        (input >> extra) || count != expected_count) {
        close(fd);
        return std::nullopt;
    }

    std::vector<MarketData> messages;
    messages.reserve(count);
    for (std::uint64_t expected = first;; ++expected) {
        Message packet{};
        if (!recv_all(fd, packet.data(), packet.size())) {
            close(fd);
            return std::nullopt;
        }

        auto update = decode(packet);
        if (!update || update->sequence != expected) {
            close(fd);
            return std::nullopt;
        }
        messages.push_back(*update);
        if (expected == last) {
            break;
        }
    }

    close(fd);
    return messages;
}

// ---------------- RECOVERY QUEUES ----------------

struct RecoveryResult {
    std::uint64_t sequence;
    std::optional<MarketData> message;
};

std::mutex request_mutex;
std::condition_variable request_cv;
std::deque<std::uint64_t> recovery_requests;

std::mutex result_mutex;
std::deque<RecoveryResult> recovery_results;

std::mutex inflight_mutex;
std::map<std::uint64_t, bool> inflight_recoveries;

std::atomic<bool> recovery_stopping{false};

bool mark_recovery_inflight(std::uint64_t sequence) {
    std::lock_guard<std::mutex> lock(inflight_mutex);

    auto [it, inserted] =
        inflight_recoveries.emplace(sequence, true);

    return inserted;
}

void clear_recovery_inflight(std::uint64_t sequence) {
    std::lock_guard<std::mutex> lock(inflight_mutex);
    inflight_recoveries.erase(sequence);
}

void request_recovery(std::uint64_t sequence) {
    {
        std::lock_guard<std::mutex> lock(request_mutex);
        recovery_requests.push_back(sequence);
    }

    request_cv.notify_one();
}

void recovery_worker() {
    while (true) {
        std::uint64_t sequence;

        {
            std::unique_lock<std::mutex> lock(request_mutex);

            request_cv.wait(lock, [] {
                return recovery_stopping.load()
                    || (!pause_recovery_worker.load() && !recovery_requests.empty());
            });

            if (recovery_stopping.load()) {
                break;
            }

            sequence = recovery_requests.front();
            recovery_requests.pop_front();
        }

        // TCP operations happen outside the UDP receiver thread.
        auto message = recover_sequence(sequence);

        {
            std::lock_guard<std::mutex> lock(result_mutex);

            recovery_results.push_back({
                sequence,
                std::move(message)
            });
        }
    }
}

// ---------------- MAIN RECEIVER ----------------

int main() {
    if (const char* env = std::getenv("TICKWIRE_VERBOSE")) {
        verbose_logging = std::string(env) != "0";
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    if (sock < 0) {
        std::cerr << "socket() failed: "
                  << std::strerror(errno) << '\n';
        return 1;
    }

    configure_udp_receive_buffer(sock, "Feed A");
    if (!configure_multicast_receiver(sock, UDP_PORT, FEED_A_GROUP, "Feed A")) {
        close(sock);
        return 1;
    }
    // Create the UDP socket for Feed B.
    int sock_b = socket(AF_INET, SOCK_DGRAM, 0);

    if (sock_b < 0) {
        std::cerr << "Feed B socket() failed: "
                << std::strerror(errno) << '\n';
        close(sock);
        return 1;
    }

    configure_udp_receive_buffer(sock_b, "Feed B");
    if (!configure_multicast_receiver(sock_b, UDP_PORT_B, FEED_B_GROUP, "Feed B")) {
        close(sock_b);
        close(sock);
        return 1;
    }
    
    std::thread worker(recovery_worker);

    std::optional<std::uint64_t> next_expected = FIRST_SEQUENCE;
    std::map<std::uint64_t, MarketData> pending;
    // Missing sequences whose gaps have already been reported.
    std::set<std::uint64_t> reported_missing;
    std::map<uint64_t, std::chrono::steady_clock::time_point> gap_detected_at;
    // Main-thread-owned retry state.
    std::map<std::uint64_t, int> attempts;
    std::map<
        std::uint64_t,
        std::chrono::steady_clock::time_point
    > retry_after;

    std::map<
    std::uint64_t,
    std::chrono::steady_clock::time_point
    > recovery_started_at;
    std::vector<long long> successful_recovery_latencies_us;

    std::optional<std::chrono::steady_clock::time_point>
    benchmark_start;

    std::chrono::steady_clock::time_point benchmark_end{};

    std::uint64_t processed_count = 0;

    auto last_udp_activity = std::chrono::steady_clock::now();
    auto last_status_check = last_udp_activity;
    auto last_range_attempt = last_udp_activity;
    std::optional<std::uint64_t> publisher_final_sequence;
    bool final_range_loaded = false;
    bool have_attempted_range = false;

    auto queue_recovery = [&](std::uint64_t sequence) {
        const auto now = std::chrono::steady_clock::now();

        if (attempts[sequence] >= MAX_RECOVERY_ATTEMPTS) {
            return;
        }

        auto retry_it = retry_after.find(sequence);

        if (retry_it != retry_after.end()
            && now < retry_it->second) {
            return;
        }

        if (!mark_recovery_inflight(sequence)) {
            return;
        }

        ++attempts[sequence];

        retry_after[sequence] =
            now + std::chrono::milliseconds(RETRY_DELAY_MS);

        if (verbose_logging) {
            std::cout << "[RECOVERY QUEUED] Sequence "
                      << sequence << " (attempt "
                      << attempts[sequence] << ")\n";
        }

        recovery_started_at[sequence] = std::chrono::steady_clock::now();
        request_recovery(sequence);
    };

    

    while (!stop_requested) {
        // 1. Wait briefly for UDP without blocking on TCP recovery.
        pollfd pfds[2]{};

        pfds[0].fd = sock;
        pfds[0].events = POLLIN;

        pfds[1].fd = sock_b;
        pfds[1].events = POLLIN;

        int ready = poll(pfds, 2, POLL_TIMEOUT_MS);

        if (ready < 0 && errno != EINTR) {
            std::cerr << "poll() failed: "
                      << std::strerror(errno) << '\n';
            break;
        }

        // 2. Receive a UDP datagram if available.
        for (int socket_index = 0; socket_index < 2; ++socket_index) {
            if (ready <= 0 ||
                !(pfds[socket_index].revents & POLLIN)) {
                continue;
            }

            const int active_sock = pfds[socket_index].fd;
            const char* feed_name =
                (socket_index == 0) ? "A" : "B";

            // Drain a batch of queued packets from this feed.
            for (int drained = 0; drained < 256; ++drained) {
                Message packet{};

                ssize_t received = recvfrom(
                    active_sock,
                    packet.data(),
                    packet.size(),
                    MSG_TRUNC | MSG_DONTWAIT,
                    nullptr,
                    nullptr
                );

                if (received >= 0) {
                    last_udp_activity = std::chrono::steady_clock::now();
                }

                if (received < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    if (errno == EINTR) {
                        continue;
                    }
                    std::cerr << "recvfrom() failed: "
                              << std::strerror(errno) << '\n';
                    break;
                }
            else if (
                received != static_cast<ssize_t>(packet.size())
            ) {
                std::cerr << "Invalid message size: "
                          << received << " bytes\n";
            } else {
                auto update = decode(packet);
                
                if (!update) {
                    std::cerr
                        << "Invalid market data message\n";
                } else {
                    const std::uint64_t seq = update->sequence;
                    if (!benchmark_start.has_value()) {
                        benchmark_start = std::chrono::steady_clock::now();
                    }
                    
                    if (verbose_logging) {
                        std::cout << "[UDP RECEIVED Feed " << feed_name
                                  << "] Sequence " << seq << '\n';
                    }


                    if (!next_expected.has_value()) {
                        next_expected = seq;
                    }

                    if (seq < *next_expected) {
                        if (verbose_logging) {
                            std::cerr << "Duplicate/Late Sequence "
                                      << seq << '\n';
                        }
                    } else {
                        auto [it, inserted] =
                            pending.emplace(seq, *update);

                        if (!inserted) {
                            if (verbose_logging) {
                                std::cerr << "[DUPLICATE] Sequence "
                                          << seq << " already buffered\n";
                            }
                        } 
                        else if (seq > *next_expected) {
                            const std::uint64_t first_missing = *next_expected;
                            std::uint64_t missing = first_missing;

                            // Track a contiguous range of newly reported missing sequences.
                            std::optional<std::uint64_t> range_start;
                            std::uint64_t range_end = 0;

                            auto flush_gap_log = [&]() {
                                if (range_start.has_value()) {
                                    if (verbose_logging) {
                                        std::cerr << "[GAP DETECTED] Missing "
                                                  << *range_start
                                                  << " through " << range_end
                                                  << '\n';
                                    }

                                    range_start.reset();
                                }
                            };

                            std::vector<std::uint64_t> recovery_candidates;

                            while (
                                missing < seq
                                && missing - first_missing < MAX_GAP_RECOVERY
                            ) {
                                // This sequence has already arrived and is buffered.
                                if (pending.find(missing) != pending.end()) {
                                    flush_gap_log();
                                } else {
                                    // Preserve the existing recovery behavior.
                                    recovery_candidates.push_back(missing);

                                    // Report this missing sequence only once.
                                    const bool newly_reported =
                                        reported_missing.insert(missing).second;

                                    if (newly_reported) {
                                        if (!range_start.has_value()) {
                                            range_start = missing;
                                        }

                                        range_end = missing;
                                    } else {
                                        // End the current range before an already-reported gap.
                                        flush_gap_log();
                                    }
                                }

                                ++missing;
                            }

                            flush_gap_log();
                            for (const auto missing_sequence : recovery_candidates) {
                                gap_detected_at.try_emplace(
                                    missing_sequence,
                                    std::chrono::steady_clock::now()
                                );
                            }
                        }
                    }
                }
            }
        }
    }

        // 3. Drain completed TCP recovery results.
        std::deque<RecoveryResult> completed;

        {
            std::lock_guard<std::mutex> lock(result_mutex);
            completed.swap(recovery_results);
        }

        
        for (auto& result : completed) {
            clear_recovery_inflight(result.sequence);

            // Once END is known, the authoritative range response supersedes
            // any one-at-a-time request that was still completing.
            if (publisher_final_sequence.has_value()) {
                recovery_started_at.erase(result.sequence);
                continue;
            }

            std::optional<long long> elapsed_us;
            auto timing_it = recovery_started_at.find(result.sequence);

            if (timing_it != recovery_started_at.end()) {
                elapsed_us = std::chrono::duration_cast<
                    std::chrono::microseconds
                >(
                    std::chrono::steady_clock::now() - timing_it->second
                ).count();

                recovery_started_at.erase(timing_it);
            }

            if (!result.message) {
                std::cerr << "RECOVERY FAILED Sequence "
                        << result.sequence << '\n';

                if (elapsed_us.has_value()) {
                    std::cout << "[FAILED RECOVERY TIME] Sequence "
                            << result.sequence << ": "
                            << *elapsed_us << " us\n";
                }

                continue;
            }

            if (elapsed_us.has_value()) {
                successful_recovery_latencies_us.push_back(*elapsed_us);

                if (verbose_logging) {
                    std::cout << "[RECOVERY LATENCY] Sequence "
                              << result.sequence << ": "
                              << *elapsed_us << " us\n";
                }
            }

            if (!next_expected.has_value()
                || result.sequence >= *next_expected) {

                auto [it, inserted] = pending.emplace(
                    result.sequence,
                    *result.message
                );

                if (inserted) {
                    if (verbose_logging) {
                        std::cout << "RECOVERED Sequence "
                                  << result.sequence << " via TCP\n";
                    }
                }
            }
        }


        // 4. Retry the current gap if later messages are buffered.
        // if (
        //     next_expected.has_value()
        //     && !pending.empty()
        //     && pending.begin()->first > *next_expected
        //     && pending.find(*next_expected) == pending.end()
        // ) {
        //     queue_recovery(*next_expected);
        // }

        // 5. Process consecutive messages in sequence order.
        if (next_expected.has_value()) {
            while (true) {
                auto current = pending.find(*next_expected);

                if (current == pending.end()) {
                    break;
                }

                const MarketData message = current->second;
                pending.erase(current);

                attempts.erase(message.sequence);
                retry_after.erase(message.sequence);
                reported_missing.erase(message.sequence);
                gap_detected_at.erase(message.sequence);

                if (verbose_logging) {
                    std::cout << "Processed sequence: "
                              << message.sequence << '\n';
                }
                ++processed_count;
                benchmark_end = std::chrono::steady_clock::now();
                // std::cout << "Instrument: "
                //           << message.instrument_id << '\n';
                // std::cout << "Bid (paise): "
                //           << message.bid_price_paise << '\n';
                // std::cout << "Ask (paise): "
                //           << message.ask_price_paise << '\n';
                // std::cout << "Quantity: "
                //           << message.quantity << '\n';

                ++(*next_expected);
            }
        }
 
        // Detect publisher completion after the UDP feeds become idle. This
        // status comes over TCP, so the completion notification itself cannot
        // be lost in overloaded UDP receive queues.
        auto now = std::chrono::steady_clock::now();
        if (!publisher_final_sequence.has_value() &&
            now - last_udp_activity >= END_STATUS_IDLE_PERIOD &&
            now - last_status_check >= END_STATUS_CHECK_INTERVAL) {
            last_status_check = now;
            pause_recovery_worker.store(true);
            request_cv.notify_all();

            const auto final_sequence = query_publisher_final_sequence();
            last_status_check = std::chrono::steady_clock::now();

            if (final_sequence.has_value()) {
                publisher_final_sequence = *final_sequence;
                if (!benchmark_start.has_value()) {
                    benchmark_start = std::chrono::steady_clock::now();
                }
                std::cout << "[PUBLISHER STATUS] End of stream through sequence "
                          << *publisher_final_sequence << '\n';

                // Drop stale one-message requests. One TCP RANGE request will
                // fill every sequence still needed, including a missing tail.
                {
                    std::lock_guard<std::mutex> lock(request_mutex);
                    recovery_requests.clear();
                }
                {
                    std::lock_guard<std::mutex> lock(inflight_mutex);
                    inflight_recoveries.clear();
                }
                recovery_started_at.clear();
                gap_detected_at.clear();
                pause_recovery_worker.store(false);
                request_cv.notify_all();
            } else {
                pause_recovery_worker.store(false);
                request_cv.notify_all();
            }
        }

        // Once the publisher confirms its final sequence, recover the entire
        // remaining interval in one TCP request. Existing UDP/pending messages
        // are harmless duplicates and are ignored by map::emplace.
        now = std::chrono::steady_clock::now();
        if (publisher_final_sequence.has_value() && !final_range_loaded &&
            (!have_attempted_range || now - last_range_attempt >= RANGE_RETRY_INTERVAL)) {
            have_attempted_range = true;
            last_range_attempt = now;

            if (next_expected.has_value() && *next_expected <= *publisher_final_sequence) {
                const auto first_missing_or_pending = *next_expected;
                auto recovered = recover_range(first_missing_or_pending,
                                               *publisher_final_sequence);
                if (recovered.has_value()) {
                    std::size_t inserted_count = 0;
                    for (const auto& message : *recovered) {
                        if (message.sequence >= *next_expected &&
                            pending.emplace(message.sequence, message).second) {
                            ++inserted_count;
                        }
                    }
                    std::cout << "[BULK RECOVERY] Received " << recovered->size()
                              << " messages for sequences " << first_missing_or_pending
                              << "-" << *publisher_final_sequence << " ("
                              << inserted_count << " added to pending)\n";
                    gap_detected_at.clear();
                    reported_missing.clear();
                    attempts.clear();
                    retry_after.clear();
                    final_range_loaded = true;
                } else {
                    std::cerr << "[BULK RECOVERY] Range request failed; will retry\n";
                }
            } else {
                final_range_loaded = true;
            }
        }

        // Continue ordinary per-sequence recovery while the publisher is live.
        // Once END is confirmed, batch recovery above owns the remaining gap.
        if (!publisher_final_sequence.has_value()) {
            now = std::chrono::steady_clock::now();
            for (const auto& [sequence, first_seen] : gap_detected_at) {
                if (!next_expected.has_value() || sequence < *next_expected) {
                    continue;
                }
                if (pending.find(sequence) != pending.end()) {
                    continue;
                }
                if (now - first_seen >= REORDER_GRACE_PERIOD) {
                    queue_recovery(sequence);
                }
            }
        }

        // Finish automatically only after every expected sequence is processed.
        if (publisher_final_sequence.has_value() &&
            final_range_loaded && next_expected.has_value() &&
            *next_expected > *publisher_final_sequence) {
            const std::uint64_t expected_total =
                *publisher_final_sequence - FIRST_SEQUENCE + 1;
            if (processed_count == expected_total) {
                std::cout << "[COMPLETE] Processed all " << processed_count
                          << " messages through sequence "
                          << *publisher_final_sequence << '\n';
                break;
            }
        }
    }

    // 6. Graceful shutdown.
    recovery_stopping.store(true);
    if (!successful_recovery_latencies_us.empty()) {
    auto& samples = successful_recovery_latencies_us;

    std::sort(samples.begin(), samples.end());

    long double total_us = 0.0L;
    for (const auto sample : samples) {
        total_us += sample;
    }

    auto percentile = [&](double p) -> long long {
        const auto rank = static_cast<std::size_t>(
            std::ceil(p * samples.size())
        );

        return samples[std::max<std::size_t>(1, rank) - 1];
    };

    std::cout << "\n=== RECOVERY LATENCY SUMMARY ===\n";
    std::cout << "Successful recoveries: " << samples.size() << '\n';

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Mean: "
              << total_us / samples.size() << " us\n";
    std::cout << "p50: " << percentile(0.50) << " us\n";
    std::cout << "p95: " << percentile(0.95) << " us\n";
    std::cout << "p99: " << percentile(0.99) << " us\n";
    } else {
        std::cout << "\nNo successful recovery samples collected.\n";
    }
    request_cv.notify_all();

    worker.join();
    close(sock);
    close(sock_b);
    if (benchmark_start.has_value() && processed_count > 1) {
    const double elapsed_seconds =
        std::chrono::duration<double>(
            benchmark_end - *benchmark_start
        ).count();

        if (elapsed_seconds > 0.0) {
            const double throughput =
                static_cast<double>(processed_count) / elapsed_seconds;

            std::cout << "\n=== RECEIVER THROUGHPUT ===\n";
            std::cout << "Processed messages: "
                    << processed_count << '\n';

            std::cout << std::fixed << std::setprecision(6);
            std::cout << "Measured elapsed time: "
                    << elapsed_seconds << " s\n";

            std::cout << std::fixed << std::setprecision(2);
            std::cout << "Effective throughput: "
                    << throughput << " messages/second\n";
        }
    }

    std::cout << "TickWire receiver stopped.\n";
    return 0;
}

#include "protocol.hpp"

#include<arpa/inet.h>
#include<array>
#include<cerrno>
#include<cstring>
#include<iostream>
#include<sys/socket.h>
#include<unistd.h>
#include<string>

#include <optional>

#include <map>
#include <cstdint>

//using namespace std;

bool send_all(int fd, const void* data, std::size_t size){
    const auto* bytes  = static_cast<const char*>(data);

    std::size_t done  = 0;

    while (done < size){
        ssize_t n = send(
            fd,
            bytes + done,
            size - done,
            MSG_NOSIGNAL
        );

        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;

        done += static_cast<std::size_t>(n);
    }
    return true;
}

bool recv_all(int fd, void* data, std::size_t size) {
    auto* bytes = static_cast<char*>(data);
    std::size_t done = 0;

    while (done < size) {
        ssize_t n = recv(
            fd,
            bytes + done, 
            size - done, 
            0
        );

        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;

        done += static_cast<std::size_t>(n);
    }

    return true;
}

bool recv_line(int fd, std::string& line){
    line.clear();

    while(line.size() < 128){
        char ch;
        if (!recv_all(fd, &ch, 1)) return false;
        if(ch =='\n') return true;
        
        line.push_back(ch);
    }
    return false;
}

std::optional<MarketData> recover_sequence(std::uint64_t sequence){
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0){
        std::cerr << "Recovery Socket failed: " << std::strerror(errno) << '\n';
        return std::nullopt;
    }
    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(9001);
    server.sin_addr.s_addr =htonl(INADDR_LOOPBACK);

    if (connect(fd, reinterpret_cast<sockaddr*>(&server), sizeof(server)) < 0){
        std::cerr << "Recovery connection failed: " << std::strerror(errno) << '\n';
        close(fd);
        return std::nullopt;
    }
    const std::string request = "REQUEST " + std::to_string(sequence) + "\n";
    if(!send_all(fd, request.data(), request.size())){
        std::cerr << "Failed to send recovery request\n";
        close(fd);
        return std::nullopt;
    }
    std::string response;

    if (!recv_line(fd, response)){
        std::cerr << "Failed to read recovery response\n";
        close(fd);
        return std::nullopt;
    }

    if (response != "OK") {
        std::cerr << "Recovery unavailable for sequence " <<sequence <<": " << response << '\n';
        close(fd);
        return std::nullopt;
    }

    Message packet{};

    if (!recv_all(fd, packet.data(), packet.size())){
        std::cerr << "Incomplete recovered message\n";
        close(fd);
        return std::nullopt;
    }

    close(fd);

    auto update = decode(packet);
    if (!update || update->sequence != sequence){
        std::cerr <<  "Invalid recovered sequence\n";
        return std::nullopt;
    }

    return update;

}

int main(){
    int sock = socket(AF_INET, SOCK_DGRAM, 0); //ipv4 udp socket

    if(sock < 0){
        std::cerr << "socket() failed: " << std::strerror(errno) << "\n";
        return 1;
    }
     //lockal listening address
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(9000);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // bind the socket to port 9000
    if (bind(sock, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0){
        std::cerr <<"bind() failed: " << std::strerror(errno) << '\n';
        close(sock);
        return 1;
    }
    std::cout << "receiver listening on 127.0.0.1:9000\n";

    //stores the sequence number
    std::optional <std::uint64_t> next_expected;

    //Reorder buffer to store the sequence number
    std::map<std::uint64_t, MarketData> pending;

    while(true){
    //recive one datagram
        Message packet{};

        ssize_t received = recvfrom(
            sock,
            packet.data(),
            packet.size(),
            0,
            nullptr,
            nullptr
        );

        if (received < 0){
            std::cerr << "recvfrom() failed: " << strerror(errno) <<'\n';
            close(sock);
            return 1;
        }

        //validate the datagram length
        if (received != static_cast<ssize_t>(packet.size())){
            std::cerr << "Invalid message size: " << received << "bytes\n";
            close(sock);
            return 1;
        }

        //Decode the recived bytes.
        auto update = decode(packet);

        if (!update){
            std::cerr << "Invalid market data message\n";
            close(sock);
            return 1;
        }

        const std::uint64_t seq = update->sequence;

        // Establish the starting sequence.
        if (!next_expected.has_value()){
            next_expected = seq;
        }
        //1. Ignore late / duplicate sequences
        if (seq < *next_expected){
            std::cerr << "Duplicate/Late Seqence " << seq <<'\n';
            continue;
        }
        
        auto [it, inserted] = pending.emplace(seq, *update);

        if (!inserted) {
            std::cerr << "[DUPLICATE] Sequence " << seq << " already buffered\n";
            continue;
        }

        // A future message has arrived before a missing one.
        //2. Out-of-order sequence (future message) -> buffer it
        // Recover missing sequences before processing later messages.
        if (seq > *next_expected){
            std::cerr <<  "[GAP DETECTED] Missing " << *next_expected << " through " << seq - 1 << '\n';
            
            const std::uint64_t first_missing = *next_expected;
            std::uint64_t missing = first_missing;

            //Bound one recovery attept to 1000 messages.
            while(missing < seq && missing  - first_missing < 1000){

                // It may already have arrived through UDP.
                if (pending.find(missing) != pending.end()){
                    ++missing;
                    continue;
                }

                std::cout << "[RECOVERY REQUEST] Sequence "  << missing << '\n';
                auto recovered = recover_sequence(missing);

                if (!recovered){
                    std::cerr << "RECOVERY FAILED Sequence " << missing << '\n';
                    break;
                }
                pending.emplace(missing, *recovered);
                std::cout << "RECOVERED Sequence " << missing << " via TCP\n";
                ++missing;
            }

            
        }
        while (true) {
            auto current = pending.find(*next_expected);

            if (current == pending.end()) {
                break;
            }

            const MarketData message = current->second;
            pending.erase(current);

            std::cout << "Processed sequence: "<< message.sequence << '\n';
            std::cout << "Instrument: " << message.instrument_id << '\n';
            std::cout << "Bid (paise): " << message.bid_price_paise << '\n';
            std::cout << "Ask (paise): " << message.ask_price_paise << '\n';
            std::cout << "Quantity: " << message.quantity << '\n';

            ++(*next_expected);
        }

        // else if (seq == *next_expected){
        //     next_expected = seq + 1;
        // }
        // else if (seq > *next_expected){
        //     //a gap has been detected
        //     std::cerr <<"GAP detected Missing Sequence: " << *next_expected << " through " << seq - 1 <<'\n';
        //     next_expected = seq + 1;
        // }
        // else{
        //     std::cerr << "Duplicate/Late Received Sequence " << seq << ", expected" << *next_expected <<'\n';
        // }

        // MarketData current = *update;
        // while(true){

        //     std::cout << "Processed sequence: " << current.sequence <<'\n';
        //     std::cout << "instrument: " << current.instrument_id << '\n';
        //     std::cout << "Bid price(paise): " << current.bid_price_paise << '\n';
        //     std::cout << "ask price(paise): " << current.ask_price_paise << '\n';
        //     std::cout << "quantity: " << current.quantity <<'\n';
        //     ++(*next_expected);
        //     auto it = pending.find(*next_expected);
        //     if (it == pending.end()){
        //         break;
        //     }
        //     current = it->second;
        //     pending.erase(it);
        // }
        // std::cout << "Received sequence: " << update->sequence <<'\n';
        // std::cout << "instrument: " << update->instrument_id << '\n';
        // std::cout << "Bid price(paise): " << update->bid_price_paise << '\n';
        // std::cout << "ask price(paise): " << update->ask_price_paise << '\n';
        // std::cout << "quantity: " << update->quantity <<'\n';
    }
    close(sock);
    return 0;
}

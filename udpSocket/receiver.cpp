#include "protocol.hpp"

#include<arpa/inet.h>
#include<array>
#include<cerrno>
#include<cstring>
#include<iostream>
#include<sys/socket.h>
#include<unistd.h>

#include <optional>

#include <map>
#include <cstdint>
//using namespace std;

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
            std::cerr << "Duplicate/Late Seqence " << seq << "Discarded"<<'\n';
            continue;
        }

        // A future message has arrived before a missing one.
        //2. Out-of-order sequence (future message) -> buffer it
        if (seq > *next_expected){
            auto [it, inserted] = pending.emplace(seq, *update);

            if (!inserted){
                std::cerr <<"Duplicate Buffered sequence " << seq << "received again\n";
                continue;
            }
            if(pending.size() == 1){
                std::cerr <<"Gap Detected Expected " << *next_expected <<", received " << seq <<'\n';
            }
            std::cout << "Buffer Sequence " << seq <<" waiting for " << *next_expected << '\n';

            continue;
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

        MarketData current = *update;
        while(true){

            std::cout << "Processed sequence: " << current.sequence <<'\n';
            std::cout << "instrument: " << current.instrument_id << '\n';
            std::cout << "Bid price(paise): " << current.bid_price_paise << '\n';
            std::cout << "ask price(paise): " << current.ask_price_paise << '\n';
            std::cout << "quantity: " << current.quantity <<'\n';
            ++(*next_expected);
            auto it = pending.find(*next_expected);
            if (it == pending.end()){
                break;
            }
            current = it->second;
            pending.erase(it);
        }
        // std::cout << "Received sequence: " << update->sequence <<'\n';
        // std::cout << "instrument: " << update->instrument_id << '\n';
        // std::cout << "Bid price(paise): " << update->bid_price_paise << '\n';
        // std::cout << "ask price(paise): " << update->ask_price_paise << '\n';
        // std::cout << "quantity: " << update->quantity <<'\n';
    }
    close(sock);
    return 0;
}

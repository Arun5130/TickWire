#include "protocol.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

bool recv_all(int fd, void* buffer, std::size_t size){
    auto* bytes = static_cast<char*>(buffer);
    std::size_t received = 0;
    while(received < size){
        ssize_t n = recv(
            fd,
            bytes + received,
            size - received,
            0
        );

        if (n < 0 && errno == EINTR){
            continue;
        }
        if (n <= 0){
            return false;
        }
        received += static_cast<std::size_t>(n);
    }
    return true;
}

// Read a newline-terminated response.
bool recv_line(int fd, std::string& line){
    line.clear();

    while(line.size() < 128){
        char ch;

        if (!recv_all(fd, &ch, 1)){
            return false;
        }

        if (ch == '\n'){
            return true;
        }
        line.push_back(ch);
    }
    return false;
}

// Send all bytes, even if send() writes only part.
bool send_all(int fd, const void* buffer, std::size_t size){
    const auto* bytes  = static_cast<const char*>(buffer);
    std::size_t sent = 0;

    while(sent < size){
        ssize_t n = send(
            fd,
            bytes + sent,
            size - sent,
            MSG_NOSIGNAL
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

int main(){
    int sock = socket(AF_INET, SOCK_STREAM,0);

    if (sock < 0){
        std::cerr << "socket() failed: " << std::strerror(errno) <<'\n';
        return 1;
    }
    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(9001);

    if (inet_pton(AF_INET, "127.0.0.1", &server.sin_addr) != 1){
        std::cerr << "Invalid serer address\n";
        close(sock);
        return 1;
    }

    if (connect(sock, reinterpret_cast<sockaddr*>(&server), sizeof(server)) < 0){
        std::cerr << "connect() failed: " << std::strerror(errno) << '\n';
        close(sock);
        return 1;
    }

    //request one specific sequence
    const std::string request = "REQUEST 9999\n";
    if (!send_all(sock,request.data(), request.size())){
        std::cerr << "Failed to send request\n";
        close(sock);
        return 1;
    }

    //read textual response
    std::string response;

    if (!recv_line(sock, response)){
        std::cerr << "Failed to read server response\n";
        close(sock);
        return 1;
    }
    if (response != "OK"){
        std::cout << "Server response: " <<response <<'\n';
        close(sock);
        return response == "NOT_FOUND" ? 0 : 1;
    }

    // Read the exact 32-byte binary message.
    Message packet{};

    if (!recv_all(sock, packet.data(), packet.size())) {
        std::cerr << "Failed to receive full message\n";
        close(sock);
        return 1;
    }

    auto update = decode(packet);

    if (!update) {
        std::cerr << "Failed to decode recovered message\n";
        close(sock);
        return 1;
    }

    std::cout << "Recovery successful\n";
    std::cout << "Sequence: " << update->sequence << '\n';
    std::cout << "Instrument: " << update->instrument_id << '\n';
    std::cout << "Bid (paise): " << update->bid_price_paise << '\n';
    std::cout << "Ask (paise): " << update->ask_price_paise << '\n';
    std::cout << "Quantity: " << update->quantity << '\n';

    close(sock);
    return 0;

}
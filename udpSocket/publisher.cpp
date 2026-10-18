#include "protocol.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

int main(){
    //create udp packet
    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    if (sock < 0){
        std::cerr << "socket() failed: " << std::strerror(errno) << '\n';
        return 1;
    }

    // configure the destination
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(9000);


    if(inet_pton(AF_INET, "127.0.0.1", &destination.sin_addr) != 1){
        std::cerr << "Invalid destination address\n";
        close(sock);
        return 1;
    }

    const std::uint64_t sequences[] ={
        5001,5002,5004,5003,5003,5005
    };
    // create market data and encode it
    for (auto seq : sequences){
        
        // if (seq == 5005) {
        //     continue;
        // }
        MarketData update{
            seq,
            101,
            245025,
            245050,
            200
        };
        Message packet = encode(update);
        //Send packet using
        ssize_t sent = sendto(
            sock,
            packet.data(),
            packet.size(),
            0,
            reinterpret_cast<sockaddr*>(&destination),
            sizeof(destination)
        );
        if (sent < 0){
            std::cerr << "sendto() failed: " << std::strerror(errno) <<'\n';
            close(sock);
            return 1;
        }
    
        if(sent != static_cast<ssize_t>(packet.size())){
            std::cerr << "unexpected number of bytes sent\n";
            close(sock);
            return 1;
        }
        std::cout <<"sent: " << sent << " bytes\n";
        std::cout <<"Sequence " << update.sequence <<'\n';
    }
    
    
    
    close(sock);
    return 0;
}
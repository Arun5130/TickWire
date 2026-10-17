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
        cerr << "socket() failed: " << strerror(errno) << '\n';
        return 1;
    }

    // configure the destination
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(9000);


    if(inet_pton(AF_INET, "127.0.0.1", &destination.sin_addr) != 1){
        cerr << "Invalid destination address\n";
        close(sock);
        return 1;
    }

    // create market data and encode it
    for (uint64_t seq = 5001; seq <= 5010; ++seq){
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
            cerr << "sendto() failed: " << strerror(errno) <<'\n';
            close(sock);
            return 1;
        }
    
        if(sent != static_cast<ssize_t>(packet.size())){
            cerr << "unexpected number of bytes sent\n";
            close(sock);
            return 1;
        }
        cout <<"sent: " << sent << " bytes\n";
        cout <<"Sequence " << update.sequence <<'\n';
    }
    
    
    
    close(sock);
    return 0;
}
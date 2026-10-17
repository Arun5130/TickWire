#include "protocol.hpp"

#include<arpa/inet.h>
#include<array>
#include<cerrno>
#include<cstring>
#include<iostream>
#include<sys/socket.h>
#include<unistd.h>
using namespace std;

int main(){
    int sock = socket(AF_INET, SOCK_DGRAM, 0); //ipv4 udp socket

    if(sock < 0){
        cerr << "socket() failed: " << strerror(errno) << "\n";
        return 1;
    }
     //lockal listening address
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(9000);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // bind the socket to port 9000
    if (bind(sock, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0){
        cerr <<"bind() failed: " << strerror(errno) << '\n';
        close(sock);
        return 1;
    }
    cout << "receiver listning on 127.0.0.1:9000\n";


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
            cerr << "recvfrom() failed: " << strerror(errno) <<'\n';
            close(sock);
            return 1;
        }

        //validate the datagram length
        if (received != static_cast<ssize_t>(packet.size())){
            cerr << "Invalid message size: " << received << "bytes\n";
            close(sock);
            return 1;
        }

        //Decode the recived bytes.
        auto update = decode(packet);

        if (!update){
            cerr << "Invalid market data message\n";
            close(sock);
            return 1;
        }


        cout << "Received sequence: " << update->sequence <<'\n';
        cout << "instrument: " << update->instrument_id << '\n';
        cout << "Bid price(paise): " << update->bid_price_paise << '\n';
        cout << "ask price(paise): " << update->ask_price_paise << '\n';
        cout << "quantity: " << update->quantity <<'\n';
    }
    close(sock);
    return 0;
}

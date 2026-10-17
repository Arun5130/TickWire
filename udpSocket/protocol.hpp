#pragma once
#include<iostream>
#include<cstdint> //fixed width integer type
#include<cstddef>
#include<array>
#include<optional>
#include<span>
#include<type_traits>
#include<cassert>
using namespace std;

//Define a market data message

struct MarketData{
    uint64_t sequence; //Unsigned ,exactly 8 bytes of memory
    uint32_t instrument_id; //4 bytes
    uint64_t bid_price_paise;
    uint64_t ask_price_paise;
    uint32_t quantity;
};

//a fixed-size array containing exactly 32 bytes.
using Message = array<byte, 32>;

//write an unsigned integer in BIG_ENDIAN byte format/order;
template<typename T>
void write_be(Message& out, size_t offset, T value){
    static_assert(is_unsigned_v<T>);

    for(size_t i = 0; i < sizeof(T); ++i){
        const auto shift = 8 * (sizeof(T) -1 - i);
        out[offset + i] = static_cast<byte>((value >> shift) & 0xFFu);
    }
}


//Read an unsigned integer in big endian byte order
template<typename T>
T read_be(span<const byte> data, size_t offset){
    static_assert(is_unsigned_v<T>);
    T value =  0;

    for(size_t i = 0; i < sizeof(T); ++i){
        value = static_cast<T>((value << 8) | to_integer<unsigned int>(data[offset + i]));
        
    }
    return value;
}

//covert to 32 byte formate
Message encode(const MarketData& m){
    Message out{};
    write_be(out, 0 , m.sequence);
    write_be(out, 8, m.instrument_id);
    write_be(out, 12, m.bid_price_paise);
    write_be(out, 20, m.ask_price_paise);
    write_be(out, 28, m.quantity);
    return out;
}

//converting back to c++ object
optional<MarketData> decode(span<const byte> data){
    if(data.size() != 32) return nullopt;
    return MarketData{
        read_be<uint64_t>(data, 0),
        read_be<uint32_t>(data, 8),
        read_be<uint64_t>(data, 12),
        read_be<uint64_t>(data, 20),
        read_be<uint32_t>(data, 28)
    };
}
#include "catch.hpp"
#include "net/server.hpp"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <vector>
#include <stdexcept>
namespace {
struct Running {
    net::Server server;
    std::exception_ptr error;
    std::thread thread;
    explicit Running(net::Config c={}):server(c),thread([this]{try{server.run();}catch(...){error=std::current_exception();}}){}
    ~Running(){server.request_stop();thread.join();}
};
struct Socket {
    int fd;
    Socket(int type,std::uint16_t port):fd(::socket(AF_INET,type,0)) {
        if(fd<0) throw std::runtime_error("socket failed");
        timeval timeout{3,0}; ::setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
        ::setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));
        sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_port=htons(port); addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
        if(::connect(fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr))<0) {::close(fd);throw std::runtime_error("connect failed");}
    }
    ~Socket(){::close(fd);}
    Socket(const Socket&)=delete;
    void send(const std::string& s){std::size_t p=0;while(p<s.size()){auto n=::send(fd,s.data()+p,s.size()-p,MSG_NOSIGNAL);if(n<=0)throw std::runtime_error("send failed");p+=static_cast<std::size_t>(n);}}
    std::string read(std::size_t count){std::string out;char buf[1024];while(out.size()<count){auto n=::recv(fd,buf,std::min(sizeof(buf),count-out.size()),0);if(n<=0)throw std::runtime_error("read failed");out.append(buf,static_cast<std::size_t>(n));}return out;}
};
}
TEST_CASE("ASCII transform preserves binary bytes", "[network]") {
    REQUIRE(net::uppercase("az AZ 19!")=="AZ AZ 19!");
    REQUIRE(net::uppercase(std::string("a\0z\xff",4))==std::string("A\0Z\xff",4));
}
TEST_CASE("Invalid address and limits rejected", "[network]") {
    net::Config c; c.address="bad"; REQUIRE_THROWS(net::Server(c));
    c.address="127.0.0.1"; c.max_clients=0; REQUIRE_THROWS(net::Server(c));
}
TEST_CASE("TCP split frames and pipeline preserve ordering", "[network]") {
    Running r; Socket s(SOCK_STREAM,r.server.port());
    s.send("hel"); s.send("lo\nworld\na\nb\n");
    REQUIRE(s.read(16)=="HELLO\nWORLD\nA\nB\n");
}
TEST_CASE("TCP half close drains complete frames", "[network]") {
    Running r; Socket s(SOCK_STREAM,r.server.port()); s.send("one\ntwo\n");
    REQUIRE(::shutdown(s.fd,SHUT_WR)==0); REQUIRE(s.read(8)=="ONE\nTWO\n");
    char byte; REQUIRE(::recv(s.fd,&byte,1,0)==0);
}
TEST_CASE("TCP boundary frame and oversized frame", "[network]") {
    Running r; Socket s(SOCK_STREAM,r.server.port());
    s.send(std::string(4096,'a')+"\n"); REQUIRE(s.read(4097)==std::string(4096,'A')+"\n");
    s.send(std::string(4097,'b')); char byte; REQUIRE(::recv(s.fd,&byte,1,0)<=0);
}
TEST_CASE("UDP datagrams binary empty and maximum", "[network]") {
    Running r; Socket s(SOCK_DGRAM,r.server.port()); char buf[5000];
    for(const auto& msg:std::vector<std::string>{"hello",std::string("a\0b",3),"",std::string(4096,'z')}) {
        REQUIRE(::send(s.fd,msg.data(),msg.size(),0)==static_cast<ssize_t>(msg.size()));
        auto n=::recv(s.fd,buf,sizeof(buf),0); REQUIRE(n==static_cast<ssize_t>(msg.size()));
        REQUIRE(std::string(buf,static_cast<std::size_t>(n))==net::uppercase(msg));
    }
}
TEST_CASE("UDP oversized datagram dropped without damaging service", "[network]") {
    Running r; Socket s(SOCK_DGRAM,r.server.port());
    s.send(std::string(4097,'x')); s.send("ok"); REQUIRE(s.read(2)=="OK");
}
TEST_CASE("Concurrent TCP and UDP clients", "[network][concurrency]") {
    Running r; std::atomic<int> completed{0}; std::vector<std::thread> threads;
    for(int i=0;i<24;++i) threads.emplace_back([&,i]{try{
        Socket s(i%2?SOCK_STREAM:SOCK_DGRAM,r.server.port());
        for(int j=0;j<40;++j){auto msg="client"+std::to_string(i)+"-"+std::to_string(j)+"\n";s.send(msg);if(s.read(msg.size())!=net::uppercase(msg))return;}
        ++completed;
    }catch(...) {}});
    for(auto& t:threads) { t.join(); }
    REQUIRE(completed==24);
}
TEST_CASE("Disconnected clients cannot receive stale completions", "[network][concurrency]") {
    Running r;
    for(int i=0;i<100;++i){Socket gone(SOCK_STREAM,r.server.port());gone.send("stale\n");}
    Socket live(SOCK_STREAM,r.server.port());live.send("fresh\n");REQUIRE(live.read(6)=="FRESH\n");
}
TEST_CASE("Stop wakes idle epoll and is idempotent", "[network][concurrency]") {
    net::Server server; std::thread t([&]{server.run();}); server.request_stop();server.request_stop();t.join();
    REQUIRE_THROWS_AS(server.run(),std::logic_error);
}
TEST_CASE("Shutdown with active clients completes", "[network][concurrency]") {
    net::Server server; std::thread t([&]{server.run();}); Socket s(SOCK_STREAM,server.port()); s.send("done\n");
    REQUIRE(s.read(5)=="DONE\n"); server.request_stop(); t.join(); char b; REQUIRE(::recv(s.fd,&b,1,0)==0);
}
TEST_CASE("TCP slow reader drains many ordered maximum frames", "[network][concurrency]") {
    Running r; Socket s(SOCK_STREAM,r.server.port());
    std::string input; for(int i=0;i<100;++i) input+=std::string(4096,'a'+i%26)+"\n";
    std::exception_ptr error;
    std::thread sender([&]{try{s.send(input);}catch(...){error=std::current_exception();}});
    std::string output;
    try {output=s.read(input.size());} catch(...) {sender.join(); throw;}
    sender.join(); REQUIRE_FALSE(error); REQUIRE(output==net::uppercase(input));
}
TEST_CASE("Client admission cap rejects excess connections", "[network]") {
    net::Config c;c.max_clients=1;Running r(c);Socket first(SOCK_STREAM,r.server.port());
    first.send("first\n");REQUIRE(first.read(6)=="FIRST\n");
    Socket excess(SOCK_STREAM,r.server.port());char b;REQUIRE(::recv(excess.fd,&b,1,0)==0);
    first.send("still\n");REQUIRE(first.read(6)=="STILL\n");
}
TEST_CASE("Binding an occupied port fails clearly", "[network]") {
    net::Server first;net::Config c;c.port=first.port();REQUIRE_THROWS(net::Server(c));
}
TEST_CASE("Concurrent stop requests are safe during traffic", "[network][concurrency]") {
    net::Server server;std::thread loop([&]{server.run();});
    Socket s(SOCK_STREAM,server.port());s.send("ready\n");REQUIRE(s.read(6)=="READY\n");
    std::vector<std::thread> callers;
    for(int i=0;i<16;++i)callers.emplace_back([&]{server.request_stop();});
    for(auto& t:callers){t.join();}loop.join();
    REQUIRE_THROWS_AS(server.run(),std::logic_error);
}
TEST_CASE("Tiny worker queue survives mixed-client overload", "[network][concurrency]") {
    net::Config c;c.workers=1;c.queue_capacity=1;Running r(c);
    std::vector<std::thread> clients;
    for(int i=0;i<32;++i)clients.emplace_back([&]{try{Socket s(SOCK_STREAM,r.server.port());s.send("load\n");s.read(5);}catch(...) {}});
    for(auto& t:clients){t.join();}
    Socket s(SOCK_STREAM,r.server.port());s.send("after\n");REQUIRE(s.read(6)=="AFTER\n");
}

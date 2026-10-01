#include "net/server.hpp"
#include "scheduler/thread_pool.hpp"
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <unordered_map>
#include <vector>
namespace net {
namespace {
constexpr std::size_t limit = 4096;
struct Fd {
    int value = -1;
    explicit Fd(int v = -1) : value(v) { if (v < 0) throw std::system_error(errno, std::generic_category()); }
    ~Fd() { if(value >= 0) ::close(value); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
};
void check(int rc) { if (rc < 0) throw std::system_error(errno, std::generic_category()); }
void wake(int fd) { std::uint64_t one=1; while (::write(fd,&one,sizeof(one))<0 && errno==EINTR) {} }
}
std::string uppercase(std::string data) {
    // ASCII-only transformation, binary bytes outside a-z are left unchanged.
    for (char& c:data) if(c>='a' && c<='z') c=static_cast<char>(c-'a'+'A');
    return data;
}
struct Server::Impl {
    Config config;
    Fd tcp{::socket(AF_INET,SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC,0)};
    Fd udp{::socket(AF_INET,SOCK_DGRAM|SOCK_NONBLOCK|SOCK_CLOEXEC,0)};
    Fd ep{::epoll_create1(EPOLL_CLOEXEC)};
    Fd notify{::eventfd(0,EFD_NONBLOCK|EFD_CLOEXEC)};
    scheduler::ThreadPool pool;
    std::atomic<bool> stopping{false}, ran{false};
    std::uint16_t bound_port=0;
    struct Client { int fd; std::string input,output; std::size_t sent=0; bool busy=false,eof=false; };
    struct Completion { std::uint64_t id; std::string data; sockaddr_in peer{}; };
    std::unordered_map<std::uint64_t,Client> clients;
    std::uint64_t next=4;
    std::size_t outstanding=0;
    std::mutex completed_mutex;
    std::deque<Completion> completed;
    explicit Impl(Config c):config(std::move(c)),pool(config.workers,config.queue_capacity) {
        if(config.max_clients==0 || config.max_clients>10000) throw std::invalid_argument("max_clients must be 1..10000");
        sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_port=htons(config.port);
        if(::inet_pton(AF_INET,config.address.c_str(),&addr.sin_addr)!=1) throw std::invalid_argument("invalid IPv4 address");
        int yes=1; check(::setsockopt(tcp.value,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes)));
        check(::bind(tcp.value,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)));
        socklen_t size=sizeof(addr); check(::getsockname(tcp.value,reinterpret_cast<sockaddr*>(&addr),&size));
        bound_port=ntohs(addr.sin_port);
        check(::bind(udp.value,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)));
        check(::listen(tcp.value,128));
        add(tcp.value,1,EPOLLIN); add(udp.value,2,EPOLLIN); add(notify.value,3,EPOLLIN);
    }
    ~Impl() { pool.shutdown(); for(auto& entry:clients) ::close(entry.second.fd); }
    void add(int fd,std::uint64_t id,std::uint32_t events) {
        epoll_event e{}; e.data.u64=id; e.events=events; check(::epoll_ctl(ep.value,EPOLL_CTL_ADD,fd,&e));
    }
    void close_client(std::uint64_t id) {
        auto it=clients.find(id); if(it==clients.end()) return;
        ::epoll_ctl(ep.value,EPOLL_CTL_DEL,it->second.fd,nullptr); ::close(it->second.fd); clients.erase(it);
    }
    void interest(std::uint64_t id) {
        auto it=clients.find(id); if(it==clients.end()) return; auto& c=it->second;
        epoll_event e{}; e.data.u64=id; e.events=0;
        if(!c.eof && !stopping && !c.busy && c.output.empty()) e.events|=EPOLLIN|EPOLLRDHUP;
        if(!c.output.empty()) e.events|=EPOLLOUT;
        if(::epoll_ctl(ep.value,EPOLL_CTL_MOD,c.fd,&e)<0) close_client(id);
    }
    bool dispatch(Completion job) {
        try {
            pool.submit([this,job=std::move(job)]() mutable {
                job.data=uppercase(std::move(job.data));
                { std::lock_guard<std::mutex> lock(completed_mutex); completed.push_back(std::move(job)); }
                wake(notify.value);
            });
            ++outstanding; return true;
        } catch(const scheduler::QueueFull&) { return false; }
    }
    void schedule(std::uint64_t id) {
        auto it=clients.find(id); if(it==clients.end()) return; auto& c=it->second;
        if(c.busy || !c.output.empty()) { interest(id); return; }
        auto pos=c.input.find('\n');
        if(pos!=std::string::npos) {
            if(pos>limit) { close_client(id); return; }
            std::string frame=c.input.substr(0,pos+1); c.input.erase(0,pos+1);
            if(!dispatch({id,std::move(frame),{}})) { close_client(id); return; }
            c.busy=true;
        } else if(c.input.size()>limit || c.eof) { close_client(id); return; }
        interest(id);
    }
    void accept_clients() {
        for(int budget=0;budget<64;++budget) {
            int fd=::accept4(tcp.value,nullptr,nullptr,SOCK_NONBLOCK|SOCK_CLOEXEC);
            if(fd<0) { if(errno==EINTR) {--budget;continue;} if(errno==EAGAIN || errno==EWOULDBLOCK) return; throw std::system_error(errno,std::generic_category()); }
            if(clients.size()>=config.max_clients) { ::close(fd); continue; }
            auto id=next++; clients.emplace(id,Client{fd,{},{},0,false,false});
            try { add(fd,id,EPOLLIN|EPOLLRDHUP); } catch(...) { close_client(id); throw; }
        }
    }
    void read_client(std::uint64_t id) {
        auto it=clients.find(id); if(it==clients.end()) return; auto& c=it->second;
        std::array<char,4096> buf{};
        while(c.input.size()<=limit) {
            auto n=::recv(c.fd,buf.data(),buf.size(),0);
            if(n>0) { c.input.append(buf.data(),static_cast<std::size_t>(n)); if(c.input.find('\n')!=std::string::npos) break; }
            else if(n==0) {c.eof=true; break;}
            else if(errno==EINTR) continue;
            else if(errno==EAGAIN || errno==EWOULDBLOCK) break;
            else { close_client(id); return; }
        }
        schedule(id);
    }
    void write_client(std::uint64_t id) {
        auto it=clients.find(id); if(it==clients.end()) return; auto& c=it->second;
        while(c.sent<c.output.size()) {
            auto n=::send(c.fd,c.output.data()+c.sent,c.output.size()-c.sent,MSG_NOSIGNAL);
            if(n>0) c.sent+=static_cast<std::size_t>(n);
            else if(n<0 && errno==EINTR) continue;
            else if(n<0 && (errno==EAGAIN || errno==EWOULDBLOCK)) { interest(id); return; }
            else { close_client(id); return; }
        }
        c.output.clear(); c.sent=0; schedule(id);
    }
    void receive_udp() {
        for(int budget=0;budget<64;++budget) {
            std::array<char,limit+1> buf{}; sockaddr_in peer{}; socklen_t size=sizeof(peer);
            auto n=::recvfrom(udp.value,buf.data(),buf.size(),MSG_TRUNC,reinterpret_cast<sockaddr*>(&peer),&size);
            if(n<0) { if(errno==EINTR) {--budget;continue;} if(errno==EAGAIN || errno==EWOULDBLOCK) return; throw std::system_error(errno,std::generic_category()); }
            if(static_cast<std::size_t>(n)>limit) continue;
            if(outstanding>=config.queue_capacity+config.workers) continue;
            dispatch({0,std::string(buf.data(),static_cast<std::size_t>(n)),peer});
        }
    }
    void completions() {
        std::uint64_t value; while(::read(notify.value,&value,sizeof(value))<0 && errno==EINTR) {}
        std::deque<Completion> ready;
        { std::lock_guard<std::mutex> lock(completed_mutex); ready.swap(completed); }
        for(auto& job:ready) {
            --outstanding;
            if(job.id==0) {
                // UDP is best effort. A nonblocking send failure drops the response.
                while(::sendto(udp.value,job.data.data(),job.data.size(),MSG_NOSIGNAL,reinterpret_cast<sockaddr*>(&job.peer),sizeof(job.peer))<0 && errno==EINTR) {}
            } else {
                auto it=clients.find(job.id); if(it==clients.end()) continue;
                it->second.busy=false; it->second.output=std::move(job.data); write_client(job.id);
            }
        }
    }
    void run() {
        if(ran.exchange(true)) throw std::logic_error("run is single-use");
        bool draining=false; std::chrono::steady_clock::time_point deadline{};
        std::array<epoll_event,64> events{};
        for(;;) {
            if(stopping && !draining) {
                draining=true; deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
                ::epoll_ctl(ep.value,EPOLL_CTL_DEL,tcp.value,nullptr);
                ::epoll_ctl(ep.value,EPOLL_CTL_DEL,udp.value,nullptr);
                // Drain complete frames already read, not arbitrary unread socket bytes.
                std::vector<std::uint64_t> ids; for(auto& e:clients) { e.second.eof=true; ids.push_back(e.first); }
                for(auto id:ids) schedule(id);
            }
            if(draining && ((clients.empty() && outstanding==0) || std::chrono::steady_clock::now()>=deadline)) break;
            auto n=::epoll_wait(ep.value,events.data(),static_cast<int>(events.size()),draining?50:-1);
            if(n<0) { if(errno==EINTR) continue; throw std::system_error(errno,std::generic_category()); }
            for(int i=0;i<n;++i) {
                auto id=events[i].data.u64; auto flags=events[i].events;
                if(id==1) { if(!stopping) accept_clients(); }
                else if(id==2) { if(!stopping) receive_udp(); }
                else if(id==3) completions();
                else {
                    if(flags&EPOLLERR) { close_client(id); continue; }
                    // recv() detects half-close after pending bytes; don't discard them on RDHUP.
                    if(!stopping && (flags&(EPOLLIN|EPOLLRDHUP|EPOLLHUP))) {
                        auto it=clients.find(id); if(it!=clients.end() && !it->second.busy && it->second.output.empty()) read_client(id);
                    }
                    if(flags&EPOLLOUT) write_client(id);
                }
            }
        }
        pool.shutdown();
        while(!clients.empty()) close_client(clients.begin()->first);
        completions();
    }
};
Server::Server(Config c):impl_(std::make_unique<Impl>(std::move(c))) {}
Server::~Server()=default;
std::uint16_t Server::port() const {return impl_->bound_port;}
void Server::run(){impl_->run();}
void Server::request_stop(){impl_->stopping=true; wake(impl_->notify.value);}
}

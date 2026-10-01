#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
namespace net {
struct Config {
    std::string address = "127.0.0.1";
    std::uint16_t port = 0; // 0 chooses an ephemeral port, shared by TCP/UDP
    std::size_t workers = 4, queue_capacity = 256, max_clients = 256;
};
// Linux-only IPv4 uppercase echo service. run() has exactly one caller.
// request_stop() is thread-safe; join run() before destruction.
class Server {
public:
    explicit Server(Config config = {});
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    std::uint16_t port() const;
    void run();
    void request_stop();
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
std::string uppercase(std::string data);
}

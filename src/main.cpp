#include "net/server.hpp"
#include <pthread.h>
#include <signal.h>
#include <iostream>
#include <thread>
int main(int argc,char** argv) {
    try {
        net::Config config;
        if(argc>3) throw std::invalid_argument("usage: network_server [port] [IPv4-address]");
        if(argc>=2) {
            std::string arg=argv[1]; std::size_t used=0; auto n=std::stoul(arg,&used);
            if(used!=arg.size() || n>65535 || arg.empty() || arg[0]=='-') throw std::invalid_argument("port must be 0..65535");
            config.port=static_cast<std::uint16_t>(n);
        }
        if(argc==3) config.address=argv[2];
        sigset_t signals; sigemptyset(&signals); sigaddset(&signals,SIGINT); sigaddset(&signals,SIGTERM);
        if(pthread_sigmask(SIG_BLOCK,&signals,nullptr)!=0) throw std::runtime_error("signal mask failed");
        net::Server server(config); std::exception_ptr error;
        const auto owner=pthread_self();
        std::thread loop([&]{try{server.run();}catch(...){error=std::current_exception(); pthread_kill(owner,SIGTERM);}});
        std::cout<<"READY "<<config.address<<":"<<server.port()<<" TCP/UDP"<<std::endl;
        // SIGINT/SIGTERM remain blocked in all threads; sigwait handles them safely.
        int signal=0; sigwait(&signals,&signal); server.request_stop(); loop.join();
        if(error) std::rethrow_exception(error);
        std::cout<<"STOPPED"<<std::endl;
    } catch(const std::exception& e){ std::cerr<<e.what()<<'\n'; return 1; }
}

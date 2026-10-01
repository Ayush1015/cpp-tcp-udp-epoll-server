#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "scheduler/thread_pool.hpp"
#include <atomic>
#include <chrono>
#include <memory>
#include <numeric>
#include <string>

using scheduler::ThreadPool;
using namespace std::chrono_literals;

TEST_CASE("Configuration rejects zero values", "[configuration]") {
    REQUIRE_THROWS_AS(ThreadPool(0, 1), std::invalid_argument);
    REQUIRE_THROWS_AS(ThreadPool(1, 0), std::invalid_argument);
}
TEST_CASE("Futures return values and support move-only jobs", "[futures]") {
    ThreadPool pool(2, 8);
    auto value = pool.submit([] { return 42; });
    auto moved = pool.submit([p = std::make_unique<int>(7)] { return *p; });
    auto nothing = pool.submit([] {});
    REQUIRE(value.get() == 42);
    REQUIRE(moved.get() == 7);
    REQUIRE_NOTHROW(nothing.get());
}
TEST_CASE("Exceptions reach the future without killing a worker", "[futures]") {
    ThreadPool pool(1, 8);
    auto failed = pool.submit([]() -> int { throw std::runtime_error("job failed"); });
    auto good = pool.submit([] { return 9; });
    REQUIRE_THROWS_WITH(failed.get(), "job failed");
    REQUIRE(good.get() == 9);
    pool.wait_idle();
    REQUIRE(pool.stats().completed == 2); // Completed includes jobs returning exceptions.
}
TEST_CASE("Queue capacity excludes active jobs and rejects overflow", "[bounds]") {
    ThreadPool pool(1, 2);
    std::promise<void> release, started;
    auto gate = release.get_future().share();
    auto start = started.get_future();
    auto running = pool.submit([&] { started.set_value(); gate.wait(); });
    const bool ready = start.wait_for(2s) == std::future_status::ready;
    CHECK(ready);
    if (!ready) { release.set_value(); return; }
    auto queued1 = pool.submit([] {});
    auto queued2 = pool.submit([] {});
    CHECK_THROWS_AS(pool.submit([] {}), scheduler::QueueFull);
    CHECK(pool.stats().queued == 2);
    CHECK(pool.stats().active == 1);
    CHECK(pool.stats().submitted == 3);
    release.set_value();
    running.get(); queued1.get(); queued2.get();
    pool.wait_idle();
    REQUIRE(pool.stats().completed == 3);
}
TEST_CASE("Single worker dequeues jobs in FIFO order", "[ordering]") {
    ThreadPool pool(1, 100);
    std::vector<int> seen;
    for (int i = 0; i < 100; ++i) { pool.submit([&, i] { seen.push_back(i); }); }
    pool.wait_idle();
    std::vector<int> expected(100);
    std::iota(expected.begin(), expected.end(), 0);
    REQUIRE(seen == expected);
}
TEST_CASE("Idle workers wake and multiple workers execute concurrently", "[concurrency]") {
    ThreadPool pool(4, 8);
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::atomic<int> started{0};
    for (int i = 0; i < 4; ++i) {
        pool.submit([&] {
            ++started;
            gate.wait();
        });
    }
    bool concurrent = false;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (started.load() == 4) { concurrent = true; break; }
        std::this_thread::yield();
    }
    release.set_value();
    pool.wait_idle();
    REQUIRE(concurrent);
    REQUIRE(pool.stats().completed == 4);
}
TEST_CASE("Concurrent producers execute every job exactly once", "[concurrency][race]") {
    constexpr int producers = 8, per_producer = 250, total = producers * per_producer;
    ThreadPool pool(6, total);
    std::vector<std::atomic<int>> counts(total);
    for (auto& count : counts) { count.store(0); }
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            for (int j = 0; j < per_producer; ++j) {
                const int id = p * per_producer + j;
                try { pool.submit([&, id] { counts[id].fetch_add(1); }); }
                catch (...) { ++errors; }
            }
        });
    }
    for (auto& thread : threads) { thread.join(); }
    pool.wait_idle();
    REQUIRE(errors.load() == 0);
    for (const auto& count : counts) { REQUIRE(count.load() == 1); }
    const auto s = pool.stats();
    REQUIRE(s.submitted == total);
    REQUIRE(s.completed == total);
    REQUIRE(s.queued == 0);
    REQUIRE(s.active == 0);
}
TEST_CASE("Shutdown drains accepted jobs and rejects subsequent submissions", "[shutdown]") {
    ThreadPool pool(3, 200);
    std::atomic<int> count{0};
    for (int i = 0; i < 200; ++i) { pool.submit([&] { ++count; }); }
    pool.shutdown();
    REQUIRE(count == 200);
    REQUIRE(pool.stats().completed == 200);
    REQUIRE_FALSE(pool.stats().accepting);
    REQUIRE_THROWS_AS(pool.submit([] {}), scheduler::PoolStopped);
    REQUIRE_NOTHROW(pool.shutdown());
    REQUIRE_NOTHROW(pool.wait_idle());
}
TEST_CASE("Shutdown on an empty pool wakes sleeping workers", "[shutdown]") {
    ThreadPool pool(4, 2);
    pool.shutdown();
    REQUIRE(pool.stats().completed == 0);
    REQUIRE_FALSE(pool.stats().accepting);
}
TEST_CASE("Destructor drains unfinished work", "[shutdown]") {
    std::atomic<int> count{0};
    {
        ThreadPool pool(2, 100);
        for (int i = 0; i < 100; ++i) { pool.submit([&] { ++count; }); }
    }
    REQUIRE(count == 100);
}
TEST_CASE("Shutdown and submission race has no lost accepted jobs", "[concurrency][race][shutdown]") {
    ThreadPool pool(4, 1000);
    std::atomic<int> accepted{0}, executed{0}, stopped{0}, unexpected{0};
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::vector<std::thread> producers;
    for (int p = 0; p < 4; ++p) {
        producers.emplace_back([&] {
            gate.wait();
            for (int j = 0; j < 250; ++j) {
                try { pool.submit([&] { ++executed; }); ++accepted; }
                catch (const scheduler::PoolStopped&) { ++stopped; }
                catch (...) { ++unexpected; }
            }
        });
    }
    std::thread closer([&] { gate.wait(); pool.shutdown(); });
    release.set_value();
    for (auto& producer : producers) { producer.join(); }
    closer.join();
    REQUIRE(unexpected == 0);
    REQUIRE(accepted.load() + stopped.load() == 1000);
    REQUIRE(executed == accepted.load());
    REQUIRE(pool.stats().completed == static_cast<std::size_t>(accepted.load()));
}
TEST_CASE("Concurrent shutdown calls safely serialize joins", "[concurrency][shutdown]") {
    ThreadPool pool(4, 100);
    for (int i = 0; i < 100; ++i) { pool.submit([] {}); }
    std::thread a([&] { pool.shutdown(); });
    std::thread b([&] { pool.shutdown(); });
    a.join(); b.join();
    REQUIRE(pool.stats().completed == 100);
}
TEST_CASE("Worker cannot deadlock by waiting for itself or joining itself", "[shutdown]") {
    ThreadPool pool(1, 8);
    auto wait = pool.submit([&] { pool.wait_idle(); });
    auto shutdown = pool.submit([&] { pool.shutdown(); });
    REQUIRE_THROWS_AS(wait.get(), std::logic_error);
    REQUIRE_THROWS_AS(shutdown.get(), std::logic_error);
    pool.wait_idle();
    REQUIRE(pool.stats().completed == 2);
}
TEST_CASE("Stats maintain accounting under concurrent reads", "[concurrency]") {
    ThreadPool pool(4, 1000);
    std::atomic<bool> bad{false}, done{false};
    std::thread observer([&] {
        while (!done.load()) {
            const auto s = pool.stats();
            if (s.submitted != s.completed + s.active + s.queued) { bad = true; }
        }
    });
    for (int i = 0; i < 1000; ++i) { pool.submit([] {}); }
    pool.wait_idle();
    done = true;
    observer.join();
    REQUIRE_FALSE(bad.load());
    REQUIRE(pool.stats().completed == 1000);
}

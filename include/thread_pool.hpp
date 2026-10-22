#pragma once
// Fixed-size worker pool built directly on POSIX threads.
//
//   * One mutex (mu_) protects the task queue and the stop flag.
//   * One condition variable (not_empty_) wakes idle workers.
//   * The queue is BOUNDED: when it is full submit() fails immediately and the
//     caller sheds load instead of letting memory / latency grow without limit.
//   * Tasks run with the lock released, and no other lock is ever acquired
//     while mu_ is held => no lock-ordering cycles => no deadlock.

#include <pthread.h>

#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "logger.hpp"

class ThreadPool {
public:
    using Task = std::function<void()>;

    ThreadPool(size_t num_threads, size_t max_queue) : max_queue_(max_queue) {
        pthread_mutex_init(&mu_, nullptr);
        pthread_cond_init(&not_empty_, nullptr);
        threads_.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            pthread_t tid;
            if (pthread_create(&tid, nullptr, &ThreadPool::worker_entry, this) != 0) {
                shutdown();  // join the workers we already started
                pthread_cond_destroy(&not_empty_);
                pthread_mutex_destroy(&mu_);
                throw std::runtime_error("pthread_create failed");
            }
            threads_.push_back(tid);
        }
    }

    ~ThreadPool() {
        shutdown();
        pthread_cond_destroy(&not_empty_);
        pthread_mutex_destroy(&mu_);
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Returns false if the pool is stopping or the queue is full (back-pressure).
    bool submit(Task task) {
        pthread_mutex_lock(&mu_);
        if (stop_ || queue_.size() >= max_queue_) {
            pthread_mutex_unlock(&mu_);
            return false;
        }
        queue_.push_back(std::move(task));
        pthread_mutex_unlock(&mu_);
        pthread_cond_signal(&not_empty_);
        return true;
    }

    size_t queued() {
        pthread_mutex_lock(&mu_);
        size_t n = queue_.size();
        pthread_mutex_unlock(&mu_);
        return n;
    }

    // Stop accepting tasks, let workers drain what is already queued, join them.
    // Idempotent.
    void shutdown() {
        std::vector<pthread_t> to_join;
        pthread_mutex_lock(&mu_);
        stop_ = true;
        to_join.swap(threads_);
        pthread_mutex_unlock(&mu_);
        pthread_cond_broadcast(&not_empty_);
        for (pthread_t t : to_join) pthread_join(t, nullptr);
    }

private:
    static void* worker_entry(void* self) {
        static_cast<ThreadPool*>(self)->worker_loop();
        return nullptr;
    }

    void worker_loop() {
        for (;;) {
            Task task;
            pthread_mutex_lock(&mu_);
            // Loop, not "if": guards against spurious wake-ups.
            while (queue_.empty() && !stop_) pthread_cond_wait(&not_empty_, &mu_);
            if (queue_.empty()) {  // stop_ is set and nothing is left to drain
                pthread_mutex_unlock(&mu_);
                return;
            }
            task = std::move(queue_.front());
            queue_.pop_front();
            pthread_mutex_unlock(&mu_);

            try {
                task();
            } catch (const std::exception& e) {
                LOG_ERROR("worker task threw: %s", e.what());
            } catch (...) {
                LOG_ERROR("worker task threw an unknown exception");
            }
        }
    }

    pthread_mutex_t mu_;
    pthread_cond_t not_empty_;
    std::deque<Task> queue_;
    std::vector<pthread_t> threads_;
    const size_t max_queue_;
    bool stop_ = false;
};

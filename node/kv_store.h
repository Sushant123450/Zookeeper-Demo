#pragma once
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

class KVStore {
private:
    std::unordered_map<std::string, std::string> store;
    std::mutex mtx;

public:
    void put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mtx);
        store[key] = value;
    }

    std::optional<std::string> get(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = store.find(key);
        if (it != store.end()) {
            return it->second;
        }
        return std::nullopt;
    }
};

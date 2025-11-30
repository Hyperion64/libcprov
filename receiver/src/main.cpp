#include <future>
#include <iostream>
#include <mutex>
#include <string>

#include "logserver.hpp"
#include "model.hpp"
#include "parser.hpp"
#include "processor.hpp"

int main() {
    std::mutex data_mutex;
    ParsedRequestQueue parsed_requests;
    std::string url = "127.0.0.1";
    int port = 9000;
    LogServer server(url, port);
    auto fut = std::async(std::launch::async, process_parsed_requests,
                          &parsed_requests);
    server.set_log_handler(
        [&](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lock(data_mutex);
                parsed_requests.push(parse_request(req.body));
            }
            std::cerr << "[http] POST /log size=" << req.body.size() << "\n";
            std::cerr << req.body << "\n";
            res.set_content("{\"status\":\"ok\"}", "application/json");
        });
    server.run(4);
    return 0;
}

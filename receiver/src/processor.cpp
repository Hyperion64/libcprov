#include <iostream>
#include <model.hpp>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

// tmp debug output
void print_set(const std::unordered_set<std::string>& s,
               const std::string& name) {
    std::cout << name << ": { ";
    for (const auto& item : s) {
        std::cout << item << " ";
    }
    std::cout << "}" << std::endl;
}

void print_prov_data(const ProvData& pd) {
    print_set(pd.reads, "Reads");
    print_set(pd.writes, "Writes");
    print_set(pd.executes, "Executes");
}

void print_processed_job_data(const ProcessedJobData& job) {
    std::cout << "Job ID: " << job.job_id << "\n"
              << "Cluster: " << job.cluster_name << "\n"
              << "Job Name: " << job.job_name << "\n"
              << "Path: " << job.path << "\n"
              << "Start Time: " << job.start_time << "\n"
              << "End Time: " << job.end_time << "\n";

    print_prov_data(job.global_prov_data);
}

//

std::string getFilePath(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& rename_map) {
    if (auto it = rename_map.find(path); it != rename_map.end()) {
        return it->second;
    }
    return path;
}

void process_exec(const Exec& exec, ProcessedJobData& processed_job_data) {
    std::unordered_map<std::string, std::string>& global_rename_map
        = processed_job_data.global_rename_map;
    std::unordered_set<std::string>& global_reads
        = processed_job_data.global_prov_data.reads;
    std::unordered_set<std::string>& global_writes
        = processed_job_data.global_prov_data.writes;
    std::unordered_set<std::string>& global_executes
        = processed_job_data.global_prov_data.executes;
    std::queue<Event> events = exec.events;
    while (!events.empty()) {
        Event event = events.front();
        SysOp op = event.operation;
        EventPayload event_payload = event.event_payload;
        switch (op) {
            case SysOp::ProcessStart:
            case SysOp::ProcessEnd:
                break;
            case SysOp::Write:
            case SysOp::Writev:
            case SysOp::Pwrite:
            case SysOp::Pwritev:
            case SysOp::Truncate:
            case SysOp::Fallocate: {
                AccessOut access_out = std::get<AccessOut>(event_payload);
                std::string path_out
                    = getFilePath(access_out.path_out, global_rename_map);
                global_writes.insert(path_out);
                break;
            }
            case SysOp::Read:
            case SysOp::Readv:
            case SysOp::Pread:
            case SysOp::Preadv: {
                const auto& access_in = std::get<AccessIn>(event_payload);
                std::string path_in
                    = getFilePath(access_in.path_in, global_rename_map);
                global_reads.insert(path_in);
                break;
            }
            case SysOp::Transfer: {
                const auto& access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out
                    = getFilePath(access_in_out.path_out, global_rename_map);
                global_writes.insert(path_out);
                std::string path_in
                    = getFilePath(access_in_out.path_in, global_rename_map);
                global_reads.insert(path_in);
                break;
            }
            case SysOp::Rename: {
                const auto& access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out
                    = getFilePath(access_in_out.path_out, global_rename_map);
                std::string path_in
                    = getFilePath(access_in_out.path_in, global_rename_map);
                if (global_rename_map.find(path_in)
                    == global_rename_map.end()) {
                    global_rename_map[path_out] = path_in;
                } else {
                    global_rename_map[path_out] = global_rename_map[path_in];
                    global_rename_map.erase(path_in);
                }
                break;
            }
            case SysOp::Link: {
                const auto& access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out
                    = getFilePath(access_in_out.path_out, global_rename_map);
                std::string path_in
                    = getFilePath(access_in_out.path_in, global_rename_map);
                global_rename_map[path_in] = path_out;
                global_rename_map[path_out] = path_in;
                global_writes.insert(path_out);
                break;
            }
            case SysOp::SymLink: {
                const auto& access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out
                    = getFilePath(access_in_out.path_out, global_rename_map);
                std::string path_in
                    = getFilePath(access_in_out.path_in, global_rename_map);
                global_rename_map[path_out] = path_in;
                global_writes.insert(path_out);
                break;
            }
            case SysOp::Unlink: {
                const auto& access_out = std::get<AccessOut>(event_payload);
                std::string path_out
                    = getFilePath(access_out.path_out, global_rename_map);
                global_rename_map.erase(path_out);
                break;
            }
            case SysOp::Exec:
            case SysOp::System: {
                const auto& access_exec = std::get<ExecCall>(event_payload);
                std::string path_exec
                    = getFilePath(access_exec.target, global_rename_map);
                global_executes.insert(path_exec);
                break;
            }
            case SysOp::Spawn: {
                const auto& access_spawn = std::get<SpawnCall>(event_payload);
                std::string path_spawn
                    = getFilePath(access_spawn.target, global_rename_map);
                global_executes.insert(path_spawn);
                break;
            }
            case SysOp::Fork:
            default:
                break;
        }
        events.pop();
    }
}

void process_parsed_requests(ParsedRequestQueue* parsed_request) {
    std::unordered_map<std::string, ProcessedJobData> processed_job_data_map;
    while (true) {
        std::queue<ParsedRequest> request_copy = parsed_request->take_all();
        while (!request_copy.empty()) {
            ParsedRequest request_copy_element = request_copy.front();
            std::string job_id = request_copy_element.job_id;
            std::string cluster_name = request_copy_element.cluster_name;
            std::string prov_data_key = job_id + cluster_name;
            if (request_copy_element.type == CallType::Start) {
                std::string path = request_copy_element.path;
                StartOrEnd start = std::get<StartOrEnd>(
                    request_copy_element.request_payload);
                ProcessedJobData new_processed_prov_data
                    = {.job_id = job_id,
                       .cluster_name = cluster_name,
                       .path = path,
                       .start_time = start.ts};
                processed_job_data_map[prov_data_key] = new_processed_prov_data;
            } else if (request_copy_element.type == CallType::End) {
                StartOrEnd end = std::get<StartOrEnd>(
                    request_copy_element.request_payload);
                processed_job_data_map[prov_data_key].end_time = end.ts;
                print_processed_job_data(processed_job_data_map[prov_data_key]);
            } else if (request_copy_element.type == CallType::Exec) {
                Exec exec
                    = std::get<Exec>(request_copy_element.request_payload);
                process_exec(exec, processed_job_data_map[prov_data_key]);
            }
            request_copy.pop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

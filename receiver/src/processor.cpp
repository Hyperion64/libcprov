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

struct RecordParameters {
    const std::unordered_map<std::string, std::string>& global_map;
    const std::unordered_map<std::string, std::string>& exec_map;
    ProvData& global;
    ProvData& exec;
    ProvData& proc;
};

std::string resolve_path(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& map) {
    auto it = map.find(path);
    return (it != map.end()) ? it->second : path;
}

void record_write(const std::string& path, RecordParameters record_parameters) {
    std::string g = resolve_path(path, record_parameters.global_map);
    std::string e = resolve_path(path, record_parameters.exec_map);
    record_parameters.global.writes.insert(g);
    record_parameters.exec.writes.insert(e);
    record_parameters.proc.writes.insert(e);
}

void record_read(const std::string& path, RecordParameters record_parameters) {
    std::string g = resolve_path(path, record_parameters.global_map);
    std::string e = resolve_path(path, record_parameters.exec_map);
    record_parameters.global.reads.insert(g);
    record_parameters.exec.reads.insert(e);
    record_parameters.proc.reads.insert(e);
}

void process_exec(const Exec& exec, ProcessedJobData& processed_job_data) {
    ExecProvData current_exec_prov_data;
    ProvData& global_prov_data = processed_job_data.global_prov_data;
    ProvData& exec_prov_data = current_exec_prov_data.prov_data;
    std::unordered_map<std::string, std::string>& global_rename_map
        = processed_job_data.global_rename_map;
    std::unordered_map<std::string, std::string>& exec_rename_map
        = current_exec_prov_data.rename_map;
    std::queue<Event> events = exec.events;
    ProvData empty_proc;
    RecordParameters record_parameters = {.global_map = global_rename_map,
                                          .exec_map = exec_rename_map,
                                          .global = global_prov_data,
                                          .exec = exec_prov_data,
                                          .proc = empty_proc};
    while (!events.empty()) {
        Event event = events.front();
        uint64_t event_pid = event.pid;
        SysOp op = event.operation;
        EventPayload event_payload = event.event_payload;
        ProcessProvData& current_process_prov_data
            = current_exec_prov_data.process_map[event_pid];
        record_parameters.proc = current_process_prov_data.prov_data;
        switch (op) {
            case SysOp::ProcessStart: {
                ProcessStart process_start
                    = std::get<ProcessStart>(event_payload);
                uint64_t ppid = process_start.ppid;
                current_process_prov_data.start_time = event.ts;
                current_process_prov_data.ppid = ppid;
                break;
            }
            case SysOp::ProcessEnd: {
                current_process_prov_data.end_time = event.ts;
                break;
            }
            case SysOp::Write:
            case SysOp::Writev:
            case SysOp::Pwrite:
            case SysOp::Pwritev:
            case SysOp::Truncate:
            case SysOp::Fallocate: {
                AccessOut access_out = std::get<AccessOut>(event_payload);
                std::string path_out = access_out.path_out;
                record_write(path_out, record_parameters);
                break;
            }
            case SysOp::Read:
            case SysOp::Readv:
            case SysOp::Pread:
            case SysOp::Preadv: {
                AccessIn access_in = std::get<AccessIn>(event_payload);
                std::string path_in = access_in.path_in;
                record_read(path_in, record_parameters);
                break;
            }
            case SysOp::Transfer: {
                AccessInOut access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out = access_in_out.path_out;
                std::string path_in = access_in_out.path_in;
                record_write(path_out, record_parameters);
                record_read(path_in, record_parameters);
                break;
            }
            case SysOp::Rename: {
                const auto& access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out
                    = resolve_path(access_in_out.path_out, global_rename_map);
                std::string path_in
                    = resolve_path(access_in_out.path_in, global_rename_map);
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
                    = resolve_path(access_in_out.path_out, global_rename_map);
                std::string path_in
                    = resolve_path(access_in_out.path_in, global_rename_map);
                global_rename_map[path_in] = path_out;
                global_rename_map[path_out] = path_in;
                record_write(path_out, record_parameters);
                break;
            }
            case SysOp::SymLink: {
                const auto& access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out
                    = resolve_path(access_in_out.path_out, global_rename_map);
                std::string path_in
                    = resolve_path(access_in_out.path_in, global_rename_map);
                global_rename_map[path_out] = path_in;
                record_write(path_out, record_parameters);
                break;
            }
            case SysOp::Unlink: {
                const auto& access_out = std::get<AccessOut>(event_payload);
                std::string path_out
                    = resolve_path(access_out.path_out, global_rename_map);
                global_rename_map.erase(path_out);
                break;
            }
            case SysOp::Exec:
            case SysOp::System: {
                const auto& access_exec = std::get<ExecCall>(event_payload);
                std::string path_exec
                    = resolve_path(access_exec.target, global_rename_map);
                global_prov_data.executes.insert(path_exec);
                break;
            }
            case SysOp::Spawn: {
                const auto& access_spawn = std::get<SpawnCall>(event_payload);
                std::string path_spawn
                    = resolve_path(access_spawn.target, global_rename_map);
                global_prov_data.executes.insert(path_spawn);
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

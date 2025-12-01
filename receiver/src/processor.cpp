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

void print_exec_data(const ExecProvData& exec) {
    std::cout << "Exec Step Name: " << exec.step_name << "\n"
              << "Start Time: " << exec.start_time << "\n"
              << "End Time: " << exec.end_time << "\n";
    print_prov_data(exec.prov_data);

    if (!exec.rename_map.empty()) {
        std::cout << "Rename Map: { ";
        for (const auto& [k, v] : exec.rename_map) {
            std::cout << k << "->" << v << " ";
        }
        std::cout << "}" << std::endl;
    }

    if (!exec.symlink_map.empty()) {
        std::cout << "Symlink Map: { ";
        for (const auto& [k, v] : exec.symlink_map) {
            std::cout << k << "->" << v << " ";
        }
        std::cout << "}" << std::endl;
    }

    std::cout << "-------------------------------------" << std::endl;
}

void print_processed_job_data(const ProcessedJobData& job) {
    std::cout << "Job ID: " << job.job_id << "\n"
              << "Cluster: " << job.cluster_name << "\n"
              << "Job Name: " << job.job_name << "\n"
              << "Path: " << job.path << "\n"
              << "Start Time: " << job.start_time << "\n"
              << "End Time: " << job.end_time << "\n";

    std::queue<ExecProvData> exec_queue_copy = job.exec_prov_data_queue;
    while (!exec_queue_copy.empty()) {
        const ExecProvData& exec = exec_queue_copy.front();
        print_exec_data(exec);
        exec_queue_copy.pop();
    }
}

//

struct RecordParameters {
    // std::unordered_map<std::string, std::string> global_map;
    std::unordered_map<std::string, std::string> exec_rename_map;
    std::unordered_map<std::string, std::string> exec_symlink_map;
    // ProvData& global;
    ProvData& exec_prov_data;
    // ProvData& proc;
};

void rename_writes(RecordParameters& record_parameters) {
    std::unordered_set<std::string>& writes
        = record_parameters.exec_prov_data.writes;
    for (const std::pair<std::string, std::string>& kv :
         record_parameters.exec_rename_map) {
        const std::string& new_file_name = kv.first;
        const std::string& original_file_name = kv.second;
        std::unordered_set<std::string>::const_iterator it
            = writes.find(original_file_name);
        if (it != writes.end()) {
            writes.erase(it);
            writes.insert(new_file_name);
        }
    }
}

std::string resolve_path(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& rename_map,
    const std::unordered_map<std::string, std::string>& symlink_map) {
    auto it_rename = rename_map.find(path);
    if (it_rename != rename_map.end()) {
        return it_rename->second;
    }
    auto it_symlink = symlink_map.find(path);
    if (it_symlink != symlink_map.end()) {
        return it_symlink->second;
    }
    return path;
}

template <auto ProvData::* member>
void record_path(const std::string& path, RecordParameters& record_parameters) {
    // std::string global_op = resolve_path(path, record_parameters.global_map);
    std::string exec_op = resolve_path(path, record_parameters.exec_rename_map,
                                       record_parameters.exec_symlink_map);
    //(record_parameters.global.*member).insert(global_op);
    (record_parameters.exec_prov_data.*member).insert(exec_op);
    //(record_parameters.proc.*member).insert(exec_op);
}

void record_write(const std::string& path,
                  RecordParameters& record_parameters) {
    record_path<&ProvData::writes>(path, record_parameters);
}

void record_read(const std::string& path, RecordParameters& record_parameters) {
    record_path<&ProvData::reads>(path, record_parameters);
}

void record_exec(const std::string& path, RecordParameters& record_parameters) {
    record_path<&ProvData::executes>(path, record_parameters);
}

void process_exec(const Exec& exec, ProcessedJobData& processed_job_data) {
    ExecProvData current_exec_prov_data;
    // ProvData& global_prov_data = processed_job_data.global_prov_data;
    ProvData& exec_prov_data = current_exec_prov_data.prov_data;
    // std::unordered_map<std::string, std::string>& global_rename_map
    //     = processed_job_data.global_rename_map;
    std::unordered_map<std::string, std::string>& exec_rename_map
        = current_exec_prov_data.rename_map;
    std::unordered_map<std::string, std::string>& exec_symlink_map
        = current_exec_prov_data.symlink_map;
    std::queue<Event> events = exec.events;
    ProvData empty_proc;
    RecordParameters record_parameters = {
        //.global_map = global_rename_map,
        .exec_rename_map = exec_rename_map,
        .exec_symlink_map = exec_symlink_map,
        //.global = global_prov_data,
        .exec_prov_data = exec_prov_data,
        //.proc = empty_proc
    };
    while (!events.empty()) {
        Event event = events.front();
        uint64_t event_pid = event.pid;
        SysOp op = event.operation;
        EventPayload event_payload = event.event_payload;
        ProcessProvData& current_process_prov_data
            = current_exec_prov_data.process_map[event_pid];
        // record_parameters.proc = current_process_prov_data.prov_data;
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
                rename_writes(record_parameters);
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
                std::string path_out = access_in_out.path_out;
                std::string path_in = access_in_out.path_in;
                if (exec_rename_map.find(path_in) == exec_rename_map.end()) {
                    exec_rename_map[path_out] = path_in;
                    // record_rename(path_out, path_in, record_parameters);
                } else {
                    exec_rename_map[path_out] = exec_rename_map[path_in];
                    exec_rename_map.erase(path_in);
                }
                break;
            }
            case SysOp::Link:
            case SysOp::SymLink: {
                const auto& access_in_out
                    = std::get<AccessInOut>(event_payload);
                std::string path_out = access_in_out.path_out;
                std::string path_in = access_in_out.path_in;
                std::string resolved_path_in
                    = resolve_path(path_in, exec_rename_map, exec_symlink_map);
                exec_symlink_map[path_out] = resolved_path_in;
                record_write(path_out, record_parameters);
                break;
            }
            case SysOp::Unlink: {
                const auto& access_out = std::get<AccessOut>(event_payload);
                std::string path_out = access_out.path_out;
                exec_symlink_map.erase(path_out);
                break;
            }
            case SysOp::Exec:
            case SysOp::System: {
                const auto& access_exec = std::get<ExecCall>(event_payload);
                std::string target = access_exec.target;
                record_exec(target, record_parameters);
                break;
            }
            case SysOp::Spawn: {
                const auto& access_spawn = std::get<SpawnCall>(event_payload);
                std::string target = access_spawn.target;
                record_exec(target, record_parameters);
                break;
            }
            case SysOp::Fork:
            default:
                break;
        }
        events.pop();
    }
    processed_job_data.exec_prov_data_queue.push(current_exec_prov_data);
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

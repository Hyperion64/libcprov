#pragma once
#include <string>

#include "model.hpp"

ParsedRequest parse_request(const std::string& json_body);

#pragma once
#include <string>

#include "model.hpp"

ParsedBatch parse_batch(const std::string& json_body);

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct GlueTypeHelpers {
	//! Whether two Glue column types read as the same DuckDB type, so 'int' and
	//! 'integer' agree; types that do not parse are compared as Hive compares
	//! them, ignoring case and whitespace
	static bool SameColumnType(const string &a, const string &b);
};

} // namespace duckdb

#include "core/hive_location.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string ComparableLocation(const string &location) {
	auto result = location;
	StringUtil::RTrim(result, "/");
	auto scheme = StringUtil::Lower(result.substr(0, 6));
	if (scheme == "s3a://" || scheme == "s3n://") {
		result = "s3://" + result.substr(6);
	} else if (StringUtil::Lower(result.substr(0, 5)) == "s3://") {
		result = "s3://" + result.substr(5);
	}
	return result;
}

bool LocationsOverlap(const string &a, const string &b) {
	return a == b || StringUtil::StartsWith(a, b + "/") || StringUtil::StartsWith(b, a + "/");
}

bool LocationHasGlob(const string &location) {
	return FileSystem::HasGlob(location);
}

unique_ptr<MultiFileList> ListLocation(FileSystem &fs, const string &location) {
	return fs.GlobFileList(location + "/**", FileGlobOptions::ALLOW_EMPTY);
}

} // namespace duckdb

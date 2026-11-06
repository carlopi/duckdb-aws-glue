#include "core/hive_location.hpp"

#include "duckdb/common/hive_partitioning.hpp"
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

string HivePartitionDirectory(const vector<string> &keys, const vector<string> &values) {
	D_ASSERT(keys.size() == values.size());
	vector<string> components;
	for (idx_t i = 0; i < keys.size(); i++) {
		auto value = values[i] == HivePartitioning::DEFAULT_PARTITION_NAME ? values[i]
		                                                                   : HivePartitioning::EscapeValue(values[i]);
		components.push_back(HivePartitioning::Escape(keys[i]) + "=" + value);
	}
	return StringUtil::Join(components, "/");
}

} // namespace duckdb

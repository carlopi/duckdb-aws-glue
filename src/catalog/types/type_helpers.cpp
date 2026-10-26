#include "catalog/types/type_helpers.hpp"

#include "core/glue_types.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

static string NormalizedHiveType(const string &type) {
	string result;
	for (auto c : type) {
		if (!StringUtil::CharacterIsSpace(c)) {
			result += StringUtil::CharacterToLower(c);
		}
	}
	return result;
}

bool GlueTypeHelpers::SameColumnType(const string &a, const string &b) {
	try {
		return GlueTypes::ToLogicalType(StringUtil::Lower(a)) == GlueTypes::ToLogicalType(StringUtil::Lower(b));
	} catch (std::exception &) {
		return NormalizedHiveType(a) == NormalizedHiveType(b);
	}
}

} // namespace duckdb

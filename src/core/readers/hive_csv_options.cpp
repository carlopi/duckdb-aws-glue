#include "core/readers/hive_csv_options.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

HiveFieldConversion HiveCSVOptions::FieldConversion(const LogicalType &type) const {
	// the whole type, not the id: an alias of VARCHAR such as JSON keeps its own conversion
	if (type == LogicalType::VARCHAR) {
		return HiveFieldConversion::NONE;
	}
	if (false_unless_true && type.id() == LogicalTypeId::BOOLEAN) {
		return HiveFieldConversion::FALSE_UNLESS_TRUE;
	}
	return null_for_invalid_data ? HiveFieldConversion::TRY_CAST : HiveFieldConversion::CAST;
}

static string RegexLiteral(const string &text) {
	string result;
	for (auto c : text) {
		if (!StringUtil::CharacterIsAlphaNumeric(c)) {
			result += '\\';
		}
		result += c;
	}
	return result;
}

string HiveCSVOptions::FieldEscapePattern() const {
	return RegexLiteral(field_escape) + "([" + RegexLiteral(field_escape) + RegexLiteral(quote) + "])?";
}

bool HiveCSVOptions::operator==(const HiveCSVOptions &other) const {
	return delimiter == other.delimiter && quote == other.quote && escape == other.escape &&
	       skip_lines == other.skip_lines && null_string == other.null_string && serde_fields == other.serde_fields &&
	       null_for_invalid_data == other.null_for_invalid_data && false_unless_true == other.false_unless_true &&
	       field_escape == other.field_escape;
}

bool HiveCSVOptions::operator!=(const HiveCSVOptions &other) const {
	return !(*this == other);
}

void HiveCSVOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "delimiter", delimiter);
	serializer.WriteProperty(101, "quote", quote);
	serializer.WriteProperty(102, "escape", escape);
	serializer.WriteProperty(103, "skip_lines", skip_lines);
	serializer.WriteProperty(104, "null_string", null_string);
	serializer.WriteProperty(105, "serde_fields", serde_fields);
	serializer.WritePropertyWithDefault<bool>(106, "null_for_invalid_data", null_for_invalid_data, true);
	serializer.WritePropertyWithDefault<bool>(107, "false_unless_true", false_unless_true, false);
	serializer.WritePropertyWithDefault<string>(109, "field_escape", field_escape, string());
}

HiveCSVOptions HiveCSVOptions::Deserialize(Deserializer &deserializer) {
	HiveCSVOptions result;
	result.delimiter = deserializer.ReadProperty<string>(100, "delimiter");
	result.quote = deserializer.ReadProperty<string>(101, "quote");
	result.escape = deserializer.ReadProperty<string>(102, "escape");
	result.skip_lines = deserializer.ReadProperty<idx_t>(103, "skip_lines");
	result.null_string = deserializer.ReadProperty<string>(104, "null_string");
	result.serde_fields = deserializer.ReadProperty<bool>(105, "serde_fields");
	result.null_for_invalid_data =
	    deserializer.ReadPropertyWithExplicitDefault<bool>(106, "null_for_invalid_data", true);
	result.false_unless_true = deserializer.ReadPropertyWithExplicitDefault<bool>(107, "false_unless_true", false);
	result.field_escape = deserializer.ReadPropertyWithExplicitDefault<string>(109, "field_escape", string());
	return result;
}

} // namespace duckdb

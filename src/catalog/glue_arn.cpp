#include "catalog/glue_arn.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//! The partition the region belongs to
string PartitionOf(const string &region) {
	if (StringUtil::StartsWith(region, "cn-")) {
		return "aws-cn";
	}
	if (StringUtil::StartsWith(region, "us-gov-")) {
		return "aws-us-gov";
	}
	return "aws";
}

} // namespace

bool IsArn(const string &path) {
	return StringUtil::StartsWith(StringUtil::Lower(path), "arn:");
}

GlueArn ParseGlueArn(const string &arn) {
	auto parts = StringUtil::Split(arn, ':');
	if (parts.size() < 6) {
		throw InvalidConfigurationException("Invalid Glue ARN '%s': expected "
		                                    "'arn:<partition>:glue:<region>:<account id>:catalog[/<catalog>/<nested>]'",
		                                    arn);
	}
	if (StringUtil::Lower(parts[2]) != "glue") {
		throw InvalidConfigurationException("'%s' is not a Glue ARN (service '%s')", arn, parts[2]);
	}
	GlueArn result;
	result.partition = parts[1];
	result.region = parts[3];
	if (result.region.empty()) {
		throw InvalidConfigurationException("Glue ARN '%s' does not specify a region", arn);
	}
	auto &account_id = parts[4];
	if (account_id.size() != 12 || !std::all_of(account_id.begin(), account_id.end(), StringUtil::CharacterIsDigit)) {
		throw InvalidConfigurationException("Glue ARN '%s' does not specify a 12-digit account id", arn);
	}
	result.account_id = account_id;
	// the resource is everything after the fifth ':'
	auto resource = StringUtil::Join(vector<string>(parts.begin() + 5, parts.end()), ":");
	const string catalog_prefix = "catalog/";
	if (resource == "catalog") {
		result.catalog_id = account_id;
	} else if (StringUtil::StartsWith(resource, catalog_prefix)) {
		auto catalog_path = resource.substr(catalog_prefix.size());
		if (catalog_path.empty() || catalog_path.front() == '/' || catalog_path.back() == '/' ||
		    StringUtil::Contains(catalog_path, "//")) {
			throw InvalidConfigurationException("Glue ARN '%s' has an empty catalog name", arn);
		}
		result.catalog_id = account_id + ":" + catalog_path;
	} else {
		throw InvalidConfigurationException(
		    "Glue ARN '%s' is not a catalog ARN: only a catalog ('catalog' or 'catalog/<name>') can be attached", arn);
	}
	return result;
}

string CatalogArn(const string &region, const string &catalog_id) {
	auto separator = catalog_id.find(':');
	auto account_id = separator == string::npos ? catalog_id : catalog_id.substr(0, separator);
	auto arn = "arn:" + PartitionOf(region) + ":glue:" + region + ":" + account_id + ":catalog";
	if (separator != string::npos) {
		arn += "/" + catalog_id.substr(separator + 1);
	}
	return arn;
}

} // namespace duckdb

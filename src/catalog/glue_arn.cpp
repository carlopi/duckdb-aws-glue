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

//! The catalog id of a non-empty catalog path
string CatalogIdOf(const string &arn, const string &account_id, const string &catalog_path) {
	if (catalog_path.empty() || catalog_path.front() == '/' || catalog_path.back() == '/' ||
	    StringUtil::Contains(catalog_path, "//")) {
		throw InvalidConfigurationException("Glue ARN '%s' has an empty catalog name", arn);
	}
	return account_id + ":" + catalog_path;
}

} // namespace

bool IsArn(const string &path) {
	return StringUtil::StartsWith(StringUtil::Lower(path), "arn:");
}

GlueArn ParseGlueArn(const string &arn) {
	auto parts = StringUtil::Split(arn, ':');
	if (parts.size() < 6) {
		throw InvalidConfigurationException("Invalid Glue ARN '%s': expected "
		                                    "'arn:<partition>:glue:<region>:<account id>:catalog[/<catalog>/<nested>]' "
		                                    "or 'arn:<partition>:glue:<region>:<account id>:database/<database>'",
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
	const string database_prefix = "database/";
	if (resource == "catalog") {
		result.catalog_id = account_id;
	} else if (StringUtil::StartsWith(resource, catalog_prefix)) {
		result.catalog_id = CatalogIdOf(arn, account_id, resource.substr(catalog_prefix.size()));
	} else if (StringUtil::StartsWith(resource, database_prefix)) {
		// the database is the last segment, the segments before it the catalog it is in
		auto path = resource.substr(database_prefix.size());
		auto separator = path.find_last_of('/');
		result.database = separator == string::npos ? path : path.substr(separator + 1);
		if (result.database.empty()) {
			throw InvalidConfigurationException("Glue ARN '%s' has an empty database name", arn);
		}
		result.catalog_id =
		    separator == string::npos ? account_id : CatalogIdOf(arn, account_id, path.substr(0, separator));
	} else {
		throw InvalidConfigurationException("Glue ARN '%s' is not a catalog or database ARN: only a catalog "
		                                    "('catalog' or 'catalog/<name>') or a database ('database/<name>') can be "
		                                    "attached",
		                                    arn);
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

string DatabaseArn(const string &region, const string &catalog_id, const string &database) {
	auto separator = catalog_id.find(':');
	auto account_id = separator == string::npos ? catalog_id : catalog_id.substr(0, separator);
	auto arn = "arn:" + PartitionOf(region) + ":glue:" + region + ":" + account_id + ":database/";
	if (separator != string::npos) {
		arn += catalog_id.substr(separator + 1) + "/";
	}
	return arn + database;
}

} // namespace duckdb

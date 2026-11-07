#include "catalog/glue_attach.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_options.hpp"
#include "catalog/glue_catalog.hpp"

#include <algorithm>
#include <regex>

namespace duckdb {

namespace {

//! Validate the ATTACH path, which identifies the Glue catalog
//! Same formats as Glue's own catalog id parameter (':' = default catalog of the current account)
void SanityCheckGlueCatalogPath(const string &path) {
	const std::regex patterns[] = {
	    std::regex("^:$"),                  // Default catalog ":" in current account
	    std::regex("^\\d{12}$"),            // Default catalog in a specific account
	    std::regex("^\\d{12}:[^:/]+$"),     // Specific catalog in a specific account
	    std::regex("^[^:]+/[^:]+$"),        // Nested catalog in the current account
	    std::regex("^\\d{12}:[^/]+/[^:]+$") // Nested catalog in a specific account
	};

	for (const auto &pattern : patterns) {
		if (std::regex_match(path, pattern)) {
			return;
		}
	}

	throw InvalidConfigurationException(
	    "Invalid Glue Catalog Format: '%s'. Expected format: ':', '12-digit account ID', "
	    "'catalog1/catalog2', or '12-digit accountId:catalog1/catalog2'.",
	    path);
}

bool IsArn(const string &path) {
	return StringUtil::StartsWith(StringUtil::Lower(path), "arn:");
}

struct GlueArn {
	string region;
	string catalog_id;
};

//! arn:<partition>:glue:<region>:<account id>:catalog[/<catalog>/<nested>...]
GlueArn ParseGlueCatalogArn(const string &arn) {
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
	result.region = parts[3];
	if (result.region.empty()) {
		throw InvalidConfigurationException("Glue ARN '%s' does not specify a region", arn);
	}
	auto &account_id = parts[4];
	if (account_id.size() != 12 || !std::all_of(account_id.begin(), account_id.end(), StringUtil::CharacterIsDigit)) {
		throw InvalidConfigurationException("Glue ARN '%s' does not specify a 12-digit account id", arn);
	}
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

} // namespace

unique_ptr<Catalog> GlueAttach::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &options) {
	GlueAttachOptions attach_options;
	attach_options.name = name;
	attach_options.path = info.path;
	auto from_arn = IsArn(info.path);
	if (from_arn) {
		auto arn = ParseGlueCatalogArn(info.path);
		attach_options.catalog_id = arn.catalog_id;
		attach_options.region = arn.region;
	} else {
		SanityCheckGlueCatalogPath(attach_options.path);
		if (attach_options.path != ":") {
			// ':' means the default catalog of the current account, which is what Glue uses without a CatalogId
			attach_options.catalog_id = attach_options.path;
		}
	}

	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			continue;
		}
		if (lower_name == "secret") {
			attach_options.secret_name = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "region") {
			auto region = entry.second.ToString();
			if (from_arn && region != attach_options.region) {
				throw InvalidConfigurationException(
				    "ATTACH option '%s' ('%s') conflicts with the region of the ARN ('%s')", entry.first, region,
				    attach_options.region);
			}
			attach_options.region = region;
		} else if (lower_name == "endpoint") {
			attach_options.endpoint = entry.second.ToString();
			StringUtil::RTrim(attach_options.endpoint, "/");
		} else if (lower_name == "default_location") {
			attach_options.default_location = entry.second.ToString();
			StringUtil::RTrim(attach_options.default_location, "/");
		} else if (lower_name == "default_schema") {
			attach_options.default_schema = Identifier(entry.second.ToString());
		} else if (lower_name == "verify_connection") {
			attach_options.verify_connection = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else {
			throw InvalidConfigurationException("Unrecognized option for Glue attach: '%s'", entry.first);
		}
	}

	// The region defaults to the region of the storage secret (an error is thrown if there is no secret)
	auto secret_entry = GlueCatalog::GetStorageSecret(context, attach_options.secret_name);
	if (attach_options.region.empty()) {
		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		auto region = kv_secret.TryGetValue("region");
		if (region.IsNull()) {
			throw InvalidConfigurationException(
			    "Secret '%s' used for Glue catalog '%s' does not have a region, provide one with the REGION option",
			    secret_entry->secret->GetName().GetIdentifierName(), name);
		}
		attach_options.region = region.ToString();
	}

	auto catalog = make_uniq<GlueCatalog>(db, options.access_mode, std::move(attach_options));
	// Fail early when the catalog can not be reached with these credentials (unless opted out)
	if (catalog->options.verify_connection) {
		// also learns the catalog id
		catalog->FetchCatalogId(context);
	}
	if (!catalog->options.default_schema.empty()) {
		GlueDatabaseInfo database;
		if (!GlueAPI::GetDatabase(context, *catalog, catalog->options.default_schema.GetIdentifierName(), database)) {
			throw InvalidConfigurationException("default_schema '%s' does not exist in Glue catalog '%s'",
			                                    catalog->options.default_schema.GetIdentifierName(),
			                                    catalog->options.path);
		}
	}
	return std::move(catalog);
}

} // namespace duckdb

#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"
#include "catalog/glue_options.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/glue/model/CreateCatalogRequest.h>
#include <aws/glue/model/DeleteCatalogRequest.h>
#include <aws/glue/model/GetCatalogRequest.h>
#include <aws/glue/model/GetCatalogsRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>

#include <sys/stat.h>

namespace duckdb {

namespace {

//! Grab the first path that exists, from a list of well-known CA bundle locations
string SelectCURLCertPath() {
	static const char *cert_file_locations[] = {// Arch, Debian-based, Gentoo
	                                            "/etc/ssl/certs/ca-certificates.crt",
	                                            // RedHat 7 based
	                                            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
	                                            // Redhat 6 based
	                                            "/etc/pki/tls/certs/ca-bundle.crt",
	                                            // OpenSUSE
	                                            "/etc/ssl/ca-bundle.pem",
	                                            // Alpine
	                                            "/etc/ssl/cert.pem"};
	for (auto &ca_file : cert_file_locations) {
		struct stat buf;
		if (stat(ca_file, &buf) == 0) {
			return ca_file;
		}
	}
	return string();
}

const string &GetCURLCertPath() {
	static string cert_path = SelectCURLCertPath();
	return cert_path;
}

//! What a client is built from: the credentials of the secret, the region and endpoint, and the settings that shape
//! the transport
struct GlueClientSettings {
	string key_id;
	string secret;
	string session_token;
	string region;
	string endpoint;
	bool via_duckdb = true;
	int64_t max_retries = -1;

	string CacheKey() const {
		return key_id + "\x1f" + session_token + "\x1f" + region + "\x1f" + endpoint + "\x1f" +
		       (via_duckdb ? "duckdb" : "sdk") + "\x1f" + to_string(max_retries);
	}
};

GlueClientSettings ResolveClientSettings(ClientContext &context, const GlueAttachOptions &options) {
	GlueClientSettings settings;
	// Take the credentials from the DuckDB secret. The secret is looked up on every call so a refreshed
	// (credential_chain / sts) secret is picked up automatically.
	auto secret_entry = GlueCatalog::GetStorageSecret(context, options.secret_name);
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto key_id_val = kv_secret.TryGetValue("key_id");
	auto secret_val = kv_secret.TryGetValue("secret");
	auto session_token_val = kv_secret.TryGetValue("session_token");
	settings.key_id = key_id_val.IsNull() ? "" : key_id_val.GetValue<string>();
	settings.secret = secret_val.IsNull() ? "" : secret_val.GetValue<string>();
	settings.session_token = session_token_val.IsNull() ? "" : session_token_val.GetValue<string>();
	if (settings.key_id.empty() || settings.secret.empty()) {
		throw InvalidConfigurationException(
		    "Secret '%s' does not contain AWS credentials (key_id / secret), can not connect to Glue catalog '%s'",
		    secret_entry->secret->GetName().GetIdentifierName(), options.name);
	}
	settings.region = options.region;
	settings.endpoint = options.endpoint;
	// The HTTP transport is chosen when the SDK client is built, so a changed setting needs a new client
	settings.via_duckdb = GlueNetworkCallsViaDuckDB(DatabaseInstance::GetDatabase(context));
	// The glue_max_retries flag: a custom retry limit for Glue API calls. -1 (the default) keeps the SDK's own
	// retry policy; 0 means no retries (single attempt); 1-10 sets that many retries, capped at 10.
	Value glue_max_retries_setting;
	if (context.TryGetCurrentSetting("glue_max_retries", glue_max_retries_setting) &&
	    !glue_max_retries_setting.IsNull()) {
		settings.max_retries = glue_max_retries_setting.GetValue<int64_t>();
		if (settings.max_retries > 10) {
			settings.max_retries = 10;
		}
	}
	return settings;
}

std::shared_ptr<Aws::Glue::GlueClient> MakeClient(ClientContext &context, const GlueClientSettings &settings) {
	// NOTE: without a region in the environment (AWS_DEFAULT_REGION / AWS_REGION) or the profile file, the SDK's
	// configuration constructor asks the EC2 instance metadata service for one before the region below is set;
	// off EC2 that is a connect timeout with retries. Set AWS_EC2_METADATA_DISABLED=true in such environments.
	Aws::Glue::GlueClientConfiguration config;
	config.region = settings.region;
	auto &endpoint = settings.endpoint;
	if (!endpoint.empty()) {
		// a Glue compatible server elsewhere (e.g. moto for tests): 'http://host:port' or 'https://host:port'
		auto lower = StringUtil::Lower(endpoint);
		if (StringUtil::StartsWith(lower, "http://")) {
			config.scheme = Aws::Http::Scheme::HTTP;
			config.endpointOverride = endpoint.substr(7);
		} else if (StringUtil::StartsWith(lower, "https://")) {
			config.scheme = Aws::Http::Scheme::HTTPS;
			config.endpointOverride = endpoint.substr(8);
		} else {
			config.endpointOverride = endpoint;
		}
	}
	auto &cert_path = GetCURLCertPath();
	if (!cert_path.empty()) {
		config.caFile = cert_path;
	}
	auto &db_config = DBConfig::GetConfig(context);
	config.userAgent = db_config.UserAgent();

	// Override the SDK's default retry policy only when glue_max_retries is set (>= 0); -1 leaves the default in
	// place. StandardRetryStrategy counts total attempts, so N retries is N + 1 attempts (0 retries = 1 attempt).
	if (settings.max_retries >= 0) {
		config.retryStrategy =
		    std::make_shared<Aws::Client::StandardRetryStrategy>(static_cast<long>(settings.max_retries + 1));
	}

	Aws::Auth::AWSCredentials credentials(settings.key_id, settings.secret, settings.session_token);
	auto provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
	return std::make_shared<Aws::Glue::GlueClient>(provider, nullptr, config);
}

} // namespace

std::shared_ptr<Aws::Glue::GlueClient> GlueAPI::GetClient(ClientContext &context, GlueCatalog &catalog) {
	auto settings = ResolveClientSettings(context, catalog.options);
	auto cache_key = settings.CacheKey();

	lock_guard<mutex> guard(catalog.client_lock);
	if (catalog.glue_client && catalog.client_cache_key == cache_key) {
		return catalog.glue_client;
	}
	catalog.glue_client = MakeClient(context, settings);
	catalog.client_cache_key = cache_key;
	return catalog.glue_client;
}

std::shared_ptr<Aws::Glue::GlueClient> GlueAPI::GetClient(ClientContext &context, const GlueAttachOptions &options) {
	return MakeClient(context, ResolveClientSettings(context, options));
}

//===--------------------------------------------------------------------===//
// Read API
//===--------------------------------------------------------------------===//
//! The catalog id of the first database that is not a resource link (a link's target is another catalog)
static string ReportedCatalogId(Aws::Glue::GlueClient &client, const GlueAttachOptions &options) {
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetDatabasesRequest request;
		SetCatalogId(request, options);
		request.SetMaxResults(20);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client.GetDatabases(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(
			    outcome, StringUtil::Format("GetDatabases (catalog '%s', region '%s')", options.path, options.region));
		}
		for (auto &database : outcome.GetResult().GetDatabaseList()) {
			auto info = ToDatabaseInfo(database);
			if (!info.is_resource_link && !info.catalog_id.empty()) {
				return info.catalog_id;
			}
		}
		next_token = outcome.GetResult().GetNextToken();
	} while (!next_token.empty());
	return string();
}

string GlueAPI::GetReportedCatalogId(ClientContext &context, GlueCatalog &catalog) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	return ReportedCatalogId(*client, catalog.options);
}

string GlueAPI::GetReportedCatalogId(ClientContext &context, const GlueAttachOptions &options) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	return ReportedCatalogId(*client, options);
}

static GlueCatalogNodeInfo ToCatalogNodeInfo(const Aws::Glue::Model::Catalog &catalog) {
	GlueCatalogNodeInfo info;
	info.catalog_id = ToStdString(catalog.GetCatalogId());
	info.name = ToStdString(catalog.GetName());
	info.resource_arn = ToStdString(catalog.GetResourceArn());
	info.description = ToStdString(catalog.GetDescription());
	if (catalog.FederatedCatalogHasBeenSet()) {
		info.kind = "federated";
	} else if (catalog.TargetRedshiftCatalogHasBeenSet()) {
		info.kind = "redshift";
	} else {
		info.kind = "native";
	}
	for (auto &parameter : catalog.GetParameters()) {
		info.parameters[ToStdString(parameter.first)] = ToStdString(parameter.second);
	}
	return info;
}

bool GlueAPI::GetCatalog(ClientContext &context, const GlueAttachOptions &options, GlueCatalogNodeInfo &result) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	Aws::Glue::Model::GetCatalogRequest request;
	request.SetCatalogId(options.catalog_id);
	auto outcome = client->GetCatalog(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetCatalog '%s'", options.catalog_id));
	}
	result = ToCatalogNodeInfo(outcome.GetResult().GetCatalog());
	return true;
}

void GlueAPI::CreateCatalog(ClientContext &context, const GlueAttachOptions &options, const string &name,
                            const string &description, const unordered_map<string, string> &parameters) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	Aws::Glue::Model::CatalogInput input;
	if (!description.empty()) {
		input.SetDescription(description);
	}
	if (!parameters.empty()) {
		input.SetParameters(ToAwsMap(parameters));
	}
	Aws::Glue::Model::CreateCatalogRequest request;
	request.SetName(name);
	request.SetCatalogInput(std::move(input));
	auto outcome = client->CreateCatalog(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome, StringUtil::Format("CreateCatalog '%s'", name));
	}
}

void GlueAPI::DeleteCatalog(ClientContext &context, const GlueAttachOptions &options) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	Aws::Glue::Model::DeleteCatalogRequest request;
	request.SetCatalogId(options.catalog_id);
	auto outcome = client->DeleteCatalog(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome, StringUtil::Format("DeleteCatalog '%s'", options.catalog_id));
	}
}

vector<GlueCatalogNodeInfo> GlueAPI::GetCatalogs(ClientContext &context, const GlueAttachOptions &options) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	vector<GlueCatalogNodeInfo> result;
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetCatalogsRequest request;
		request.SetIncludeRoot(true);
		request.SetRecursive(true);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetCatalogs(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, "GetCatalogs");
		}
		for (auto &catalog : outcome.GetResult().GetCatalogList()) {
			result.push_back(ToCatalogNodeInfo(catalog));
		}
		next_token = outcome.GetResult().GetNextToken();
	} while (!next_token.empty());
	return result;
}

} // namespace duckdb

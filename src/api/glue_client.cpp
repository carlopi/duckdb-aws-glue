#include "api/glue_api_util.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

namespace {

struct GlueCredentials {
	string key_id;
	string secret;
	string session_token;
};

struct GlueEndpoint {
	//! Scheme and authority, e.g. 'https://glue.eu-central-1.amazonaws.com'
	string proto_host_port;
	//! The value of the Host header
	string authority;
	string path;
};

//! How often a request Glue refused for its rate is sent again
constexpr idx_t GLUE_THROTTLING_RETRIES = 6;
constexpr idx_t GLUE_THROTTLING_BACKOFF_MS = 50;

//! The secret is looked up on every call so a refreshed (credential_chain / sts) secret is picked up automatically
GlueCredentials GetCredentials(ClientContext &context, const GlueCatalog &catalog) {
	auto secret_entry = GlueCatalog::GetStorageSecret(context, catalog.options.secret_name);
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto key_id_val = kv_secret.TryGetValue("key_id");
	auto secret_val = kv_secret.TryGetValue("secret");
	auto session_token_val = kv_secret.TryGetValue("session_token");
	GlueCredentials result;
	result.key_id = key_id_val.IsNull() ? "" : key_id_val.GetValue<string>();
	result.secret = secret_val.IsNull() ? "" : secret_val.GetValue<string>();
	result.session_token = session_token_val.IsNull() ? "" : session_token_val.GetValue<string>();
	if (result.key_id.empty() || result.secret.empty()) {
		throw InvalidConfigurationException(
		    "Secret '%s' does not contain AWS credentials (key_id / secret), can not connect to Glue catalog '%s'",
		    secret_entry->secret->GetName().GetIdentifierName(), catalog.options.name);
	}
	return result;
}

//! AWS, or a Glue compatible server elsewhere (e.g. moto for tests): 'host:port', 'http://host:port' or
//! 'https://host:port'
GlueEndpoint GetEndpoint(const GlueAttachOptions &options) {
	string scheme = "https://";
	string endpoint = options.endpoint;
	if (endpoint.empty()) {
		endpoint = "glue." + options.region + ".amazonaws.com";
	} else {
		auto lower = StringUtil::Lower(endpoint);
		if (StringUtil::StartsWith(lower, "http://")) {
			scheme = "http://";
			endpoint = endpoint.substr(7);
		} else if (StringUtil::StartsWith(lower, "https://")) {
			endpoint = endpoint.substr(8);
		}
	}
	GlueEndpoint result;
	auto path_start = endpoint.find('/');
	result.authority = endpoint.substr(0, path_start);
	result.path = path_start == string::npos ? "/" : endpoint.substr(path_start);
	result.proto_host_port = scheme + result.authority;
	return result;
}

string GetPayloadHash(EncryptionUtil &encryption_util, const string &data) {
	string result(CryptoHash::GetHexDigestSize(CryptoHashFunction::SHA256), '\0');
	encryption_util.HashHex(CryptoHashFunction::SHA256, const_data_ptr_cast(data.data()), data.size(), &result[0]);
	return result;
}

//! The headers of a Glue request, signed with AWS Signature Version 4. The Host header is part of the signature but
//! is left to the HTTP client to send.
HTTPHeaders SignRequest(DatabaseInstance &db, const GlueCredentials &credentials, const string &region,
                        const GlueEndpoint &endpoint, const string &operation, const string &body) {
	auto encryption_util = db.GetEncryptionUtil(true);
	auto timestamp = Timestamp::GetCurrentTimestamp();
	string date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
	string datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");
	auto payload_hash = GetPayloadHash(*encryption_util, body);

	// in the (alphabetical) order of the canonical request
	vector<pair<string, string>> signed_headers;
	signed_headers.emplace_back("content-type", "application/x-amz-json-1.1");
	signed_headers.emplace_back("host", endpoint.authority);
	signed_headers.emplace_back("x-amz-date", datetime_now);
	if (!credentials.session_token.empty()) {
		signed_headers.emplace_back("x-amz-security-token", credentials.session_token);
	}
	signed_headers.emplace_back("x-amz-target", "AWSGlue." + operation);

	HTTPHeaders result(db);
	string canonical_headers;
	string signed_header_names;
	for (auto &header : signed_headers) {
		canonical_headers += header.first + ":" + header.second + "\n";
		if (!signed_header_names.empty()) {
			signed_header_names += ";";
		}
		signed_header_names += header.first;
		if (header.first != "host") {
			result.Insert(header.first, header.second);
		}
	}

	SignatureV4Params signature_params;
	signature_params.canonical_request =
	    "POST\n" + endpoint.path + "\n\n" + canonical_headers + "\n" + signed_header_names + "\n" + payload_hash;
	signature_params.credential_scope = date_now + "/" + region + "/glue/aws4_request";
	signature_params.region = region;
	signature_params.service = "glue";
	signature_params.secret_access_key = credentials.secret;
	signature_params.date_now = date_now;
	signature_params.datetime_now = datetime_now;
	auto signature = HTTPUtil::CreateSignatureV4(*encryption_util, signature_params);

	result.Insert("Authorization", "AWS4-HMAC-SHA256 Credential=" + credentials.key_id + "/" +
	                                   signature_params.credential_scope + ", SignedHeaders=" + signed_header_names +
	                                   ", Signature=" + signature);
	return result;
}

//! 'com.amazonaws.glue#EntityNotFoundException' or 'EntityNotFoundException:http://...' -> EntityNotFoundException
string NormalizeErrorType(string error_type) {
	auto hash = error_type.find('#');
	if (hash != string::npos) {
		error_type = error_type.substr(hash + 1);
	}
	auto colon = error_type.find(':');
	if (colon != string::npos) {
		error_type = error_type.substr(0, colon);
	}
	return error_type;
}

bool IsThrottled(const GlueResponse &response) {
	return response.error_type == "ThrottlingException" || response.error_type == "TooManyRequestsException" ||
	       response.error_type == "RequestLimitExceeded";
}

GlueResponse SendRequest(ClientContext &context, const GlueCatalog &catalog, const string &operation,
                         const string &body) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto credentials = GetCredentials(context, catalog);
	auto endpoint = GetEndpoint(catalog.options);
	auto headers = SignRequest(db, credentials, catalog.options.region, endpoint, operation, body);

	auto &http_util = HTTPUtil::Get(db);
	auto url = endpoint.proto_host_port + endpoint.path;
	// the parameters of the calling connection carry its logger: the request shows up in that connection's HTTP log
	auto params = http_util.InitializeParameters(context, url);
	auto client = http_util.InitializeClient(*params, endpoint.proto_host_port);
	PostRequestInfo info(url, headers, *params, const_data_ptr_cast(body.c_str()), body.size());
	info.try_request = true;
	auto http_response = http_util.Request(info, client);
	if (!http_response) {
		throw IOException("Glue %s failed: no response from '%s'", operation, url);
	}
	if (http_response->HasRequestError()) {
		throw IOException("Glue %s failed: %s (request to '%s')", operation, http_response->GetRequestError(), url);
	}

	GlueResponse result;
	result.document = ParseGlueJson(info.buffer_out);
	auto status = static_cast<idx_t>(http_response->status);
	if (status >= 200 && status < 300) {
		return result;
	}
	result.error_type = NormalizeErrorType(result.GetString("__type"));
	if (result.error_type.empty() && http_response->HasHeader("x-amzn-ErrorType")) {
		result.error_type = NormalizeErrorType(http_response->GetHeaderValue("x-amzn-ErrorType"));
	}
	if (result.error_type.empty()) {
		result.error_type = StringUtil::Format("HTTP %d", status);
	}
	result.error_message = result.GetString("message");
	if (result.error_message.empty()) {
		result.error_message = result.GetString("Message");
	}
	if (result.error_message.empty()) {
		result.error_message = info.buffer_out.empty() ? http_response->GetError() : info.buffer_out;
	}
	return result;
}

} // namespace

GlueResponse GlueAPI::Call(ClientContext &context, const GlueCatalog &catalog, const string &operation,
                           const GlueRequest &request) {
	auto &db = DatabaseInstance::GetDatabase(context);
	if (!db.ExtensionIsLoaded("httpfs")) {
		// httpfs provides the HTTP client that can send the requests
		ExtensionHelper::AutoLoadExtension(db, "httpfs");
	}
	auto body = request.ToString();
	for (idx_t attempt = 0;; attempt++) {
		auto response = SendRequest(context, catalog, operation, body);
		if (!IsThrottled(response) || attempt >= GLUE_THROTTLING_RETRIES) {
			return response;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(GLUE_THROTTLING_BACKOFF_MS << attempt));
	}
}

//===--------------------------------------------------------------------===//
// Read API
//===--------------------------------------------------------------------===//
void GlueAPI::VerifyConnection(ClientContext &context, GlueCatalog &catalog) {
	GlueRequest request(catalog);
	request.SetInteger(request.Root(), "MaxResults", 1);
	auto response = Call(context, catalog, "GetDatabases", request);
	if (!response.IsSuccess()) {
		response.Throw(StringUtil::Format("GetDatabases (catalog '%s', region '%s')", catalog.options.path,
		                                  catalog.options.region));
	}
}

} // namespace duckdb

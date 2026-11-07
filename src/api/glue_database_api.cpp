#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"
#include "catalog/glue_transaction.hpp"

#include "duckdb/common/string_util.hpp"

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/glue/model/CreateDatabaseRequest.h>
#include <aws/glue/model/DatabaseIdentifier.h>
#include <aws/glue/model/DeleteDatabaseRequest.h>
#include <aws/glue/model/GetDatabaseRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>
#include <aws/glue/model/UpdateDatabaseRequest.h>

namespace duckdb {

static vector<GlueDatabaseInfo> ListDatabases(Aws::Glue::GlueClient &client, const GlueAttachOptions &options) {
	vector<GlueDatabaseInfo> result;
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetDatabasesRequest request;
		SetCatalogId(request, options);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client.GetDatabases(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, "GetDatabases");
		}
		auto &databases = outcome.GetResult();
		for (auto &database : databases.GetDatabaseList()) {
			result.push_back(ToDatabaseInfo(database));
		}
		next_token = databases.GetNextToken();
	} while (!next_token.empty());
	return result;
}

vector<GlueDatabaseInfo> GlueAPI::GetDatabases(ClientContext &context, GlueCatalog &catalog) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	return ListDatabases(*client, catalog.options);
}

vector<GlueDatabaseInfo> GlueAPI::GetDatabases(ClientContext &context, const GlueAttachOptions &options) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	return ListDatabases(*client, options);
}

static bool FetchDatabase(Aws::Glue::GlueClient &client, const GlueAttachOptions &options, const string &database_name,
                          GlueDatabaseInfo &result, string *raw_json) {
	Aws::Glue::Model::GetDatabaseRequest request;
	SetCatalogId(request, options);
	request.SetName(database_name);
	auto outcome = client.GetDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetDatabase '%s'", database_name));
	}
	auto &database = outcome.GetResult().GetDatabase();
	result = ToDatabaseInfo(database);
	if (raw_json) {
		*raw_json = ToStdString(database.Jsonize().View().WriteReadable());
	}
	return true;
}

bool GlueAPI::GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                          GlueDatabaseInfo &result, string *raw_json) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	return FetchDatabase(*client, catalog.options, database_name, result, raw_json);
}

bool GlueAPI::GetDatabase(ClientContext &context, const GlueAttachOptions &options, const string &database_name,
                          GlueDatabaseInfo &result) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	return FetchDatabase(*client, options, database_name, result, nullptr);
}

static GlueCreateOutcome PutDatabase(Aws::Glue::GlueClient &client, const GlueAttachOptions &options,
                                     const GlueDatabaseInfo &database) {
	Aws::Glue::Model::DatabaseInput input;
	input.SetName(database.name);
	if (!database.description.empty()) {
		input.SetDescription(database.description);
	}
	if (!database.location_uri.empty()) {
		input.SetLocationUri(database.location_uri);
	}
	if (!database.parameters.empty()) {
		input.SetParameters(ToAwsMap(database.parameters));
	}
	if (!database.target_database.empty()) {
		Aws::Glue::Model::DatabaseIdentifier target;
		target.SetDatabaseName(database.target_database);
		if (!database.target_catalog_id.empty()) {
			target.SetCatalogId(database.target_catalog_id);
		}
		if (!database.target_region.empty()) {
			target.SetRegion(database.target_region);
		}
		input.SetTargetDatabase(std::move(target));
	}
	Aws::Glue::Model::CreateDatabaseRequest request;
	SetCatalogId(request, options);
	request.SetDatabaseInput(input);
	auto outcome = client.CreateDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			return GlueCreateOutcome::ALREADY_EXISTED;
		}
		ThrowGlueError(outcome, StringUtil::Format("CreateDatabase '%s'", database.name));
	}
	return GlueCreateOutcome::CREATED;
}

GlueCreateOutcome GlueAPI::CreateDatabase(ClientContext &context, GlueCatalog &catalog,
                                          const GlueDatabaseInfo &database) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "CreateDatabase");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	return PutDatabase(*client, catalog.options, database);
}

GlueCreateOutcome GlueAPI::CreateDatabase(ClientContext &context, const GlueAttachOptions &options,
                                          const GlueDatabaseInfo &database) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	return PutDatabase(*client, options, database);
}

void GlueAPI::UpdateDatabase(ClientContext &context, GlueCatalog &catalog, const GlueDatabaseInfo &database) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetDatabaseRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetName(database.name);
	auto get_outcome = client->GetDatabase(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database.name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetDatabase '%s'", database.name));
	}
	// UpdateDatabase replaces the whole definition: carry over what is not changed here
	auto &current = get_outcome.GetResult().GetDatabase();
	Aws::Glue::Model::DatabaseInput input;
	input.SetName(database.name);
	input.SetDescription(database.description);
	input.SetParameters(ToAwsMap(database.parameters));
	if (!database.location_uri.empty()) {
		input.SetLocationUri(database.location_uri);
	}
	if (current.CreateTableDefaultPermissionsHasBeenSet()) {
		input.SetCreateTableDefaultPermissions(current.GetCreateTableDefaultPermissions());
	}
	if (current.TargetDatabaseHasBeenSet()) {
		input.SetTargetDatabase(current.GetTargetDatabase());
	}
	if (current.FederatedDatabaseHasBeenSet()) {
		input.SetFederatedDatabase(current.GetFederatedDatabase());
	}
	Aws::Glue::Model::UpdateDatabaseRequest request;
	SetCatalogId(request, catalog);
	request.SetName(database.name);
	request.SetDatabaseInput(input);
	auto outcome = client->UpdateDatabase(request);
	if (!outcome.IsSuccess()) {
		if (outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::CONCURRENT_MODIFICATION) {
			throw TransactionException("Glue database \"%s\" was changed by another client while it was altered, run "
			                           "the statement again",
			                           database.name);
		}
		ThrowGlueError(outcome, StringUtil::Format("UpdateDatabase '%s'", database.name));
	}
}

static void DropDatabase(Aws::Glue::GlueClient &client, const GlueAttachOptions &options, const string &database_name) {
	Aws::Glue::Model::DeleteDatabaseRequest request;
	SetCatalogId(request, options);
	request.SetName(database_name);
	auto outcome = client.DeleteDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("DeleteDatabase '%s'", database_name));
	}
}

void GlueAPI::DeleteDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "DeleteDatabase");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	DropDatabase(*client, catalog.options, database_name);
}

void GlueAPI::DeleteDatabase(ClientContext &context, const GlueAttachOptions &options, const string &database_name) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, options);
	DropDatabase(*client, options, database_name);
}

} // namespace duckdb

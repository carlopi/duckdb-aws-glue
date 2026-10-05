#include "api/glue_api_util.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

vector<GlueDatabaseInfo> GlueAPI::GetDatabases(ClientContext &context, GlueCatalog &catalog) {
	vector<GlueDatabaseInfo> result;
	string next_token;
	do {
		GlueRequest request(catalog);
		if (!next_token.empty()) {
			request.SetString(request.Root(), "NextToken", next_token);
		}
		auto response = Call(context, catalog, "GetDatabases", request);
		if (!response.IsSuccess()) {
			response.Throw("GetDatabases");
		}
		for (auto database : GlueJsonElements(response.Get("DatabaseList"))) {
			result.push_back(ToDatabaseInfo(database));
		}
		next_token = response.GetString("NextToken");
	} while (!next_token.empty());
	return result;
}

bool GlueAPI::GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                          GlueDatabaseInfo &result, string *raw_json) {
	GlueRequest request(catalog);
	request.SetString(request.Root(), "Name", database_name);
	auto response = Call(context, catalog, "GetDatabase", request);
	if (!response.IsSuccess()) {
		if (response.IsEntityNotFound()) {
			return false;
		}
		response.Throw(StringUtil::Format("GetDatabase '%s'", database_name));
	}
	auto database = response.Get("Database");
	result = ToDatabaseInfo(database);
	if (raw_json) {
		*raw_json = GlueJsonToString(database);
	}
	return true;
}

void GlueAPI::CreateDatabase(ClientContext &context, GlueCatalog &catalog, const GlueDatabaseInfo &database) {
	CheckWritable(catalog, "CreateDatabase");
	GlueRequest request(catalog);
	auto input = request.SetObject(request.Root(), "DatabaseInput");
	request.SetString(input, "Name", database.name);
	if (!database.description.empty()) {
		request.SetString(input, "Description", database.description);
	}
	if (!database.location_uri.empty()) {
		request.SetString(input, "LocationUri", database.location_uri);
	}
	if (!database.parameters.empty()) {
		request.SetMap(input, "Parameters", database.parameters);
	}
	auto response = Call(context, catalog, "CreateDatabase", request);
	if (!response.IsSuccess()) {
		if (response.IsAlreadyExists()) {
			throw CatalogException("Glue database with name \"%s\" already exists", database.name);
		}
		response.Throw(StringUtil::Format("CreateDatabase '%s'", database.name));
	}
}

void GlueAPI::UpdateDatabase(ClientContext &context, GlueCatalog &catalog, const GlueDatabaseInfo &database) {
	GlueRequest get_request(catalog);
	get_request.SetString(get_request.Root(), "Name", database.name);
	auto get_response = Call(context, catalog, "GetDatabase", get_request);
	if (!get_response.IsSuccess()) {
		if (get_response.IsEntityNotFound()) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database.name);
		}
		get_response.Throw(StringUtil::Format("GetDatabase '%s'", database.name));
	}
	// UpdateDatabase replaces the whole definition: carry over what is not changed here
	auto current = get_response.Get("Database");
	GlueRequest request(catalog);
	request.SetString(request.Root(), "Name", database.name);
	auto input = request.SetObject(request.Root(), "DatabaseInput");
	request.SetString(input, "Name", database.name);
	request.SetString(input, "Description", database.description);
	request.SetString(input, "LocationUri", database.location_uri);
	request.SetMap(input, "Parameters", database.parameters);
	for (auto key : {"CreateTableDefaultPermissions", "TargetDatabase", "FederatedDatabase"}) {
		request.Copy(input, key, GlueJsonGet(current, key));
	}
	auto response = Call(context, catalog, "UpdateDatabase", request);
	if (!response.IsSuccess()) {
		response.Throw(StringUtil::Format("UpdateDatabase '%s'", database.name));
	}
}

void GlueAPI::DeleteDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name) {
	CheckWritable(catalog, "DeleteDatabase");
	GlueRequest request(catalog);
	request.SetString(request.Root(), "Name", database_name);
	auto response = Call(context, catalog, "DeleteDatabase", request);
	if (!response.IsSuccess()) {
		if (response.IsEntityNotFound()) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database_name);
		}
		response.Throw(StringUtil::Format("DeleteDatabase '%s'", database_name));
	}
}

} // namespace duckdb

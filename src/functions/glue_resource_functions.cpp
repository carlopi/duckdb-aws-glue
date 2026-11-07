#include "functions/glue_functions.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_arn.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_options.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/external_resource_type_registry.hpp"
#include "duckdb/main/secret/secret.hpp"

#include <algorithm>

namespace duckdb {

//! The external resource type: a node of the account's catalog tree. Also stamped as a parameter on every catalog
//! the create callback makes, so a catalog carries its own provenance.
static constexpr const char *GLUE_CATALOG_TYPE = "aws:glue:catalog";
static constexpr const char *RESOURCE_TYPE_PARAMETER = "duckdb-external-resource-type";

namespace {

struct ResourceBindData : public TableFunctionData {
	//! The params (list) or handle (status, destroy) MAP
	Value input;
};

//! The rows of a callback, built in one go from the Glue responses and emitted a vector at a time
struct ResourceState : public GlobalTableFunctionState {
	vector<vector<Value>> rows;
	idx_t cursor = 0;
	bool ran = false;
};

unique_ptr<GlobalTableFunctionState> ResourceInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<ResourceState>();
}

//! Read one key out of a params / handle MAP; "" when absent or NULL
string MapLookup(const Value &map_value, const string &key) {
	if (map_value.IsNull()) {
		return string();
	}
	for (auto &entry : MapValue::GetChildren(map_value)) {
		auto &kv = StructValue::GetChildren(entry);
		if (!kv[0].IsNull() && StringValue::Get(kv[0]) == key) {
			return kv[1].IsNull() ? string() : StringValue::Get(kv[1]);
		}
	}
	return string();
}

Value MapOf(vector<Value> keys, vector<Value> values) {
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

//! The connection the params / handle describe: 'region' (default: the region of the secret), 'endpoint' and
//! 'secret', the same keys as the ATTACH options. A handle names its region through its ARN.
GlueAttachOptions OptionsOf(ClientContext &context, const Value &input, const string &catalog_id,
                            const string &region = string()) {
	GlueAttachOptions options;
	options.name = GLUE_CATALOG_TYPE;
	options.path = catalog_id.empty() ? ":" : catalog_id;
	options.catalog_id = catalog_id;
	options.secret_name = StringUtil::Lower(MapLookup(input, "secret"));
	options.endpoint = MapLookup(input, "endpoint");
	StringUtil::RTrim(options.endpoint, "/");
	options.region = region.empty() ? MapLookup(input, "region") : region;
	if (options.region.empty()) {
		auto secret_entry = GlueCatalog::GetStorageSecret(context, options.secret_name);
		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		auto region = kv_secret.TryGetValue("region");
		if (region.IsNull()) {
			throw InvalidConfigurationException("%s: secret '%s' does not have a region, provide one with 'region'",
			                                    GLUE_CATALOG_TYPE, secret_entry->secret->GetName().GetIdentifierName());
		}
		options.region = region.ToString();
	}
	return options;
}

//! The handle of a catalog: its ARN, with the endpoint and secret the params named, so that the handle the listing
//! emits is the one a registration from the same params holds
Value HandleOf(const string &arn, const Value &params) {
	vector<Value> keys {Value("arn")};
	vector<Value> values {Value(arn)};
	for (auto key : {"endpoint", "secret"}) {
		auto value = MapLookup(params, key);
		if (!value.empty()) {
			keys.emplace_back(key);
			values.emplace_back(value);
		}
	}
	return MapOf(std::move(keys), std::move(values));
}

//! The catalog a handle names
GlueArn HandleArn(const Value &handle, const char *callback) {
	auto arn = MapLookup(handle, "arn");
	if (arn.empty()) {
		throw InvalidInputException("%s: handle is missing 'arn'", callback);
	}
	return ParseGlueArn(arn);
}

//! The ARN of a catalog node: the one Glue reports, or the one its id and region make
string ArnOf(const GlueCatalogNodeInfo &node, const string &region) {
	return node.resource_arn.empty() ? CatalogArn(region, node.catalog_id) : node.resource_arn;
}

Value MetadataOf(const GlueCatalogNodeInfo &node) {
	return MapOf({Value("catalog_id"), Value("type"), Value("description")},
	             {Value(node.catalog_id), Value(node.kind), Value(node.description)});
}

void EmitRows(ResourceState &state, DataChunk &output) {
	idx_t count = MinValue<idx_t>(state.rows.size() - state.cursor, STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		auto &row = state.rows[state.cursor + i];
		for (idx_t col = 0; col < output.ColumnCount(); col++) {
			output.data[col].SetValue(i, row[col]);
		}
	}
	output.SetCardinality(count);
	state.cursor += count;
}

//===--------------------------------------------------------------------===//
// create(params MAP) -> TABLE(handle MAP)
//
// Without 'path' the root catalog of 'region' (default: the region of the secret), which exists in every region and
// is only resolved, never created. With 'path' a catalog under the root, which Glue only creates as a federated one,
// 'connection' and 'identifier' naming its source, or a Redshift-managed one, 'redshift_catalog_arn'; a nested
// 'a/b/c' needs 'a/b' to exist. CreateCatalog is synchronous, so the handle is of a catalog that already exists.
// Also 'description', 'endpoint' and 'secret'.
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> CreateBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                    vector<Identifier> &names) {
	auto result = make_uniq<ResourceBindData>();
	result->input = input.inputs[0];
	names.emplace_back("handle");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	return std::move(result);
}

//! The root catalog of the region: the catalog id Glue reports for it (GetCatalog wants a catalog id, so it is the
//! root of the catalogs listing), or, where there is no catalogs API, the one its databases report
string RootCatalogId(ClientContext &context, const GlueAttachOptions &root) {
	try {
		for (auto &node : GlueAPI::GetCatalogs(context, root, string(), false)) {
			if (node.catalog_id.find(':') == string::npos) {
				return node.catalog_id;
			}
		}
	} catch (GlueException &) {
	}
	auto reported = GlueAPI::GetReportedCatalogId(context, root);
	if (reported.empty()) {
		throw IOException("%s create: the account id of the root catalog in region '%s' can not be told: it has "
		                  "no databases yet and the catalogs API is unavailable",
		                  GLUE_CATALOG_TYPE, root.region);
	}
	return reported;
}

void CreateFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ResourceState>();
	auto &bind = data_p.bind_data->Cast<ResourceBindData>();
	if (!state.ran) {
		state.ran = true;
		auto root = OptionsOf(context, bind.input, string());
		auto path = MapLookup(bind.input, "path");
		if (path.empty()) {
			state.rows.push_back({HandleOf(CatalogArn(root.region, RootCatalogId(context, root)), bind.input)});
			EmitRows(state, output);
			return;
		}

		GlueCatalogInput input;
		input.name = path;
		input.description = MapLookup(bind.input, "description");
		input.parameters[RESOURCE_TYPE_PARAMETER] = GLUE_CATALOG_TYPE;
		input.connection_name = MapLookup(bind.input, "connection");
		input.connection_type = MapLookup(bind.input, "connection_type");
		input.identifier = MapLookup(bind.input, "identifier");
		input.redshift_catalog_arn = MapLookup(bind.input, "redshift_catalog_arn");
		if (input.connection_name.empty() == input.identifier.empty() && input.connection_name.empty() &&
		    input.redshift_catalog_arn.empty()) {
			throw InvalidInputException("%s create: Glue creates no native catalog below the root: give 'connection' "
			                            "and 'identifier' for a federated catalog or 'redshift_catalog_arn' for a "
			                            "Redshift-managed one, or no 'path' for the root catalog of the region",
			                            GLUE_CATALOG_TYPE);
		}
		if (input.connection_name.empty() != input.identifier.empty()) {
			throw InvalidInputException("%s create: a federated catalog needs both 'connection' and 'identifier'",
			                            GLUE_CATALOG_TYPE);
		}
		// a nested catalog goes under a catalog that exists; Glue wants its id qualified with the account
		auto account_id = RootCatalogId(context, root);
		auto separator = path.find_last_of('/');
		if (separator != string::npos) {
			auto parent = path.substr(0, separator);
			GlueCatalogNodeInfo parent_node;
			if (parent.empty() ||
			    !GlueAPI::GetCatalog(context, OptionsOf(context, bind.input, account_id + ":" + parent, root.region),
			                         parent_node)) {
				throw InvalidInputException("%s create: the parent catalog '%s' of '%s' does not exist",
				                            GLUE_CATALOG_TYPE, parent, path);
			}
		}
		GlueAPI::CreateCatalog(context, root, input);

		// the handle is the ARN, which Glue reports for the created catalog
		GlueCatalogNodeInfo node;
		if (!GlueAPI::GetCatalog(context, OptionsOf(context, bind.input, account_id + ":" + path, root.region), node)) {
			throw IOException("%s create: catalog '%s' is not found after CreateCatalog", GLUE_CATALOG_TYPE, path);
		}
		state.rows.push_back({HandleOf(ArnOf(node, root.region), bind.input)});
	}
	EmitRows(state, output);
}

//===--------------------------------------------------------------------===//
// status(handle MAP) -> TABLE(state VARCHAR, result MAP)
//
// A catalog exists or it does not, so one call that lists its databases decides 'ready'; the call fails with Glue's
// error otherwise. REGISTER EXTERNAL RESOURCE resolves a user-supplied handle through here, so a bad ARN fails now
// rather than at the first query. The result is what ATTACH takes: the ARN as the path, 'glue' as the type, and the
// endpoint and secret of the handle as options.
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> StatusBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                    vector<Identifier> &names) {
	auto result = make_uniq<ResourceBindData>();
	result->input = input.inputs[0];
	names.emplace_back("state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("result");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	return std::move(result);
}

void StatusFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ResourceState>();
	auto &bind = data_p.bind_data->Cast<ResourceBindData>();
	if (!state.ran) {
		state.ran = true;
		auto arn = HandleArn(bind.input, "aws:glue:catalog status");
		auto options = OptionsOf(context, bind.input, arn.catalog_id, arn.region);
		GlueAPI::GetReportedCatalogId(context, options);

		vector<Value> keys {Value("uri"), Value("attached_db_type")};
		vector<Value> values {Value(MapLookup(bind.input, "arn")), Value("glue")};
		for (auto key : {"endpoint", "secret"}) {
			auto value = MapLookup(bind.input, key);
			if (!value.empty()) {
				keys.emplace_back(key);
				values.emplace_back(value);
			}
		}
		state.rows.push_back({Value("ready"), MapOf(std::move(keys), std::move(values))});
	}
	EmitRows(state, output);
}

//===--------------------------------------------------------------------===//
// destroy(handle MAP) -> TABLE(status VARCHAR)
//
// DeleteCatalog on the catalog the handle names, after the catalogs under it, deepest first. The root catalog of a
// region exists whether or not anything refers to it, so destroying it does nothing and reports 'kept'.
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> DestroyBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                     vector<Identifier> &names) {
	auto result = make_uniq<ResourceBindData>();
	result->input = input.inputs[0];
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

void DestroyFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ResourceState>();
	auto &bind = data_p.bind_data->Cast<ResourceBindData>();
	if (!state.ran) {
		state.ran = true;
		auto arn = HandleArn(bind.input, "aws:glue:catalog destroy");
		if (arn.catalog_id == arn.account_id) {
			state.rows.push_back({Value("kept")});
			EmitRows(state, output);
			return;
		}
		auto options = OptionsOf(context, bind.input, arn.catalog_id, arn.region);
		auto subtree = GlueAPI::GetCatalogs(context, options, arn.catalog_id);
		std::sort(subtree.begin(), subtree.end(), [](const GlueCatalogNodeInfo &a, const GlueCatalogNodeInfo &b) {
			return std::count(a.catalog_id.begin(), a.catalog_id.end(), '/') >
			       std::count(b.catalog_id.begin(), b.catalog_id.end(), '/');
		});
		for (auto &node : subtree) {
			GlueAPI::DeleteCatalog(context, OptionsOf(context, bind.input, node.catalog_id, arn.region));
		}
		GlueAPI::DeleteCatalog(context, options);
		state.rows.push_back({Value("deleted")});
	}
	EmitRows(state, output);
}

//===--------------------------------------------------------------------===//
// list(params MAP) -> TABLE(handle MAP, name VARCHAR, reference VARCHAR, state VARCHAR, metadata MAP)
//
// Every node of the catalog tree: the default catalog and the catalogs under it, native, federated or managed. Where
// Glue has no catalogs API (a local server) or refuses it, the default catalog alone, found as the catalog of its
// databases.
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> ListBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                  vector<Identifier> &names) {
	auto result = make_uniq<ResourceBindData>();
	result->input = input.inputs[0];
	auto map_vv = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	names.emplace_back("handle");
	return_types.emplace_back(map_vv);
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("reference");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("metadata");
	return_types.emplace_back(map_vv);
	return std::move(result);
}

void ListFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ResourceState>();
	auto &bind = data_p.bind_data->Cast<ResourceBindData>();
	if (!state.ran) {
		state.ran = true;
		auto options = OptionsOf(context, bind.input, string());
		vector<GlueCatalogNodeInfo> nodes;
		try {
			nodes = GlueAPI::GetCatalogs(context, options);
		} catch (GlueException &) {
			GlueCatalogNodeInfo root;
			root.catalog_id = GlueAPI::GetReportedCatalogId(context, options);
			if (!root.catalog_id.empty()) {
				root.name = root.catalog_id;
				root.kind = "native";
				nodes.push_back(std::move(root));
			}
		}
		for (auto &node : nodes) {
			auto arn = ArnOf(node, options.region);
			state.rows.push_back(
			    {HandleOf(arn, bind.input), Value(node.name), Value(arn), Value("ready"), MetadataOf(node)});
		}
	}
	EmitRows(state, output);
}

} // namespace

void RegisterGlueCatalogResource(ExtensionLoader &loader) {
	auto map_vv = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	loader.RegisterFunction(TableFunction("__glue__catalog__create", {map_vv}, CreateFun, CreateBind, ResourceInit));
	loader.RegisterFunction(TableFunction("__glue__catalog__status", {map_vv}, StatusFun, StatusBind, ResourceInit));
	loader.RegisterFunction(TableFunction("__glue__catalog__destroy", {map_vv}, DestroyFun, DestroyBind, ResourceInit));
	loader.RegisterFunction(TableFunction("__glue__catalog__list", {map_vv}, ListFun, ListBind, ResourceInit));

	ExternalResourceType type;
	type.name = GLUE_CATALOG_TYPE;
	type.kind = "catalog";
	type.create_function = "__glue__catalog__create";
	type.status_function = "__glue__catalog__status";
	type.destroy_function = "__glue__catalog__destroy";
	type.list_function = "__glue__catalog__list";
	type.origin = "extension";
	ExternalResourceTypeRegistry::Get(loader.GetDatabaseInstance()).Add(std::move(type));
}

} // namespace duckdb

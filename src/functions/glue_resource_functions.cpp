#include "functions/glue_functions.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_arn.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_options.hpp"
#include "api/glue_http_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/external_resource_type_registry.hpp"
#include "duckdb/main/external_resources_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <algorithm>

namespace duckdb {

//! The external resource type: a node of the account's catalog tree, or a database of one. Also stamped as a
//! parameter on every catalog and database the create callback makes, so they carry their own provenance.
static constexpr const char *GLUE_TYPE = "aws:glue";
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
//! 'secret', the same keys as the ATTACH options. A handle names its region through its ARN, a listing sweeps regions.
GlueAttachOptions OptionsOf(ClientContext &context, const Value &input, const string &catalog_id,
                            const string &region = string()) {
	GlueAttachOptions options;
	options.name = GLUE_TYPE;
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
			                                    GLUE_TYPE, secret_entry->secret->GetName().GetIdentifierName());
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

//! The catalog or database a handle names
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

//! What a catalog node is: 'native', 'federated:<connection type>' or 'redshift'
string KindOf(const GlueCatalogNodeInfo &node) {
	if (node.kind == "federated" && !node.connection_type.empty()) {
		return node.kind + ":" + node.connection_type;
	}
	return node.kind;
}

Value MetadataOf(const GlueCatalogNodeInfo &node) {
	return MapOf({Value("kind"), Value("type"), Value("catalog_id"), Value("description")},
	             {Value("catalog"), Value(KindOf(node)), Value(node.catalog_id), Value(node.description)});
}

//! The ARN of the database a resource link points at; its region is the link's when it names none
string TargetArnOf(const GlueDatabaseInfo &database, const string &region) {
	return DatabaseArn(database.target_region.empty() ? region : database.target_region, database.target_catalog_id,
	                   database.target_database);
}

Value MetadataOf(const GlueDatabaseInfo &database, const string &region) {
	return MapOf({Value("kind"), Value("type"), Value("target"), Value("location"), Value("description")},
	             {Value("database"), Value(database.is_resource_link ? "link" : "native"),
	              Value(database.is_resource_link ? TargetArnOf(database, region) : string()),
	              Value(database.location_uri), Value(database.description)});
}

//! The catalog the 'catalog' param names: an ARN, the name of a registered aws:glue resource, or a Glue catalog id;
//! without it the root catalog of the region
GlueArn CatalogParam(ClientContext &context, const Value &params, const string &region) {
	auto catalog = MapLookup(params, "catalog");
	GlueArn result;
	result.region = region;
	if (catalog.empty()) {
		return result;
	}
	if (!IsArn(catalog)) {
		auto registered = ExternalResourcesManager::Get(context).Lookup(catalog);
		if (registered) {
			if (registered->type != GLUE_TYPE) {
				throw InvalidInputException("%s: the registered resource '%s' is of type '%s', not a Glue catalog",
				                            GLUE_TYPE, catalog, registered->type);
			}
			catalog = MapLookup(registered->handle, "arn");
		} else {
			// a Glue catalog id starts with the account: '<account id>' or '<account id>:<catalog>/<nested>'
			auto account_id = catalog.substr(0, catalog.find(':'));
			if (account_id.size() != 12 ||
			    !std::all_of(account_id.begin(), account_id.end(), StringUtil::CharacterIsDigit)) {
				throw InvalidInputException("%s: 'catalog' must be a catalog ARN, the name of a registered resource "
				                            "or a Glue catalog id starting with the account id ('%s')",
				                            GLUE_TYPE, catalog);
			}
			result.catalog_id = catalog;
			return result;
		}
	}
	result = ParseGlueArn(catalog);
	if (!result.database.empty()) {
		throw InvalidInputException("%s: 'catalog' names a database, not a catalog ('%s')", GLUE_TYPE, catalog);
	}
	return result;
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
// Without 'path' or 'database' the root catalog of 'region' (default: the region of the secret), which exists in
// every region and is only resolved, never created. With 'path' a catalog under the root, which Glue only creates as
// a federated one, 'connection' and 'identifier' naming its source, or a Redshift-managed one, 'redshift_catalog_arn';
// a nested 'a/b/c' needs 'a/b' to exist. With 'database' a database in the catalog 'catalog' names (default: the
// root): with 'target', the ARN of a database anywhere, a resource link to it, else a database of its own, with
// 'location' for the files of its tables. CreateCatalog and CreateDatabase are synchronous, so the handle is of
// something that already exists. Also 'description', 'endpoint' and 'secret'.
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
		                  GLUE_TYPE, root.region);
	}
	return reported;
}

//! A database, or a resource link to the database 'target' names, in the catalog 'catalog' names
Value CreateDatabase(ClientContext &context, const Value &params, const GlueAttachOptions &root,
                     const string &database) {
	auto catalog = CatalogParam(context, params, root.region);
	auto catalog_id = catalog.catalog_id.empty() ? RootCatalogId(context, root) : catalog.catalog_id;
	auto options = OptionsOf(context, params, catalog_id, catalog.region);

	GlueDatabaseInfo input;
	input.name = database;
	input.description = MapLookup(params, "description");
	input.location_uri = MapLookup(params, "location");
	input.parameters[RESOURCE_TYPE_PARAMETER] = GLUE_TYPE;
	auto target = MapLookup(params, "target");
	if (!target.empty()) {
		if (!input.location_uri.empty()) {
			throw InvalidInputException("%s create: a resource link has no 'location' of its own", GLUE_TYPE);
		}
		auto target_arn = ParseGlueArn(target);
		if (target_arn.database.empty()) {
			throw InvalidInputException("%s create: 'target' must be the ARN of a database ('%s')", GLUE_TYPE, target);
		}
		input.target_catalog_id = target_arn.catalog_id;
		input.target_database = target_arn.database;
		input.target_region = target_arn.region;
	}
	if (GlueAPI::CreateDatabase(context, options, input) == GlueCreateOutcome::ALREADY_EXISTED) {
		throw InvalidInputException("%s create: database '%s' already exists in catalog '%s'", GLUE_TYPE, database,
		                            catalog_id);
	}
	return HandleOf(DatabaseArn(options.region, catalog_id, database), params);
}

void CreateFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ResourceState>();
	auto &bind = data_p.bind_data->Cast<ResourceBindData>();
	if (!state.ran) {
		state.ran = true;
		auto root = OptionsOf(context, bind.input, string());
		auto path = MapLookup(bind.input, "path");
		auto database = MapLookup(bind.input, "database");
		if (!path.empty() && !database.empty()) {
			throw InvalidInputException("%s create: give 'path' for a catalog or 'database' for a database, not both",
			                            GLUE_TYPE);
		}
		if (!database.empty()) {
			state.rows.push_back({CreateDatabase(context, bind.input, root, database)});
			EmitRows(state, output);
			return;
		}
		if (path.empty()) {
			state.rows.push_back({HandleOf(CatalogArn(root.region, RootCatalogId(context, root)), bind.input)});
			EmitRows(state, output);
			return;
		}

		GlueCatalogInput input;
		input.name = path;
		input.description = MapLookup(bind.input, "description");
		input.parameters[RESOURCE_TYPE_PARAMETER] = GLUE_TYPE;
		input.connection_name = MapLookup(bind.input, "connection");
		input.connection_type = MapLookup(bind.input, "connection_type");
		input.identifier = MapLookup(bind.input, "identifier");
		input.redshift_catalog_arn = MapLookup(bind.input, "redshift_catalog_arn");
		if (input.connection_name.empty() == input.identifier.empty() && input.connection_name.empty() &&
		    input.redshift_catalog_arn.empty()) {
			throw InvalidInputException("%s create: Glue creates no native catalog below the root: give 'connection' "
			                            "and 'identifier' for a federated catalog or 'redshift_catalog_arn' for a "
			                            "Redshift-managed one, or no 'path' for the root catalog of the region",
			                            GLUE_TYPE);
		}
		if (input.connection_name.empty() != input.identifier.empty()) {
			throw InvalidInputException("%s create: a federated catalog needs both 'connection' and 'identifier'",
			                            GLUE_TYPE);
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
				throw InvalidInputException("%s create: the parent catalog '%s' of '%s' does not exist", GLUE_TYPE,
				                            parent, path);
			}
		}
		GlueAPI::CreateCatalog(context, root, input);

		// the handle is the ARN, which Glue reports for the created catalog
		GlueCatalogNodeInfo node;
		if (!GlueAPI::GetCatalog(context, OptionsOf(context, bind.input, account_id + ":" + path, root.region), node)) {
			throw IOException("%s create: catalog '%s' is not found after CreateCatalog", GLUE_TYPE, path);
		}
		state.rows.push_back({HandleOf(ArnOf(node, root.region), bind.input)});
	}
	EmitRows(state, output);
}

//===--------------------------------------------------------------------===//
// status(handle MAP) -> TABLE(state VARCHAR, result MAP)
//
// A catalog or a database exists or it does not, so one call decides 'ready': one page of a catalog's databases, or
// the database itself; the call fails with Glue's error otherwise. REGISTER EXTERNAL RESOURCE resolves a user-supplied
// handle through here, so a bad ARN fails now rather than at the first query. The result is what ATTACH takes: the
// ARN as the path (a database ARN attaches its catalog with the database as the default schema), 'glue' as the type,
// and the endpoint and secret of the handle as options.
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
		auto arn = HandleArn(bind.input, "aws:glue status");
		auto options = OptionsOf(context, bind.input, arn.catalog_id, arn.region);
		if (arn.database.empty()) {
			GlueAPI::GetReportedCatalogId(context, options);
		} else {
			GlueDatabaseInfo database;
			if (!GlueAPI::GetDatabase(context, options, arn.database, database)) {
				throw InvalidInputException("%s status: database '%s' does not exist in catalog '%s'", GLUE_TYPE,
				                            arn.database, arn.catalog_id);
			}
		}

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
// DeleteDatabase on the database the handle names (a resource link: the link alone, its target is untouched), or
// DeleteCatalog on the catalog it names, after the catalogs under it, deepest first. The root catalog of a region
// exists whether or not anything refers to it, so destroying it does nothing and reports 'kept'.
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
		auto arn = HandleArn(bind.input, "aws:glue destroy");
		if (!arn.database.empty()) {
			GlueAPI::DeleteDatabase(context, OptionsOf(context, bind.input, arn.catalog_id, arn.region), arn.database);
			state.rows.push_back({Value("deleted")});
			EmitRows(state, output);
			return;
		}
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
// Every node of the catalog tree, the default catalog and the catalogs under it, native, federated or managed, and
// the databases of the root, native ones and resource links; the databases inside the other nodes are the nodes' own
// structure and are listed with 'catalog' naming the node. Where Glue has no catalogs API (a local server) or refuses
// it, the root alone stands for the tree. A catalog tree is per region: 'region' lists that one, its errors thrown;
// without it the default regions are swept in parallel, as the aws extension lists table buckets, and a region that
// refuses (not enabled for the account, no permission) is left out.
//===--------------------------------------------------------------------===//

//! The regions a sweep covers: the default-enabled commercial regions, the list the aws extension sweeps
const vector<string> &DefaultAwsRegions() {
	static const vector<string> regions = {
	    "us-east-1",      "us-east-2",      "us-west-1",      "us-west-2",      "ca-central-1",  "sa-east-1",
	    "eu-west-1",      "eu-west-2",      "eu-west-3",      "eu-central-1",   "eu-north-1",    "ap-south-1",
	    "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-southeast-1", "ap-southeast-2"};
	return regions;
}

//! What one region's tree lists: its nodes and the databases of its root, or of the one catalog asked for
struct RegionListing {
	vector<GlueCatalogNodeInfo> nodes;
	vector<GlueDatabaseInfo> databases;
};

//! The listing of one region, or of one catalog of it ('catalog_only'); empty when the region refuses, unless 'rethrow'
RegionListing RegionNodes(ClientContext &context, const GlueAttachOptions &options, bool catalog_only, bool rethrow) {
	RegionListing result;
	try {
		if (catalog_only) {
			result.databases = GlueAPI::GetDatabases(context, options);
			return result;
		}
		// the databases first: where the catalogs API is unavailable, they are also what tells the root's id
		result.databases = GlueAPI::GetDatabases(context, options);
		try {
			result.nodes = GlueAPI::GetCatalogs(context, options);
		} catch (GlueException &) {
			GlueCatalogNodeInfo root;
			for (auto &database : result.databases) {
				if (!database.is_resource_link && !database.catalog_id.empty()) {
					root.catalog_id = database.catalog_id;
					break;
				}
			}
			if (!root.catalog_id.empty()) {
				root.name = root.catalog_id;
				root.kind = "native";
				result.nodes.push_back(std::move(root));
			}
		}
	} catch (std::exception &) {
		if (rethrow) {
			throw;
		}
		return RegionListing();
	}
	return result;
}

class ListRegionTask : public BaseExecutorTask {
public:
	ListRegionTask(TaskExecutor &executor, ClientContext &context, GlueAttachOptions options, RegionListing &result)
	    : BaseExecutorTask(executor), context(context), options(std::move(options)), result(result) {
	}

	void ExecuteTask() override {
		GlueHttpClientContextScope http_scope(context);
		result = RegionNodes(context, options, false, false);
	}
	string TaskType() const override {
		return "ListRegionTask";
	}

private:
	ClientContext &context;
	GlueAttachOptions options;
	RegionListing &result;
};

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
		// a sweep leaves out the regions that refuse, so what is wrong for every region is checked first
		GlueCatalog::GetStorageSecret(context, StringUtil::Lower(MapLookup(bind.input, "secret")));
		auto region = MapLookup(bind.input, "region");
		auto catalog = CatalogParam(context, bind.input, region);
		if (!catalog.catalog_id.empty()) {
			// the databases of one catalog: its region when it is an ARN's, else the one asked for
			auto options = OptionsOf(context, bind.input, catalog.catalog_id, catalog.region);
			auto listing = RegionNodes(context, options, true, true);
			for (auto &database : listing.databases) {
				auto arn = DatabaseArn(options.region, catalog.catalog_id, database.name);
				state.rows.push_back({HandleOf(arn, bind.input), Value(database.name), Value(arn), Value("ready"),
				                      MetadataOf(database, options.region)});
			}
			EmitRows(state, output);
			return;
		}
		auto regions = region.empty() ? DefaultAwsRegions() : vector<string> {region};
		vector<RegionListing> listings(regions.size());
		if (regions.size() == 1) {
			listings[0] = RegionNodes(context, OptionsOf(context, bind.input, string(), regions[0]), false, true);
		} else {
			TaskExecutor executor(context, TaskSchedulerType::ASYNC);
			for (idx_t i = 0; i < regions.size(); i++) {
				executor.ScheduleTask(make_uniq<ListRegionTask>(
				    executor, context, OptionsOf(context, bind.input, string(), regions[i]), listings[i]));
			}
			executor.WorkOnTasks();
		}
		for (idx_t i = 0; i < regions.size(); i++) {
			auto &listing = listings[i];
			for (auto &node : listing.nodes) {
				auto arn = ArnOf(node, regions[i]);
				state.rows.push_back(
				    {HandleOf(arn, bind.input), Value(node.name), Value(arn), Value("ready"), MetadataOf(node)});
			}
			// the root's databases, under the root's id; a region without a known root lists none
			string root_id;
			for (auto &node : listing.nodes) {
				if (node.catalog_id.find(':') == string::npos) {
					root_id = node.catalog_id;
				}
			}
			if (root_id.empty()) {
				continue;
			}
			for (auto &database : listing.databases) {
				auto arn = DatabaseArn(regions[i], root_id, database.name);
				state.rows.push_back({HandleOf(arn, bind.input), Value(database.name), Value(arn), Value("ready"),
				                      MetadataOf(database, regions[i])});
			}
		}
	}
	EmitRows(state, output);
}

} // namespace

void RegisterGlueResource(ExtensionLoader &loader) {
	auto map_vv = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	loader.RegisterFunction(TableFunction("__glue__create", {map_vv}, CreateFun, CreateBind, ResourceInit));
	loader.RegisterFunction(TableFunction("__glue__status", {map_vv}, StatusFun, StatusBind, ResourceInit));
	loader.RegisterFunction(TableFunction("__glue__destroy", {map_vv}, DestroyFun, DestroyBind, ResourceInit));
	loader.RegisterFunction(TableFunction("__glue__list", {map_vv}, ListFun, ListBind, ResourceInit));

	ExternalResourceType type;
	type.name = GLUE_TYPE;
	type.kind = "catalog";
	type.create_function = "__glue__create";
	type.status_function = "__glue__status";
	type.destroy_function = "__glue__destroy";
	type.list_function = "__glue__list";
	type.origin = "extension";
	ExternalResourceTypeRegistry::Get(loader.GetDatabaseInstance()).Add(std::move(type));
}

} // namespace duckdb

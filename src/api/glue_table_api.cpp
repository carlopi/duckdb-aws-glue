#include "api/glue_api_util.hpp"

#include "duckdb/common/string_util.hpp"

#include <functional>

namespace duckdb {

vector<GlueTableInfo> GlueAPI::GetTables(ClientContext &context, GlueCatalog &catalog, const string &database_name) {
	vector<GlueTableInfo> result;
	string next_token;
	do {
		GlueRequest request(catalog);
		request.SetString(request.Root(), "DatabaseName", database_name);
		if (!next_token.empty()) {
			request.SetString(request.Root(), "NextToken", next_token);
		}
		auto response = Call(context, catalog, "GetTables", request);
		if (!response.IsSuccess()) {
			response.Throw(StringUtil::Format("GetTables (database '%s')", database_name));
		}
		for (auto table : GlueJsonElements(response.Get("TableList"))) {
			result.push_back(ToTableInfo(table));
		}
		next_token = response.GetString("NextToken");
	} while (!next_token.empty());
	return result;
}

bool GlueAPI::GetAnyTableName(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              string &table_name) {
	string next_token;
	do {
		GlueRequest request(catalog);
		request.SetString(request.Root(), "DatabaseName", database_name);
		request.SetInteger(request.Root(), "MaxResults", 1);
		if (!next_token.empty()) {
			request.SetString(request.Root(), "NextToken", next_token);
		}
		auto response = Call(context, catalog, "GetTables", request);
		if (!response.IsSuccess()) {
			response.Throw(StringUtil::Format("GetTables (database '%s')", database_name));
		}
		auto tables = GlueJsonElements(response.Get("TableList"));
		if (!tables.empty()) {
			table_name = GlueJsonString(tables[0], "Name");
			return true;
		}
		next_token = response.GetString("NextToken");
	} while (!next_token.empty());
	return false;
}

bool GlueAPI::GetTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                       const string &table_name, GlueTableInfo &result, string *raw_json) {
	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "Name", table_name);
	auto response = Call(context, catalog, "GetTable", request);
	if (!response.IsSuccess()) {
		if (response.IsEntityNotFound()) {
			return false;
		}
		response.Throw(StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto table = response.Get("Table");
	result = ToTableInfo(table);
	if (raw_json) {
		*raw_json = GlueJsonToString(table);
	}
	return true;
}

void GlueAPI::CreateHiveTable(ClientContext &context, GlueCatalog &catalog, const GlueTableInfo &table) {
	CheckWritable(catalog, "CreateTable");
	if (table.location.empty()) {
		throw InvalidInputException("Can not create Hive table '%s.%s' without a location", table.database_name,
		                            table.name);
	}

	// An external table, described the way Hive / Athena / Spark expect it for the file format
	string serde_library;
	string input_format;
	string output_format;
	unordered_map<string, string> serde_parameters;
	auto parameters = table.parameters;
	switch (table.file_format) {
	case HiveFileFormat::PARQUET:
		serde_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
		serde_parameters.emplace("serialization.format", "1");
		input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
		output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
		parameters.emplace("classification", "parquet");
		break;
	case HiveFileFormat::CSV:
		if (table.csv_quote.empty() && table.csv_escape.empty()) {
			// Hive's "ROW FORMAT DELIMITED FIELDS TERMINATED BY '<delimiter>'": no quoting
			serde_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
			serde_parameters.emplace("field.delim", table.csv_delimiter);
			serde_parameters.emplace("serialization.format", table.csv_delimiter);
		} else {
			// quoted fields: Hive's "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH
			// SERDEPROPERTIES (...)", the escape character defaults to the quote character like DuckDB's COPY
			auto quote = table.csv_quote.empty() ? "\"" : table.csv_quote;
			auto escape = table.csv_escape.empty() ? quote : table.csv_escape;
			serde_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
			serde_parameters.emplace("separatorChar", table.csv_delimiter);
			serde_parameters.emplace("quoteChar", quote);
			serde_parameters.emplace("escapeChar", escape);
		}
		input_format = "org.apache.hadoop.mapred.TextInputFormat";
		output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
		parameters.emplace("classification", "csv");
		parameters.emplace("delimiter", table.csv_delimiter);
		break;
	case HiveFileFormat::JSON:
		// one JSON object per line
		serde_library = "org.apache.hive.hcatalog.data.JsonSerDe";
		input_format = "org.apache.hadoop.mapred.TextInputFormat";
		output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
		parameters.emplace("classification", "json");
		break;
	case HiveFileFormat::AVRO:
		// Avro container files, the schema travels in the files
		serde_library = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
		input_format = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
		output_format = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
		parameters.emplace("classification", "avro");
		break;
	}
	parameters.emplace("EXTERNAL", "TRUE");

	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", table.database_name);
	auto table_input = request.SetObject(request.Root(), "TableInput");
	request.SetString(table_input, "Name", table.name);
	request.SetString(table_input, "TableType", "EXTERNAL_TABLE");
	auto storage_descriptor = request.SetObject(table_input, "StorageDescriptor");
	request.SetString(storage_descriptor, "InputFormat", input_format);
	request.SetString(storage_descriptor, "OutputFormat", output_format);
	request.SetString(storage_descriptor, "Location", table.location);
	request.SetColumns(storage_descriptor, "Columns", table.columns);
	auto serde_info = request.SetObject(storage_descriptor, "SerdeInfo");
	request.SetString(serde_info, "SerializationLibrary", serde_library);
	if (!serde_parameters.empty()) {
		request.SetMap(serde_info, "Parameters", serde_parameters);
	}
	request.SetBoolean(storage_descriptor, "Compressed", false);
	request.SetInteger(storage_descriptor, "NumberOfBuckets", table.number_of_buckets);
	if (!table.bucket_columns.empty()) {
		request.SetStrings(storage_descriptor, "BucketColumns", table.bucket_columns);
	}
	if (!table.sort_columns.empty()) {
		auto sort_columns = request.SetArray(storage_descriptor, "SortColumns");
		for (auto &sort_column : table.sort_columns) {
			auto order = request.AppendObject(sort_columns);
			request.SetString(order, "Column", sort_column.name);
			// Glue's SortOrder is 1 for ascending, 0 for descending
			request.SetInteger(order, "SortOrder", sort_column.sort_order == GlueSortOrder::DESCENDING ? 0 : 1);
		}
	}
	if (!table.partition_keys.empty()) {
		request.SetColumns(table_input, "PartitionKeys", table.partition_keys);
	}
	request.SetMap(table_input, "Parameters", parameters);

	auto response = Call(context, catalog, "CreateTable", request);
	if (!response.IsSuccess()) {
		if (response.IsAlreadyExists()) {
			throw CatalogException("Table with name \"%s\" already exists in Glue database \"%s\"", table.name,
			                       table.database_name);
		}
		response.Throw(
		    StringUtil::Format("CreateTable '%s.%s' (location '%s')", table.database_name, table.name, table.location));
	}
}

//! The parameters that mark a view as written by DuckDB
static void SetViewParameters(GlueRequest &request, yyjson_mut_val *table_input, const GlueViewInfo &view) {
	auto parameters = request.Get(table_input, "Parameters");
	if (!parameters) {
		parameters = request.SetObject(table_input, "Parameters");
	}
	request.SetString(parameters, "duckdb_view", "true");
	request.SetString(parameters, "duckdb_view_version", "1");
	// the database the unqualified names in the SQL belong to (the one the view is created in)
	request.SetString(parameters, "duckdb_view_default_database", view.database_name);
	if (view.secure) {
		request.SetString(parameters, "duckdb_view_secure", "true");
	} else {
		request.Remove(parameters, "duckdb_view_secure");
	}
}

//! Glue's (Hive's) two view fields: the SQL as written and the SQL with names resolved. Both are free text and readers
//! differ in which one they parse, so both carry the same DuckDB SQL - there is no separate expansion
static void SetViewDefinition(GlueRequest &request, yyjson_mut_val *table_input, const GlueViewInfo &view) {
	request.SetString(table_input, "TableType", "VIRTUAL_VIEW");
	request.SetString(table_input, "ViewOriginalText", view.sql);
	request.SetString(table_input, "ViewExpandedText", view.sql);
	auto storage_descriptor = request.SetObject(table_input, "StorageDescriptor");
	if (!view.columns.empty()) {
		request.SetColumns(storage_descriptor, "Columns", view.columns);
	}
}

void GlueAPI::CreateView(ClientContext &context, GlueCatalog &catalog, const GlueViewInfo &view) {
	CheckWritable(catalog, "CreateView");
	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", view.database_name);
	auto table_input = request.SetObject(request.Root(), "TableInput");
	request.SetString(table_input, "Name", view.name);
	SetViewDefinition(request, table_input, view);
	SetViewParameters(request, table_input, view);
	auto response = Call(context, catalog, "CreateTable", request);
	if (!response.IsSuccess()) {
		if (response.IsAlreadyExists()) {
			throw CatalogException("View with name \"%s\" already exists in Glue database \"%s\"", view.name,
			                       view.database_name);
		}
		response.Throw(StringUtil::Format("CreateView '%s.%s'", view.database_name, view.name));
	}
}

//! UpdateTable replaces the whole definition: fetch the current one, let 'modify' change the TableInput built from
//! it, and send it back
static void UpdateGlueTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                            const string &table_name,
                            const std::function<void(GlueRequest &, yyjson_mut_val *)> &modify) {
	GlueRequest get_request(catalog);
	get_request.SetString(get_request.Root(), "DatabaseName", database_name);
	get_request.SetString(get_request.Root(), "Name", table_name);
	auto get_response = GlueAPI::Call(context, catalog, "GetTable", get_request);
	if (!get_response.IsSuccess()) {
		if (get_response.IsEntityNotFound()) {
			throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
			                       database_name);
		}
		get_response.Throw(StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto table = get_response.Get("Table");

	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	auto table_input = request.SetObject(request.Root(), "TableInput");
	// the members of a Table that a TableInput takes as well
	for (auto key : {"Name", "Description", "Owner", "LastAccessTime", "LastAnalyzedTime", "Retention", "PartitionKeys",
	                 "ViewOriginalText", "ViewExpandedText", "TableType", "Parameters", "TargetTable"}) {
		request.Copy(table_input, key, GlueJsonGet(table, key));
	}
	if (!request.Copy(table_input, "StorageDescriptor", GlueJsonGet(table, "StorageDescriptor"))) {
		request.SetObject(table_input, "StorageDescriptor");
	}
	modify(request, table_input);

	auto response = GlueAPI::Call(context, catalog, "UpdateTable", request);
	if (!response.IsSuccess()) {
		response.Throw(StringUtil::Format("UpdateTable '%s.%s'", database_name, table_name));
	}
}

void GlueAPI::UpdateView(ClientContext &context, GlueCatalog &catalog, const GlueViewInfo &view) {
	CheckWritable(catalog, "UpdateView");
	// CREATE OR REPLACE keeps what is not the definition: the description, owner and parameters other engines or
	// users set stay, the SQL, the columns and our own parameters are replaced
	UpdateGlueTable(context, catalog, view.database_name, view.name,
	                [&](GlueRequest &request, yyjson_mut_val *table_input) {
		                SetViewDefinition(request, table_input, view);
		                SetViewParameters(request, table_input, view);
	                });
}

void GlueAPI::UpdateTableColumns(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                 const string &table_name, const vector<GlueColumn> &columns) {
	CheckWritable(catalog, "UpdateTable");
	UpdateGlueTable(context, catalog, database_name, table_name,
	                [&](GlueRequest &request, yyjson_mut_val *table_input) {
		                request.SetColumns(request.Get(table_input, "StorageDescriptor"), "Columns", columns);
	                });
}

void GlueAPI::SetTableLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                               const string &table_name, const string &location) {
	CheckWritable(catalog, "UpdateTable");
	UpdateGlueTable(context, catalog, database_name, table_name,
	                [&](GlueRequest &request, yyjson_mut_val *table_input) {
		                request.SetString(request.Get(table_input, "StorageDescriptor"), "Location", location);
	                });
}

void GlueAPI::UpdateTableParameters(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const string &table_name, const vector<pair<string, string>> &set,
                                    const vector<string> &unset) {
	// Reached only through ALTER TABLE, which the binder refuses on a read-only attach before it gets here.
	UpdateGlueTable(context, catalog, database_name, table_name,
	                [&](GlueRequest &request, yyjson_mut_val *table_input) {
		                auto parameters = request.Get(table_input, "Parameters");
		                if (!parameters) {
			                parameters = request.SetObject(table_input, "Parameters");
		                }
		                for (auto &key : unset) {
			                request.Remove(parameters, key);
		                }
		                for (auto &entry : set) {
			                request.SetString(parameters, entry.first, entry.second);
		                }
	                });
}

void GlueAPI::DeleteTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                          const string &table_name) {
	CheckWritable(catalog, "DeleteTable");
	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "Name", table_name);
	auto response = Call(context, catalog, "DeleteTable", request);
	if (!response.IsSuccess()) {
		if (response.IsEntityNotFound()) {
			throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
			                       database_name);
		}
		response.Throw(StringUtil::Format("DeleteTable '%s.%s'", database_name, table_name));
	}
}

} // namespace duckdb

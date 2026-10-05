#include "api/glue_api_util.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//===--------------------------------------------------------------------===//
// Request
//===--------------------------------------------------------------------===//
GlueRequest::GlueRequest(const GlueCatalog &catalog) : doc(yyjson_mut_doc_new(nullptr)) {
	root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	if (!catalog.options.catalog_id.empty()) {
		SetString(root, "CatalogId", catalog.options.catalog_id);
	}
}

GlueRequest::~GlueRequest() {
	yyjson_mut_doc_free(doc);
}

void GlueRequest::Set(yyjson_mut_val *parent, const string &key, yyjson_mut_val *value) {
	yyjson_mut_obj_remove_keyn(parent, key.c_str(), key.size());
	yyjson_mut_obj_add(parent, yyjson_mut_strncpy(doc, key.c_str(), key.size()), value);
}

yyjson_mut_val *GlueRequest::SetObject(yyjson_mut_val *parent, const string &key) {
	auto result = yyjson_mut_obj(doc);
	Set(parent, key, result);
	return result;
}

yyjson_mut_val *GlueRequest::SetArray(yyjson_mut_val *parent, const string &key) {
	auto result = yyjson_mut_arr(doc);
	Set(parent, key, result);
	return result;
}

yyjson_mut_val *GlueRequest::AppendObject(yyjson_mut_val *array) {
	auto result = yyjson_mut_obj(doc);
	yyjson_mut_arr_append(array, result);
	return result;
}

void GlueRequest::SetString(yyjson_mut_val *parent, const string &key, const string &value) {
	Set(parent, key, yyjson_mut_strncpy(doc, value.c_str(), value.size()));
}

void GlueRequest::SetInteger(yyjson_mut_val *parent, const string &key, int64_t value) {
	Set(parent, key, yyjson_mut_sint(doc, value));
}

void GlueRequest::SetBoolean(yyjson_mut_val *parent, const string &key, bool value) {
	Set(parent, key, yyjson_mut_bool(doc, value));
}

void GlueRequest::SetStrings(yyjson_mut_val *parent, const string &key, const vector<string> &values) {
	auto array = SetArray(parent, key);
	for (auto &value : values) {
		yyjson_mut_arr_add_strncpy(doc, array, value.c_str(), value.size());
	}
}

void GlueRequest::SetMap(yyjson_mut_val *parent, const string &key, const unordered_map<string, string> &values) {
	auto object = SetObject(parent, key);
	for (auto &entry : values) {
		SetString(object, entry.first, entry.second);
	}
}

void GlueRequest::SetColumns(yyjson_mut_val *parent, const string &key, const vector<GlueColumn> &columns) {
	auto array = SetArray(parent, key);
	for (auto &column : columns) {
		auto object = AppendObject(array);
		SetString(object, "Name", column.name);
		SetString(object, "Type", column.type);
		if (!column.comment.empty()) {
			SetString(object, "Comment", column.comment);
		}
	}
}

yyjson_mut_val *GlueRequest::Copy(yyjson_mut_val *parent, const string &key, yyjson_val *source) {
	if (!source) {
		return nullptr;
	}
	auto result = yyjson_val_mut_copy(doc, source);
	Set(parent, key, result);
	return result;
}

yyjson_mut_val *GlueRequest::Get(yyjson_mut_val *parent, const string &key) const {
	return yyjson_mut_obj_getn(parent, key.c_str(), key.size());
}

void GlueRequest::Remove(yyjson_mut_val *parent, const string &key) {
	yyjson_mut_obj_remove_keyn(parent, key.c_str(), key.size());
}

string GlueRequest::ToString() const {
	size_t length;
	auto json = yyjson_mut_write(doc, 0, &length);
	if (!json) {
		throw InternalException("Failed to serialize Glue request");
	}
	string result(json, length);
	free(json);
	return result;
}

//===--------------------------------------------------------------------===//
// Response
//===--------------------------------------------------------------------===//
yyjson_val *GlueResponse::Get(const char *key) const {
	if (!document) {
		return nullptr;
	}
	return GlueJsonGet(yyjson_doc_get_root(document.get()), key);
}

string GlueResponse::GetString(const char *key) const {
	if (!document) {
		return string();
	}
	return GlueJsonString(yyjson_doc_get_root(document.get()), key);
}

void GlueResponse::Throw(const string &operation) const {
	throw IOException("Glue %s failed: %s (%s)", operation, error_message, error_type);
}

shared_ptr<yyjson_doc> ParseGlueJson(const string &json) {
	auto document = yyjson_read(json.c_str(), json.size(), 0);
	if (!document) {
		return nullptr;
	}
	return shared_ptr<yyjson_doc>(document, yyjson_doc_free);
}

yyjson_val *GlueJsonGet(yyjson_val *object, const char *key) {
	if (!object || !yyjson_is_obj(object)) {
		return nullptr;
	}
	return yyjson_obj_get(object, key);
}

static string JsonToString(yyjson_val *value) {
	if (!value || !yyjson_is_str(value)) {
		return string();
	}
	return string(yyjson_get_str(value), yyjson_get_len(value));
}

string GlueJsonString(yyjson_val *object, const char *key) {
	return JsonToString(GlueJsonGet(object, key));
}

vector<yyjson_val *> GlueJsonElements(yyjson_val *array) {
	vector<yyjson_val *> result;
	if (!array || !yyjson_is_arr(array)) {
		return result;
	}
	yyjson_arr_iter iterator = yyjson_arr_iter_with(array);
	yyjson_val *value;
	while ((value = yyjson_arr_iter_next(&iterator))) {
		result.push_back(value);
	}
	return result;
}

vector<string> GlueJsonStrings(yyjson_val *array) {
	vector<string> result;
	for (auto value : GlueJsonElements(array)) {
		result.push_back(JsonToString(value));
	}
	return result;
}

unordered_map<string, string> GlueJsonMap(yyjson_val *object) {
	unordered_map<string, string> result;
	if (!object || !yyjson_is_obj(object)) {
		return result;
	}
	yyjson_obj_iter iterator = yyjson_obj_iter_with(object);
	yyjson_val *key;
	while ((key = yyjson_obj_iter_next(&iterator))) {
		result.emplace(JsonToString(key), JsonToString(yyjson_obj_iter_get_val(key)));
	}
	return result;
}

string GlueJsonToString(yyjson_val *value) {
	if (!value) {
		return string();
	}
	size_t length;
	auto json = yyjson_val_write(value, YYJSON_WRITE_PRETTY, &length);
	if (!json) {
		throw InternalException("Failed to serialize Glue response");
	}
	string result(json, length);
	free(json);
	return result;
}

//===--------------------------------------------------------------------===//
// Conversion
//===--------------------------------------------------------------------===//
vector<GlueColumn> ToColumns(yyjson_val *columns) {
	vector<GlueColumn> result;
	for (auto column : GlueJsonElements(columns)) {
		GlueColumn glue_column;
		glue_column.name = GlueJsonString(column, "Name");
		glue_column.type = GlueJsonString(column, "Type");
		glue_column.comment = GlueJsonString(column, "Comment");
		result.push_back(std::move(glue_column));
	}
	return result;
}

GlueDatabaseInfo ToDatabaseInfo(yyjson_val *database) {
	GlueDatabaseInfo result;
	result.name = GlueJsonString(database, "Name");
	result.description = GlueJsonString(database, "Description");
	result.location_uri = GlueJsonString(database, "LocationUri");
	result.parameters = GlueJsonMap(GlueJsonGet(database, "Parameters"));
	return result;
}

GlueTableInfo ToTableInfo(yyjson_val *table) {
	GlueTableInfo result;
	result.name = GlueJsonString(table, "Name");
	result.database_name = GlueJsonString(table, "DatabaseName");
	result.glue_table_type = GlueJsonString(table, "TableType");
	result.table_type = GlueTableTypeFromString(result.glue_table_type);
	result.view_original_text = GlueJsonString(table, "ViewOriginalText");
	result.view_expanded_text = GlueJsonString(table, "ViewExpandedText");
	result.description = GlueJsonString(table, "Description");
	auto storage_descriptor = GlueJsonGet(table, "StorageDescriptor");
	result.location = GlueJsonString(storage_descriptor, "Location");
	result.input_format = GlueJsonString(storage_descriptor, "InputFormat");
	result.output_format = GlueJsonString(storage_descriptor, "OutputFormat");
	auto serde_info = GlueJsonGet(storage_descriptor, "SerdeInfo");
	result.serde_library = GlueJsonString(serde_info, "SerializationLibrary");
	result.serde_parameters = GlueJsonMap(GlueJsonGet(serde_info, "Parameters"));
	result.columns = ToColumns(GlueJsonGet(storage_descriptor, "Columns"));
	result.partition_keys = ToColumns(GlueJsonGet(table, "PartitionKeys"));
	result.bucket_columns = GlueJsonStrings(GlueJsonGet(storage_descriptor, "BucketColumns"));
	auto number_of_buckets = GlueJsonGet(storage_descriptor, "NumberOfBuckets");
	if (number_of_buckets && yyjson_is_num(number_of_buckets)) {
		result.number_of_buckets = NumericCast<int32_t>(static_cast<int64_t>(yyjson_get_num(number_of_buckets)));
	}
	for (auto column : GlueJsonElements(GlueJsonGet(storage_descriptor, "SortColumns"))) {
		GlueColumn sort_column;
		sort_column.name = GlueJsonString(column, "Column");
		// Glue's SortOrder is 1 for ascending, 0 for descending
		auto sort_order = GlueJsonGet(column, "SortOrder");
		auto descending = sort_order && yyjson_is_num(sort_order) && yyjson_get_num(sort_order) == 0;
		sort_column.sort_order = descending ? GlueSortOrder::DESCENDING : GlueSortOrder::ASCENDING;
		result.sort_columns.push_back(std::move(sort_column));
	}
	result.parameters = GlueJsonMap(GlueJsonGet(table, "Parameters"));
	return result;
}

GluePartitionInfo ToPartitionInfo(yyjson_val *partition) {
	GluePartitionInfo result;
	result.values = GlueJsonStrings(GlueJsonGet(partition, "Values"));
	result.location = GlueJsonString(GlueJsonGet(partition, "StorageDescriptor"), "Location");
	return result;
}

string PartitionValuesToString(const vector<string> &values) {
	return StringUtil::Join(values, ", ");
}

void CheckWritable(const GlueCatalog &catalog, const string &operation) {
	if (catalog.access_mode == AccessMode::READ_ONLY) {
		throw InvalidInputException("Cannot execute Glue %s on database %s which is attached in read-only mode!",
		                            operation, catalog.GetName());
	}
}

} // namespace duckdb

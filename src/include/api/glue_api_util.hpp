#pragma once

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include "yyjson.hpp"

namespace duckdb {

using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_val;
using duckdb_yyjson::yyjson_val;

//! The JSON body of a Glue API request. Setting a member replaces a member with the same name.
class GlueRequest {
public:
	//! An empty request object, naming the catalog unless it is the default one of the account
	explicit GlueRequest(const GlueCatalog &catalog);
	~GlueRequest();
	GlueRequest(const GlueRequest &) = delete;
	GlueRequest &operator=(const GlueRequest &) = delete;

public:
	yyjson_mut_val *Root() const {
		return root;
	}
	yyjson_mut_val *SetObject(yyjson_mut_val *parent, const string &key);
	yyjson_mut_val *SetArray(yyjson_mut_val *parent, const string &key);
	yyjson_mut_val *AppendObject(yyjson_mut_val *array);
	void SetString(yyjson_mut_val *parent, const string &key, const string &value);
	void SetInteger(yyjson_mut_val *parent, const string &key, int64_t value);
	void SetBoolean(yyjson_mut_val *parent, const string &key, bool value);
	void SetStrings(yyjson_mut_val *parent, const string &key, const vector<string> &values);
	void SetMap(yyjson_mut_val *parent, const string &key, const unordered_map<string, string> &values);
	void SetColumns(yyjson_mut_val *parent, const string &key, const vector<GlueColumn> &columns);
	//! Copy a value of a response below 'key', returns nullptr (and sets nothing) if 'source' is nullptr
	yyjson_mut_val *Copy(yyjson_mut_val *parent, const string &key, yyjson_val *source);
	//! The member 'key' of 'parent', nullptr if there is none
	yyjson_mut_val *Get(yyjson_mut_val *parent, const string &key) const;
	void Remove(yyjson_mut_val *parent, const string &key);
	string ToString() const;

private:
	void Set(yyjson_mut_val *parent, const string &key, yyjson_mut_val *value);

private:
	yyjson_mut_doc *doc;
	yyjson_mut_val *root;
};

//! The answer of Glue to a request: the JSON object of a successful call, or the error
class GlueResponse {
public:
	bool IsSuccess() const {
		return error_type.empty();
	}
	bool IsEntityNotFound() const {
		return error_type == "EntityNotFoundException";
	}
	bool IsAlreadyExists() const {
		return error_type == "AlreadyExistsException";
	}
	//! The member 'key' of the response object, nullptr if there is none
	yyjson_val *Get(const char *key) const;
	//! The string member 'key' of the response object, empty if there is none
	string GetString(const char *key) const;
	[[noreturn]] void Throw(const string &operation) const;

public:
	//! The Glue exception name (e.g. EntityNotFoundException), empty if the call succeeded
	string error_type;
	string error_message;
	shared_ptr<yyjson_doc> document;
};

//! Parse a JSON document, nullptr if 'json' is not valid JSON
shared_ptr<yyjson_doc> ParseGlueJson(const string &json);
//! The member 'key' of a JSON object, nullptr if 'object' is not an object or has no such member
yyjson_val *GlueJsonGet(yyjson_val *object, const char *key);
//! The string member 'key' of a JSON object, empty if absent
string GlueJsonString(yyjson_val *object, const char *key);
//! The elements of a JSON array, empty if 'array' is not an array
vector<yyjson_val *> GlueJsonElements(yyjson_val *array);
vector<string> GlueJsonStrings(yyjson_val *array);
unordered_map<string, string> GlueJsonMap(yyjson_val *object);
string GlueJsonToString(yyjson_val *value);

vector<GlueColumn> ToColumns(yyjson_val *columns);
GlueDatabaseInfo ToDatabaseInfo(yyjson_val *database);
GlueTableInfo ToTableInfo(yyjson_val *table);
GluePartitionInfo ToPartitionInfo(yyjson_val *partition);
string PartitionValuesToString(const vector<string> &values);
void CheckWritable(const GlueCatalog &catalog, const string &operation);

} // namespace duckdb

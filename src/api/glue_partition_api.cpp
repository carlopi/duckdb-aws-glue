#include "api/glue_api_util.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! GetPartition, the response holds the partition on success
static GlueResponse FetchPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                   const string &table_name, const vector<string> &values) {
	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "TableName", table_name);
	request.SetStrings(request.Root(), "PartitionValues", values);
	return GlueAPI::Call(context, catalog, "GetPartition", request);
}

//! GetPartition for a partition that has to exist
static GlueResponse FetchExistingPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                           const string &table_name, const vector<string> &values) {
	auto response = FetchPartition(context, catalog, database_name, table_name, values);
	if (!response.IsSuccess()) {
		if (response.IsEntityNotFound()) {
			throw CatalogException("Partition [%s] does not exist in Glue table '%s.%s'",
			                       PartitionValuesToString(values), database_name, table_name);
		}
		response.Throw(StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                  PartitionValuesToString(values)));
	}
	return response;
}

//! GetTable for the StorageDescriptor of the table: a partition carries its own, the table's with its location
static GlueResponse FetchTableForPartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                            const string &table_name) {
	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "Name", table_name);
	auto response = GlueAPI::Call(context, catalog, "GetTable", request);
	if (!response.IsSuccess()) {
		response.Throw(StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	return response;
}

//! Set 'key' of 'parent' to a copy of 'storage_descriptor' (an empty one if that is absent) with another location
static void SetStorageDescriptor(GlueRequest &request, yyjson_mut_val *parent, yyjson_val *storage_descriptor,
                                 const string &location) {
	auto descriptor = request.Copy(parent, "StorageDescriptor", storage_descriptor);
	if (!descriptor) {
		descriptor = request.SetObject(parent, "StorageDescriptor");
	}
	request.SetString(descriptor, "Location", location);
}

//! Set 'key' of 'parent' to a copy of 'source', an empty object if that is absent
static void CopyOrSetObject(GlueRequest &request, yyjson_mut_val *parent, const string &key, yyjson_val *source) {
	if (!request.Copy(parent, key, source)) {
		request.SetObject(parent, key);
	}
}

void GlueAPI::SetPartitionLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                   const string &table_name, const vector<string> &values, const string &location) {
	CheckWritable(catalog, "UpdatePartition");
	auto get_response = FetchExistingPartition(context, catalog, database_name, table_name, values);
	auto partition = get_response.Get("Partition");

	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "TableName", table_name);
	request.SetStrings(request.Root(), "PartitionValueList", values);
	auto input = request.SetObject(request.Root(), "PartitionInput");
	request.SetStrings(input, "Values", GlueJsonStrings(GlueJsonGet(partition, "Values")));
	SetStorageDescriptor(request, input, GlueJsonGet(partition, "StorageDescriptor"), location);
	CopyOrSetObject(request, input, "Parameters", GlueJsonGet(partition, "Parameters"));
	auto response = Call(context, catalog, "UpdatePartition", request);
	if (!response.IsSuccess()) {
		response.Throw(StringUtil::Format("UpdatePartition '%s.%s' [%s]", database_name, table_name,
		                                  PartitionValuesToString(values)));
	}
}

bool GlueAPI::GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                           const string &table_name, const vector<string> &values, GluePartitionInfo &result) {
	auto response = FetchPartition(context, catalog, database_name, table_name, values);
	if (!response.IsSuccess()) {
		if (response.IsEntityNotFound()) {
			return false;
		}
		response.Throw(StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                  PartitionValuesToString(values)));
	}
	result = ToPartitionInfo(response.Get("Partition"));
	return true;
}

bool GlueAPI::CreatePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const GluePartitionInput &partition, bool if_not_exists) {
	CheckWritable(catalog, "CreatePartition");
	auto table_response = FetchTableForPartitions(context, catalog, database_name, table_name);
	auto table_descriptor = GlueJsonGet(table_response.Get("Table"), "StorageDescriptor");

	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "TableName", table_name);
	auto input = request.SetObject(request.Root(), "PartitionInput");
	request.SetStrings(input, "Values", partition.values);
	SetStorageDescriptor(request, input, table_descriptor, partition.location);
	auto response = Call(context, catalog, "CreatePartition", request);
	if (!response.IsSuccess()) {
		if (response.IsAlreadyExists()) {
			if (if_not_exists) {
				return false;
			}
			throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
			                       PartitionValuesToString(partition.values), database_name, table_name);
		}
		response.Throw(StringUtil::Format("CreatePartition '%s.%s' [%s]", database_name, table_name,
		                                  PartitionValuesToString(partition.values)));
	}
	return true;
}

bool GlueAPI::DeletePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values) {
	CheckWritable(catalog, "DeletePartition");
	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "TableName", table_name);
	request.SetStrings(request.Root(), "PartitionValues", values);
	auto response = Call(context, catalog, "DeletePartition", request);
	if (!response.IsSuccess()) {
		if (response.IsEntityNotFound()) {
			return false;
		}
		response.Throw(StringUtil::Format("DeletePartition '%s.%s' [%s]", database_name, table_name,
		                                  PartitionValuesToString(values)));
	}
	return true;
}

void GlueAPI::RenamePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values,
                              const vector<string> &new_values) {
	CheckWritable(catalog, "UpdatePartition");
	// the partition to rename, with everything it carries (location, SerDe, parameters)
	auto get_response = FetchExistingPartition(context, catalog, database_name, table_name, values);
	auto partition = get_response.Get("Partition");

	// the new values must be free
	auto check_response = FetchPartition(context, catalog, database_name, table_name, new_values);
	if (check_response.IsSuccess()) {
		throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
		                       PartitionValuesToString(new_values), database_name, table_name);
	}
	if (!check_response.IsEntityNotFound()) {
		check_response.Throw(StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                        PartitionValuesToString(new_values)));
	}

	GlueRequest request(catalog);
	request.SetString(request.Root(), "DatabaseName", database_name);
	request.SetString(request.Root(), "TableName", table_name);
	request.SetStrings(request.Root(), "PartitionValueList", values);
	auto input = request.SetObject(request.Root(), "PartitionInput");
	request.SetStrings(input, "Values", new_values);
	CopyOrSetObject(request, input, "StorageDescriptor", GlueJsonGet(partition, "StorageDescriptor"));
	CopyOrSetObject(request, input, "Parameters", GlueJsonGet(partition, "Parameters"));
	auto response = Call(context, catalog, "UpdatePartition", request);
	if (!response.IsSuccess()) {
		response.Throw(StringUtil::Format("UpdatePartition '%s.%s' [%s] -> [%s]", database_name, table_name,
		                                  PartitionValuesToString(values), PartitionValuesToString(new_values)));
	}
}

//! The partitions Glue returns per request. 1000 is the maximum the API allows.
static constexpr int64_t GLUE_PARTITIONS_PAGE_SIZE = 1000;
//! The requests to run at the same time when the catalog is AWS and the setting leaves the choice open
static constexpr idx_t GLUE_DEFAULT_PARTITION_SEGMENTS = 8;
//! The maximum Glue accepts for Segment::TotalSegments
static constexpr idx_t GLUE_MAX_PARTITION_SEGMENTS = 10;

//! One chain of GetPartitions requests: pages through the partitions of segment 'segment_number' (the whole table
//! when 'total_segments' is 1) and appends them to 'result'
static void FetchPartitionSegment(ClientContext &context, const GlueCatalog &catalog, const string &database_name,
                                  const string &table_name, idx_t segment_number, idx_t total_segments,
                                  vector<GluePartitionInfo> &result) {
	string next_token;
	do {
		GlueRequest request(catalog);
		request.SetString(request.Root(), "DatabaseName", database_name);
		request.SetString(request.Root(), "TableName", table_name);
		// Only the values and the location of a partition are used. The column schema every partition repeats is a
		// large part of the response, and fewer bytes per partition means more partitions per page.
		request.SetBoolean(request.Root(), "ExcludeColumnSchema", true);
		request.SetInteger(request.Root(), "MaxResults", GLUE_PARTITIONS_PAGE_SIZE);
		if (total_segments > 1) {
			auto segment = request.SetObject(request.Root(), "Segment");
			request.SetInteger(segment, "SegmentNumber", NumericCast<int64_t>(segment_number));
			request.SetInteger(segment, "TotalSegments", NumericCast<int64_t>(total_segments));
		}
		if (!next_token.empty()) {
			request.SetString(request.Root(), "NextToken", next_token);
		}
		auto response = GlueAPI::Call(context, catalog, "GetPartitions", request);
		if (!response.IsSuccess()) {
			response.Throw(StringUtil::Format("GetPartitions '%s.%s'", database_name, table_name));
		}
		for (auto partition : GlueJsonElements(response.Get("Partitions"))) {
			result.push_back(ToPartitionInfo(partition));
		}
		next_token = response.GetString("NextToken");
	} while (!next_token.empty());
}

//! The number of GetPartitions requests to run at the same time: the setting when it is given, otherwise one request
//! against a server given with ENDPOINT (moto ignores Segment and answers every segment with the whole table) and
//! GLUE_DEFAULT_PARTITION_SEGMENTS against AWS.
static idx_t GetPartitionSegmentCount(ClientContext &context, const GlueCatalog &catalog) {
	idx_t segments = 0;
	Value setting;
	if (context.TryGetCurrentSetting("glue_get_partitions_segments", setting) && !setting.IsNull()) {
		segments = setting.GetValue<idx_t>();
	}
	if (segments == 0) {
		segments = catalog.options.endpoint.empty() ? GLUE_DEFAULT_PARTITION_SEGMENTS : 1;
	}
	if (segments > GLUE_MAX_PARTITION_SEGMENTS) {
		segments = GLUE_MAX_PARTITION_SEGMENTS;
	}
	return segments;
}

vector<GluePartitionInfo> GlueAPI::GetPartitions(ClientContext &context, GlueCatalog &catalog,
                                                 const string &database_name, const string &table_name) {
	auto total_segments = GetPartitionSegmentCount(context, catalog);
	vector<vector<GluePartitionInfo>> segment_results(total_segments);

	if (total_segments == 1) {
		FetchPartitionSegment(context, catalog, database_name, table_name, 0, 1, segment_results[0]);
	} else {
		// The segments do not overlap, so their requests can run at the same time
		vector<ErrorData> errors(total_segments);
		vector<thread> workers;
		for (idx_t segment = 1; segment < total_segments; segment++) {
			workers.emplace_back([&, segment]() {
				try {
					FetchPartitionSegment(context, catalog, database_name, table_name, segment, total_segments,
					                      segment_results[segment]);
				} catch (std::exception &ex) {
					errors[segment] = ErrorData(ex);
				}
			});
		}
		try {
			FetchPartitionSegment(context, catalog, database_name, table_name, 0, total_segments, segment_results[0]);
		} catch (std::exception &ex) {
			errors[0] = ErrorData(ex);
		}
		for (auto &worker : workers) {
			worker.join();
		}
		for (auto &error : errors) {
			if (error.HasError()) {
				error.Throw();
			}
		}
	}

	vector<GluePartitionInfo> result;
	unordered_set<string> seen;
	for (auto &segment_result : segment_results) {
		for (auto &partition : segment_result) {
			// A Glue compatible server that ignores Segment answers every segment with the whole table: the values
			// identify the partition, so what was seen already is dropped here
			if (total_segments > 1 && !seen.insert(StringUtil::Join(partition.values, "\x1f")).second) {
				continue;
			}
			result.push_back(std::move(partition));
		}
	}
	return result;
}

void GlueAPI::BatchCreatePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const string &table_name, const vector<GluePartitionInput> &partitions) {
	CheckWritable(catalog, "BatchCreatePartition");
	if (partitions.empty()) {
		return;
	}
	auto table_response = FetchTableForPartitions(context, catalog, database_name, table_name);
	auto table_descriptor = GlueJsonGet(table_response.Get("Table"), "StorageDescriptor");

	// BatchCreatePartition accepts at most 100 partitions per call
	constexpr idx_t BATCH_SIZE = 100;
	for (idx_t offset = 0; offset < partitions.size(); offset += BATCH_SIZE) {
		GlueRequest request(catalog);
		request.SetString(request.Root(), "DatabaseName", database_name);
		request.SetString(request.Root(), "TableName", table_name);
		auto inputs = request.SetArray(request.Root(), "PartitionInputList");
		for (idx_t i = offset; i < MinValue<idx_t>(offset + BATCH_SIZE, partitions.size()); i++) {
			auto &partition = partitions[i];
			auto input = request.AppendObject(inputs);
			request.SetStrings(input, "Values", partition.values);
			SetStorageDescriptor(request, input, table_descriptor, partition.location);
		}
		auto response = Call(context, catalog, "BatchCreatePartition", request);
		if (!response.IsSuccess()) {
			response.Throw(StringUtil::Format("BatchCreatePartition '%s.%s'", database_name, table_name));
		}
		for (auto error : GlueJsonElements(response.Get("Errors"))) {
			auto detail = GlueJsonGet(error, "ErrorDetail");
			auto code = GlueJsonString(detail, "ErrorCode");
			if (code == "AlreadyExistsException") {
				// appending to an existing partition
				continue;
			}
			throw IOException("Glue BatchCreatePartition '%s.%s' failed for partition [%s]: %s (%s)", database_name,
			                  table_name,
			                  StringUtil::Join(GlueJsonStrings(GlueJsonGet(error, "PartitionValues")), ", "),
			                  GlueJsonString(detail, "ErrorMessage"), code);
		}
	}
}

} // namespace duckdb

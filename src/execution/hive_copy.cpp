#include "execution/hive_copy.hpp"

#include "duckdb/common/exception.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! A CREATE TABLE AS writes only to a location without data files: the files of a dropped table there would read as
//! rows of the new one. Files a scan skips (_SUCCESS, directory markers, empty files) do not count.
static void ThrowIfLocationHoldsFiles(ClientContext &context, const GlueTableInfo &table_info, const string &location) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto listing = fs.GlobFileList(location + "/**", FileGlobOptions::ALLOW_EMPTY);
	for (auto &file : listing->Files()) {
		if (!IsDataFile(file, location + "/")) {
			continue;
		}
		throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' is not supported: its location '%s' "
		                            "already holds files (e.g. '%s'), such as those of a dropped table, which would "
		                            "read as rows of the new table. Delete them, or give the table another location",
		                            table_info.schema_name, table_info.name, location, file.path);
	}
}

GlueHiveCopy::GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                           unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality, GlueCatalog &catalog_p,
                           optional_ptr<GlueSchemaEntry> schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                           GlueTableInfo table_info_p, string write_id_p, string location_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      catalog(catalog_p), schema(schema_p), info(std::move(info_p)), table_info(std::move(table_info_p)),
      write_id(std::move(write_id_p)), location(std::move(location_p)) {
	D_ASSERT((schema != nullptr) == (info != nullptr));
}

unique_ptr<GlobalSinkState> GlueHiveCopy::GetGlobalSinkState(ClientContext &context) const {
	annotated_lock_guard<annotated_mutex> guard(start_lock);
	if (!write_started) {
		GlueHiveWrite write;
		write.write_id = write_id;
		write.database_name = table_info.schema_name;
		write.table_name = table_info.name;
		write.location = location;
		if (info) {
			// CREATE TABLE AS
			ThrowIfLocationHoldsFiles(context, table_info, location);
			// the client to drop the table with is taken first: once the table exists, nothing may stop the undo
			write.client = GlueAPI::GetClient(context, catalog);
			auto &create_schema = *schema.get_mutable();
			auto created = create_schema.CreateTableFromInfo(catalog.GetCatalogTransaction(context), *info, table_info);
			if (!created) {
				throw CatalogException("Table with name \"%s\" appeared between planning and execution; retry CREATE "
				                       "TABLE IF NOT EXISTS",
				                       table_info.name);
			}
			write.created_table_schema = schema;
			try {
				write.created_table_time =
				    GlueAPI::GetTableCreateTime(write.client, catalog, table_info.schema_name, table_info.name);
			} catch (std::exception &ex) {
				// not known: the undo then drops the table if it holds no other writer's files
				DUCKDB_LOG_WARNING(context, "Could not fetch the Glue table '%s.%s' just created: %s",
				                   table_info.schema_name, table_info.name, ErrorData(ex).RawMessage());
			}
		}
		GlueTransaction::Get(context, catalog).AddHiveWrite(std::move(write));
		write_started = true;
	}
	return PhysicalCopyToFile::GetGlobalSinkState(context);
}

string GlueHiveCopy::GetName() const {
	return info ? "GLUE_HIVE_CREATE_TABLE_AS_COPY" : PhysicalCopyToFile::GetName();
}

} // namespace duckdb

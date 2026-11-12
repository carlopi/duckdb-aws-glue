#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
struct HiveScanInfo;
struct HiveTableSample;

//! What Glue said about a table the running query scans
struct GlueTableEntry {
	annotated_mutex lock;
	//! Null until the first scan of a partitioned table asks Glue for them
	shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(lock);
};

//! The sample of a table, measured once per query for every scan of the table
struct HiveTableSampleEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table measures it. Shared and immutable once set: every scan of the table in
	//! the query (self joins, subqueries, a CTE inlined twice) reads the one sample, after the lock is released
	shared_ptr<const HiveTableSample> sample DUCKDB_GUARDED_BY(lock);
};

//! The footers of the files of one scan, read once per query when planning first asks for statistics
struct HiveScanFootersEntry {
	annotated_mutex lock;
	bool read DUCKDB_GUARDED_BY(lock) = false;
	//! A copy of the scan's bind data that opened every file for its footer (maximum_files_for_stats); null when not
	//! read. It holds the scan's HiveScanInfo, so it is kept here rather than in the HiveScanInfo
	shared_ptr<const FunctionData> footers DUCKDB_GUARDED_BY(lock);
};

//! A directory's recursive listing, fetched a page at a time as it is read
struct HiveDirectoryListingEntry {
	annotated_mutex lock;
	//! Null until some scan first asks for this directory
	shared_ptr<MultiFileList> listing DUCKDB_GUARDED_BY(lock);
};

//! What the running query learned about the tables it scans, dropped when the query ends: the partitions of a table
//! (asked for once however often the query scans it), the recursive listings of directories (shared by the estimate and
//! every scan listing the same directory) and the table samples
class HiveQueryCache : public ClientContextState {
public:
	static shared_ptr<HiveQueryCache> Get(ClientContext &context);
	//! The key of a Glue table in GetGlueTable
	static string TableKey(const string &catalog, const string &database, const string &table);

	void QueryEnd(ClientContext &context) override;

	//! 'key' identifies the table (TableKey)
	shared_ptr<GlueTableEntry> GetGlueTable(const string &key);
	//! 'table' identifies the table: its catalog and name, or the location of a hive_scan
	shared_ptr<HiveTableSampleEntry> GetSample(const string &table);
	//! The footers of the scan of 'info'
	shared_ptr<HiveScanFootersEntry> GetScanFooters(const HiveScanInfo &info);
	//! The recursive listing of 'directory', fetched page by page as it is read. With 'cache_in_connection' the
	//! listing is kept beyond the query (glue_cache_listings)
	shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory,
	                                              bool cache_in_connection = false);
	//! The id of the next scan bound in this query, starting at 1
	idx_t NextScanId();

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<GlueTableEntry>> glue_tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveDirectoryListingEntry>> directory_listings DUCKDB_GUARDED_BY(lock);
	//! By address: an entry's footers keep its HiveScanInfo alive, so the address is not reused while it is here
	unordered_map<const HiveScanInfo *, shared_ptr<HiveScanFootersEntry>> scan_footers DUCKDB_GUARDED_BY(lock);
	idx_t scan_count DUCKDB_GUARDED_BY(lock) = 0;
};

//! The partitions Glue registers for the table of 'info' in this query, fetched once for every scan of the table
shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info);
//! Drop the connection's cached directory listings
void ClearHiveListingCache(ClientContext &context);

} // namespace duckdb

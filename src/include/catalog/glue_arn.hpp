#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

//! What a Glue ARN names: a catalog, or a database in one
struct GlueArn {
	string partition;
	string region;
	string account_id;
	//! The Glue catalog id: the account id, or '<account id>:<catalog>/<nested>' for a nested catalog
	string catalog_id;
	//! The database a database ARN names, empty for a catalog ARN
	string database;
};

bool IsArn(const string &path);
//! arn:<partition>:glue:<region>:<account id>:catalog[/<catalog>/<nested>...] or
//! arn:<partition>:glue:<region>:<account id>:database/[<catalog>/<nested>/]<database>
GlueArn ParseGlueArn(const string &arn);
//! The ARN of the catalog with the given Glue catalog id in the region
string CatalogArn(const string &region, const string &catalog_id);
//! The ARN of a database of that catalog
string DatabaseArn(const string &region, const string &catalog_id, const string &database);

} // namespace duckdb

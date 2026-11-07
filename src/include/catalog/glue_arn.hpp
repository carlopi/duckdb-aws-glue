#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

//! The catalog a Glue ARN names
struct GlueArn {
	string partition;
	string region;
	string account_id;
	//! The Glue catalog id: the account id, or '<account id>:<catalog>/<nested>' for a nested catalog
	string catalog_id;
};

bool IsArn(const string &path);
//! arn:<partition>:glue:<region>:<account id>:catalog[/<catalog>/<nested>...]
GlueArn ParseGlueArn(const string &arn);
//! The ARN of the catalog with the given Glue catalog id in the region
string CatalogArn(const string &region, const string &catalog_id);

} // namespace duckdb

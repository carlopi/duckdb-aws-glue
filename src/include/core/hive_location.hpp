#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class FileSystem;
class MultiFileList;

//! A location to compare with other locations: s3a://, s3n:// and S3:// name the same bucket and key as s3://, and a
//! trailing slash is dropped. Only for comparing; files are listed and read at the location as registered.
string ComparableLocation(const string &location);
//! Whether one comparable location holds the other, so that files of one are files of the other
bool LocationsOverlap(const string &a, const string &b);

//! Whether listing a location would read it as a glob pattern (*, ? or [), which misses its own files and matches
//! those of other prefixes
bool LocationHasGlob(const string &location);
//! Every file below a location, recursively; empty when there are none. Check LocationHasGlob first
unique_ptr<MultiFileList> ListLocation(FileSystem &fs, const string &location);

} // namespace duckdb

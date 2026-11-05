//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/glue_file_credentials.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include "core/glue_info.hpp"

namespace duckdb {

class GlueTable;

//! Gives the scans of a Glue table the credentials to read its files with, as the name of a secret that every listed
//! file names in its options (ExtendedOpenFileInfo::SECRET_OPTION). An extension registers one as an
//! ExtensionCallback; without one, every file is read with the secret that matches its path.
class GlueFileCredentialsCallback : public ExtensionCallback {
public:
	//! Whether the scans of `table`, whose definition Glue returned as `table_info` for this query, read with
	//! credentials. Creates no secret.
	virtual bool HasFileCredentials(ClientContext &context, const GlueTable &table,
	                                const GlueTableInfo &table_info) = 0;
	//! The name of the secret for a scan of `table`, or "" to read its files by path. Asked each time a scan of the
	//! table is bound. A secret of the transaction (SecretPersistType::TRANSACTION) also serves the listings, which
	//! are made by path, and ends with the transaction.
	virtual string GetFileSecret(ClientContext &context, const GlueTable &table, const GlueTableInfo &table_info) = 0;

	//! The registered callback, if any. Throws when more than one is.
	static optional_ptr<GlueFileCredentialsCallback> Find(ClientContext &context) {
		optional_ptr<GlueFileCredentialsCallback> found;
		for (auto &callback : ExtensionCallback::Iterate(context)) {
			auto credentials_callback = dynamic_cast<GlueFileCredentialsCallback *>(callback.get());
			if (!credentials_callback) {
				continue;
			}
			if (found) {
				throw InvalidConfigurationException("More than one extension registered a GlueFileCredentialsCallback");
			}
			found = credentials_callback;
		}
		return found;
	}
	static bool Has(ClientContext &context, const GlueTable &table, const GlueTableInfo &table_info) {
		auto found = Find(context);
		return found && found->HasFileCredentials(context, table, table_info);
	}
	static string Get(ClientContext &context, const GlueTable &table, const GlueTableInfo &table_info) {
		auto found = Find(context);
		return found ? found->GetFileSecret(context, table, table_info) : string();
	}
};

} // namespace duckdb

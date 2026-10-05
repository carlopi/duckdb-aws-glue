add_library_unity(
  glue_api
  OBJECT
  glue_api_util.cpp
  glue_client.cpp
  glue_database_api.cpp
  glue_partition_api.cpp
  glue_table_api.cpp)

set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_api>
    PARENT_SCOPE)

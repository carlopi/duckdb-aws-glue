cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME glue)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD
    "17"
    CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(GLUE_EXTENSION_FILES)
add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${GLUE_EXTENSION_FILES})
build_loadable_extension(${TARGET_NAME} " " ${GLUE_EXTENSION_FILES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

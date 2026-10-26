add_library_unity(glue_catalog_types OBJECT type_helpers.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_catalog_types>
    PARENT_SCOPE)

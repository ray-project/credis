# Adapted from
#     https://github.com/caffe2/caffe2/blob/master/cmake/Modules/FindLevelDB.cmake
# Official CMake support for leveldb is being developed/tracked at:
#     https://github.com/google/leveldb/issues/466
#     https://github.com/pwnall/leveldb/tree/cmake

# - Find LevelDB
#
#  LevelDB_INCLUDES  - List of LevelDB includes
#  LevelDB_LIBRARIES - List of libraries when using LevelDB.
#  LevelDB_FOUND     - True if LevelDB found.

set(LEVELDB_ROOT "${CMAKE_SOURCE_DIR}/leveldb")

message(STATUS ${LEVELDB_ROOT})

# NOTES:
# - Use NO_DEFAULT_PATH to force/hint at both to pick up the leveldb that gets
#   included and built under credis.
# - Prefer to use leveldb's shared lib.  Its static lib seems to not have been
#   compiled with -fPIC, and thus errors occur during our credis linking.

# Look for the header file.
find_path(LevelDB_INCLUDE NAMES leveldb/db.h
                          PATHS ${LEVELDB_ROOT}/include /opt/local/include /usr/local/include /usr/include
                          DOC "Path in which the file leveldb/db.h is located."
                          NO_DEFAULT_PATH)

# Look for the library.
find_library(LevelDB_LIBRARY NAMES leveldb
                             PATHS ${LEVELDB_ROOT}/out-shared ${LEVELDB_ROOT}/out-static /usr/local/lib /usr/lib
                             DOC "Path to leveldb library."
                             NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LevelDB DEFAULT_MSG LevelDB_INCLUDE LevelDB_LIBRARY)

if(LEVELDB_FOUND)
  message(STATUS "Found LevelDB (include: ${LevelDB_INCLUDE}, library: ${LevelDB_LIBRARY})")
  set(LevelDB_INCLUDES ${LevelDB_INCLUDE})
  set(LevelDB_LIBRARIES ${LevelDB_LIBRARY})
  mark_as_advanced(LevelDB_INCLUDE LevelDB_LIBRARY)

  if(EXISTS "${LevelDB_INCLUDE}/leveldb/db.h")
    file(STRINGS "${LevelDB_INCLUDE}/leveldb/db.h" __version_lines
           REGEX "static const int k[^V]+Version[ \t]+=[ \t]+[0-9]+;")

    foreach(__line ${__version_lines})
      if(__line MATCHES "[^k]+kMajorVersion[ \t]+=[ \t]+([0-9]+);")
        set(LEVELDB_VERSION_MAJOR ${CMAKE_MATCH_1})
      elseif(__line MATCHES "[^k]+kMinorVersion[ \t]+=[ \t]+([0-9]+);")
        set(LEVELDB_VERSION_MINOR ${CMAKE_MATCH_1})
      endif()
    endforeach()

    if(LEVELDB_VERSION_MAJOR AND LEVELDB_VERSION_MINOR)
      set(LEVELDB_VERSION "${LEVELDB_VERSION_MAJOR}.${LEVELDB_VERSION_MINOR}")
    endif()

    # caffe_clear_vars(__line __version_lines)
  endif()
endif()

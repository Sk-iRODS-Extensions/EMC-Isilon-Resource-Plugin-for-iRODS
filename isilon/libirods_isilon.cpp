/**
 * iRODS resource plugin for EMC Isilon storage
 *
 * Copyright © 2016 by EMC Corporation, All Rights Reserved
 *
 * This software contains the intellectual property of EMC Corporation or is licensed to
 * EMC Corporation from third parties. Use of this software and the intellectual property
 * contained therein is expressly limited to the terms and conditions of the License
 * Agreement under which it is provided by or on behalf of EMC.
 */

#include "libirods_isilon.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include <irods_string_tokenize.hpp>
#include <irods_hierarchy_parser.hpp>
#include <irods_resource_redirect.hpp>
#include <irods_file_object.hpp>
#include <irods_collection_object.hpp>
#include <miscServerFunct.hpp>

// =-=-=-=-=-=-=-
// STL includes
#include <vector>
#include <string>
#include <sstream>

typedef handle<int,int(*)(int)> unix_file_handle;
typedef handle<int,irods::error(*)(int)> isilon_file_handle;
#ifdef ISILON_NO_CACHED_CONNECTIONS
typedef handle<class isilonConnectionDesc **,irods::error(*)(class isilonConnectionDesc **)> connection_handle;
#endif

static int NEXT_OBJ_DESC_NUM = 0;
synchro_map<int, class isilonObjectDesc*> OBJ_DESC_MAP;
synchro_map<std::string, class isilonConnectionDesc*> CONNECTION_DESC_MAP;
const char *HDFS_CLIENT = "HADOOFUS_CLIENT";

/**
 * BEGIN: Auxiliary functions
 */

/**
 * Allocates new file descriptor and returns its number
 */
ISILON_LOCAL int isilonNewFileDesc( isilonFileMode mode,
                                    const char* path,
                                    int buf_size,
                                    struct hdfs_object *last_block)
{
    isilonFileDesc *file_desc = new isilonFileDesc( mode, path, buf_size,
                                                    last_block);

    OBJ_DESC_MAP.insert( std::make_pair( NEXT_OBJ_DESC_NUM, file_desc));

#ifdef ISILON_DEBUG
    std::string mode_str;

    mode_str = (mode == ISILON_MODE_READ) ? "read" :
                   (mode == ISILON_MODE_WRITE) ? "write" : "unknown";
    ISILON_LOG( "\tFile descriptor %d created", NEXT_OBJ_DESC_NUM);
    ISILON_LOG( "\t\tpath: %s", path);
    ISILON_LOG( "\t\tmode: %s", mode_str.c_str());
    ISILON_LOG( "\t\tbuffer size: %d", buf_size);
#endif

    return NEXT_OBJ_DESC_NUM++;
}

/**
 * Allocates new directory descriptor and returns its number
 */
ISILON_LOCAL int isilonNewDirDesc( const char* path,
                                   hdfs_object *dir_list)
{
    isilonDirDesc *dir_desc = new isilonDirDesc( path, dir_list);

    OBJ_DESC_MAP.insert( std::make_pair( NEXT_OBJ_DESC_NUM, dir_desc));
    ISILON_LOG( "\tDirectory descriptor %d created", NEXT_OBJ_DESC_NUM);
    ISILON_LOG( "\t\tpath: %s", path);
    ISILON_LOG( "\t\tobjects in dir: %d",
                dir_list->ob_val._directory_listing._num_files);

    return NEXT_OBJ_DESC_NUM++;
}

/**
 * Check if object descriptor number is valid (i.e. corresponds to
 * some existing object descriptor)
 */
ISILON_LOCAL irods::error isilonIsObjIDValid( int od)
{
    irods::error result = SUCCESS();

    result = ISILON_ASSERT_ERROR( OBJ_DESC_MAP.find( od) != OBJ_DESC_MAP.end(),
                                  ISILON_ERR_UNKNOWN_OBJ_DESC,
                                  od);

    return result;
}

/**
 * Get file descriptor corresponding to particular ID
 */
ISILON_LOCAL irods::error isilonGetFileDescByID( int num, isilonFileDesc **fd)
{
    irods::error result = SUCCESS();

    result = isilonIsObjIDValid( num);
    ISILON_ERROR_CHECK_PASS( result);
#ifdef ISILON_DEBUG
    *fd = dynamic_cast<isilonFileDesc*>( OBJ_DESC_MAP.at( num));
    result = ISILON_ASSERT_ERROR( *fd, ISILON_ERR_UNEXPECTED_OBJECT_TYPE);
    ISILON_ERROR_CHECK( result);
#else
    *fd = static_cast<isilonFileDesc*>( OBJ_DESC_MAP.at( num));
#endif

    return result;
}

/**
 * Get directory descriptor corresponding to particular ID
 */
ISILON_LOCAL irods::error isilonGetDirDescByID( int num, isilonDirDesc **dd)
{
    irods::error result = SUCCESS();

    result = isilonIsObjIDValid( num);
    ISILON_ERROR_CHECK_PASS( result);
#ifdef ISILON_DEBUG
    *dd = dynamic_cast<isilonDirDesc*>( OBJ_DESC_MAP.at( num));
    result = ISILON_ASSERT_ERROR( *dd, ISILON_ERR_UNEXPECTED_OBJECT_TYPE);
    ISILON_ERROR_CHECK( result);
#else
    *dd = static_cast<isilonDirDesc*>( OBJ_DESC_MAP.at( num));
#endif

    return result;
}

#ifdef ISILON_DEBUG
/** Return a string that corresponds to a File System object type
 *
 * We do not check inside the function if a function argument is valid
 * since it is a debug functionality
 */
ISILON_LOCAL std::string isilonGetObjType( isilonObjectDesc *obj_desc)
{
    std::string obj_type_str;

    if ( dynamic_cast<isilonFileDesc*>( obj_desc) )
    {
        obj_type_str = "File";
    } else if ( dynamic_cast<isilonDirDesc*>( obj_desc) )
    {
        obj_type_str = "Directory";
    } else
    {
        obj_type_str = "Unknown object type";
    }

    return obj_type_str;
}
#endif

/**
 * Destroy object descriptor corresponding to particular ID
 */
ISILON_LOCAL irods::error isilonDestroyObjDesc( int num)
{
    irods::error result = SUCCESS();

    result = isilonIsObjIDValid( num);
    ISILON_ERROR_CHECK_PASS( result);

    isilonObjectDesc *obj_desc = OBJ_DESC_MAP.at( num);

    OBJ_DESC_MAP.erase( num);
#ifdef ISILON_DEBUG
    std::string obj_type_str = isilonGetObjType( obj_desc);
#endif
    delete obj_desc;
    ISILON_LOG( "\t%s descriptor %d destroyed", obj_type_str.c_str(), num);
    
    return result;
}

/**
 * Clean object descriptor table and set the first available object ID to zero
 */
ISILON_LOCAL irods::error isilonCleanObjDescTable()
{
    irods::error result = SUCCESS();
    auto it = OBJ_DESC_MAP.begin();

    while ( it != OBJ_DESC_MAP.end() )
    {
        isilonDestroyObjDesc( it->first);
        OBJ_DESC_MAP.erase( it);
        it = OBJ_DESC_MAP.begin();
    }

    NEXT_OBJ_DESC_NUM = 0;

    return result;
}

/**
 * Set offset of an object corresponding to particular ID
 */
ISILON_LOCAL irods::error isilonSetObjOffsetByID( int num,
                                                  long long offset)
{
    irods::error result = SUCCESS();

    result = isilonIsObjIDValid( num);
    ISILON_ERROR_CHECK_PASS( result);
    OBJ_DESC_MAP.at( num)->setOffset( offset);

#ifdef ISILON_DEBUG
    isilonObjectDesc *obj_desc = OBJ_DESC_MAP.at( num);
    std::string obj_type_str = isilonGetObjType( obj_desc);

    ISILON_LOG( "\t%s descriptor %d advanced to offset %lld",
                obj_type_str.c_str(), num, offset);
#endif

    return result;
}

/**
 * Return current offset of an object corresponding to particular ID
 */
ISILON_LOCAL irods::error isilonGetObjOffsetByID( int num,
                                                  long long *offset)
{
    irods::error result = SUCCESS();

    result = isilonIsObjIDValid( num);
    ISILON_ERROR_CHECK_PASS( result);
    *offset = OBJ_DESC_MAP.at( num)->getOffset();

    return result;
}

/**
 * Return mode of a file corresponding to particular ID
 */
ISILON_LOCAL irods::error isilonGetFileModeByID( int num,
                                                 isilonFileMode *mode)
{
    irods::error result = SUCCESS();
    isilonFileDesc *fd = 0;

    result = isilonGetFileDescByID( num, &fd);
    ISILON_ERROR_CHECK_PASS( result);
    *mode = fd->getMode();

    return result;
}

/**
 *  Releases pointers to hdfs_object objects
 */
ISILON_LOCAL void isilonFreeHDFSObjs( int n, ...)
{
    va_list vl;

    va_start( vl, n);

    for ( int i = 0; i < n; i++ )
    {
        struct hdfs_object **obj = va_arg( vl, struct hdfs_object **);

        if ( *obj )
        {
            hdfs_object_free( *obj);
            *obj = 0;
        }
    }

    va_end( vl);
}

/**
 * Compose a key to address connection object in a map
 */
ISILON_LOCAL std::string isilonGetConnectionKey( class isilonConnectionDesc* connection)
{
    std::stringstream ss;
    std::string port_str, buff_size_str;
    
    ss << connection->getPort();
    ss >> port_str;
    ss << connection->getBuffSize();
    ss >> buff_size_str;

    return buff_size_str + connection->getHost() + port_str + connection->getUser();
}

/**
 * Check validity of connection descriptor
 */
ISILON_LOCAL irods::error isilonIsConnectionDescValid( class isilonConnectionDesc* connection)
{
    irods::error result = SUCCESS();

    result = ISILON_ASSERT_ERROR( connection, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);

    std::string key = isilonGetConnectionKey( connection);

    result = ISILON_ASSERT_ERROR( CONNECTION_DESC_MAP.find( key) != CONNECTION_DESC_MAP.end(),
                                  ISILON_ERR_UNKNOWN_CONNECTION_DESC);

    return result;
}

/**
 * Extract connection properties from iRODS property map
 */
ISILON_LOCAL irods::error isilonParseConnectionProps( irods::plugin_property_map& prop_map,
                                                      std::string& host_name,
                                                      unsigned long *port_num,
                                                      std::string& user_name,
                                                      unsigned long *buf_size)
{
    irods::error result = SUCCESS();
    irods::error local_res = SUCCESS();

    local_res = prop_map.get<std::string>( ISILON_HOST_KEY, host_name);

    /* Parameter verification occurs during construction of resource object.
       We cannot fail at construction step. (Because as far as we know iRODS doesn't
       provide a mechanism for handling errors of this type). If no valid conversion of
       user-supplied values can be done, we use default values */
    if ( !local_res.ok() )
    {
        host_name = "HOST_NAME_NOT_PROVIDED";
    }

    std::string port_name;

    ISILON_LOG( "\t\tParsing connection props...");
    ISILON_LOG( "\t\t\tHost: %s", host_name.c_str());
    local_res = prop_map.get<std::string>( ISILON_PORT_KEY, port_name);
    
    if ( !local_res.ok() )
    {
        ISILON_LOG( "\t\t\tNo port provided, defaulting to 8020");
        *port_num = 8020;
    } else
    {
        char *reminder = 0;

        /* low-level "strtol" is used here because of low-level exception
           handling in iRODS */
        errno = 0;
        *port_num = strtoul( port_name.c_str(), &reminder, 10);

        if ( errno || (reminder == port_name.c_str()) || (*reminder != '\0') )
        {
            ISILON_LOG( "\t\t\tNon-convertable value for port, defaulting to 8020");
            *port_num = 8020;
        }

        ISILON_LOG( "\t\t\tPort: %lu", *port_num);
    }

    local_res = prop_map.get<std::string>( ISILON_USER_KEY, user_name);

    if ( !local_res.ok() )
    {
        ISILON_LOG( "\t\t\tUser: no user name provided, defaulting to \"root\"");
        user_name = "root";
    } else
    {
        ISILON_LOG( "\t\t\tUser: %s", user_name.c_str());
    }

    std::string bufsize_str;

    local_res = prop_map.get<std::string>( ISILON_BUFSIZE_KEY, bufsize_str);

    if ( !local_res.ok() )
    {
        ISILON_LOG( "\t\t\tBuffer size: no buffer size provided, defaulting to 64Mb");
        *buf_size = 64*1024*1024;
    } else
    {
        /* low-level "strtol" is used here because of low-level exception
           handling in iRODS */
        errno = 0;

        char *reminder = 0;
        unsigned long tmp_buf_size = strtoul( bufsize_str.c_str(), &reminder, 10);

        if ( errno || (reminder == bufsize_str.c_str()) || (*reminder != '\0') )
        {
            ISILON_LOG( "\t\t\tBuffer size: non-convertable value, defaulting to 64Mb");
            tmp_buf_size = 64;
        } else if ( tmp_buf_size < 1 )
        {
            ISILON_LOG( "\t\t\tBuffer size cannot be less than 1Mb. Using 1Mb");
            tmp_buf_size = 1;
        } else if ( tmp_buf_size > 256 )
        {
            ISILON_LOG( "\t\t\tBuffer size cannot be bigger than 256Mb. Using 256Mb");
            tmp_buf_size = 256;
        }

        /* Multiplication here is overflow-safe since we constrained buffer size
           to be in the range [1, 256Mb] */
        *buf_size = tmp_buf_size * 1024 * 1024;
        ISILON_LOG( "\t\t\tResulting buffer size: %lu bytes", *buf_size);
    }

    return result;
}

/**
 * Get connection descriptor
 *
 * The function checks if Isilon connection corresponding to
 * given credentials and parameters already exists. If so, the
 * handle of existing connection is returned. If not, new connection
 * is created and added to a list of available connections
 */
ISILON_LOCAL irods::error isilonGetConnection( irods::plugin_property_map& prop_map,
                                               class isilonConnectionDesc **connection)
{
    const char *err = 0;
    irods::error result = SUCCESS();
    irods::error local_res = SUCCESS();
    std::string host_name, user_name;
    unsigned long port_num = 0;
    unsigned long buff_size = 0;

    ISILON_LOG( "\tConnection requested");
    prop_map.get<std::string>( ISILON_HOST_KEY, host_name);
    prop_map.get<std::string>( ISILON_USER_KEY, user_name);
    prop_map.get<unsigned long>( ISILON_PORT_KEY, port_num);
    prop_map.get<unsigned long>( ISILON_BUFSIZE_KEY, buff_size);
    ISILON_LOG( "\t\tHost: %s", host_name.c_str());
    ISILON_LOG( "\t\tUser: %s", user_name.c_str());
    ISILON_LOG( "\t\tPort: %lu", port_num);
    ISILON_LOG( "\t\tBuffer size: %lu", buff_size);

    /* We convert here numerical port back to string and don't use original string
       representation, because several original string representations may
       correspond to the same integer number */
    std::stringstream ss;
    std::string port_name;
    std::string bufsize_name;

    ss << port_num;
    ss >> port_name;
    ss << buff_size;
    ss >> bufsize_name;

    /* Calculation here should be absolutely equal to calculation inside
       "isilonGetConnectionKey". It's not good to have two different places for
       the same calculation. Should be moved to a separate function when becomes a burden */
    std::string key_str = bufsize_name + host_name + port_name + user_name;

#ifndef ISILON_NO_CACHED_CONNECTIONS
    if ( CONNECTION_DESC_MAP.find( key_str) != CONNECTION_DESC_MAP.end() )
    {
        ISILON_LOG( "\tConnection already exists");
        *connection = CONNECTION_DESC_MAP.at( key_str);
        
        return result;
    }
#endif
        
    ISILON_LOG( "\tConnection doesn't exist. Creating new");

    struct hdfs_namenode *name_node = 0;

    name_node = hdfs_namenode_new( host_name.c_str(), port_name.c_str(),
                                   user_name.c_str(), HDFS_NO_KERB, &err);
    result = ISILON_ASSERT_ERROR( name_node, ISILON_ERR_NEW_NAME_NODE_FAIL, err);
    ISILON_ERROR_CHECK( result);

    int64_t version = 0;
    struct hdfs_object *exception = 0;

    version = hdfs_getProtocolVersion( name_node, HADOOFUS_CLIENT_PROTOCOL_STR,
                                       61L, &exception);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_GET_PROTOCOL_VERSION_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        isilonFreeHDFSObjs( 1, &exception);
        hdfs_namenode_delete( name_node);

        return result;
    }

    result = ISILON_ASSERT_ERROR( version == 61, ISILON_ERR_INVALID_HDFS_PROTOCOL_VERSION,
                                  version);

    if ( !result.ok() )
    {
        hdfs_namenode_delete( name_node);

        return result;
    }

    *connection = new isilonConnectionDesc( host_name, port_num,
                                            user_name, name_node, buff_size);
#ifndef ISILON_NO_CACHED_CONNECTIONS
    CONNECTION_DESC_MAP.insert( std::make_pair( key_str, *connection));
#endif
    ISILON_LOG( "\tConnection to Name Node established");

    return result;
}

/**
 * Close connection
 */
ISILON_LOCAL irods::error isilonCloseConnection( class isilonConnectionDesc **connection)
{
    irods::error result = SUCCESS();
    
    if ( !connection )
    {
        return result;
    }

    result = isilonIsConnectionDescValid( *connection);
    ISILON_ERROR_CHECK_PASS( result);
    ISILON_LOG( "\tClosing connection:");
    ISILON_LOG( "\t\tHost: %s", ((*connection)->getHost()).c_str());
    ISILON_LOG( "\t\tPort: %ld", (*connection)->getPort());
    ISILON_LOG( "\t\tUser: %s", ((*connection)->getUser()).c_str());
    ISILON_LOG( "\t\tBuffer size: %d", (*connection)->getBuffSize());
    
    std::string key = isilonGetConnectionKey( *connection);

#ifndef ISILON_NO_CACHED_CONNECTIONS
    CONNECTION_DESC_MAP.erase( key);
#endif
    delete *connection;
    *connection = 0;

    return result;
}

#ifdef ISILON_NO_CACHED_CONNECTIONS
#define ISILON_GET_CONNECTION( prop_map_, conn_) \
            irods::error get_conn_result = SUCCESS(); \
            get_conn_result = isilonGetConnection( prop_map_, conn_); \
            ISILON_ERROR_CHECK_PASS( get_conn_result); \
            connection_handle conn_handle( conn_, isilonCloseConnection)
#else
#define ISILON_GET_CONNECTION( prop_map_, conn_) \
            irods::error get_conn_result = SUCCESS(); \
            get_conn_result = isilonGetConnection( prop_map_, conn_); \
            ISILON_ERROR_CHECK_PASS( get_conn_result)
#endif

/**
 * Release all connection handles
 */
ISILON_LOCAL irods::error isilonReleaseConnections()
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Closing all connections...");

    auto it = CONNECTION_DESC_MAP.begin();

    while ( it != CONNECTION_DESC_MAP.end() )
    {
        result = isilonCloseConnection( &(it->second));
        ISILON_ERROR_CHECK_PASS( result);
        CONNECTION_DESC_MAP.erase( it);
        it = CONNECTION_DESC_MAP.begin();
    }

    return result;
}

/**
 * Generate a full path name from the partial physical path
 * and the specified resource's vault path
 */
ISILON_LOCAL irods::error isilonGenerateFullPath( irods::plugin_property_map& _prop_map,
                                                  const std::string&          _phy_path,
                                                  std::string&                _ret_string)
{
    irods::error result = SUCCESS();
    irods::error ret;
    std::string vault_path;

    ret = _prop_map.get<std::string>( irods::RESOURCE_PATH, vault_path);
    result = ISILON_ASSERT_ERROR( ret.ok(), ISILON_ERR_NO_VAULT_PATH);
    ISILON_ERROR_CHECK( result);

    if ( _phy_path.compare( 0, 1, "/" ) != 0
         && _phy_path.compare( 0, vault_path.size(), vault_path ) != 0 )
    {
        _ret_string  = vault_path;
        _ret_string += "/";
        _ret_string += _phy_path;
    } else {
        /* The physical path already contains the vault path */
        _ret_string = _phy_path;
    }

    return result;
} // isilonGenerateFullPath

/**
 * Update the physical path in the file object
 */
ISILON_LOCAL irods::error isilonCheckPath( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();
    /* try dynamic cast on ptr */
    irods::data_object_ptr data_obj = boost::dynamic_pointer_cast<irods::data_object>( _ctx.fco());

    result = ISILON_ASSERT_ERROR( data_obj.get(), ISILON_ERR_CAST_FCO_FAIL);
    ISILON_ERROR_CHECK( result);

    std::string full_path;
    irods::error ret = isilonGenerateFullPath( _ctx.prop_map(),
                                               data_obj->physical_path(),
                                               full_path);
    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_GEN_FULL_PATH_FAIL);

    if ( result.ok() )
    {
        data_obj->physical_path( full_path);
    }

    return result;
} // isilonCheckPath

/**
 * Check the basic operation parameters and update the physical path in the file object
 */
template<typename DEST_TYPE>
ISILON_LOCAL irods::error isilonCheckParamsAndPath( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();
    irods::error ret;

    /* verify that the resc context is valid */
    ret = _ctx.valid<DEST_TYPE>();
    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_CONTEXT);
    ISILON_ERROR_CHECK( result);
    result = isilonCheckPath( _ctx);
    ISILON_ERROR_CHECK_PASS( result);

    return result;
} // isilonCheckParamsAndPath

/**
 * Check the basic operation parameters and update the physical path in the file object
 */
ISILON_LOCAL irods::error isilonCheckParamsAndPath( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();
    irods::error ret;

    /* verify that the resc context is valid */
    ret = _ctx.valid();
    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_CONTEXT);
    ISILON_ERROR_CHECK( result);
    result = isilonCheckPath( _ctx);
    ISILON_ERROR_CHECK_PASS( result);

    return result;
} // isilonCheckParamsAndPath

/**
 * Convert HDFS exception into Unix error code
 */
ISILON_LOCAL irods::error isilonGetErrCodeFromException( struct hdfs_object *exception,
                                                         int *code)
{
    irods::error result = SUCCESS();

    result = ISILON_ASSERT_ERROR( exception && code, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);

    switch( hdfs_exception_get_type( exception) )
    {
        case H_ACCESS_CONTROL_EXCEPTION:
            *code = EACCES;
        case H_FILE_NOT_FOUND_EXCEPTION:
            *code = ENOENT;
        default:
            *code = EIO;
    }

    return result;
}

/**
 * Get HDFS object metadata
 */
ISILON_LOCAL irods::error isilonGetHDFSFileInfo( struct hdfs_namenode *nn,
                                                 const char *path,
                                                 struct hdfs_object **fstatus,
                                                 int *err_code)
{
    irods::error result = SUCCESS();
    struct hdfs_object *exception = 0;

    if ( err_code )
    {
        *err_code = 0;
    }

    result = ISILON_ASSERT_ERROR( nn && path && fstatus, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);
    *fstatus = 0;
    ISILON_LOG( "\tCollecting object info for path: %s", path);
    *fstatus = hdfs_getFileInfo( nn, path, &exception);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_GET_FILE_INFO_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        if ( err_code )
        {
            isilonGetErrCodeFromException( exception, err_code);
        }

        isilonFreeHDFSObjs( 2, &exception, fstatus);

        return result;
    }

    result = ISILON_ASSERT_ERROR( (*fstatus)->ob_type != H_NULL, ISILON_ERR_FILE_NOT_EXIST,
                                  path);

    if ( !result.ok() )
    {
        ISILON_LOG( "\t\tObject doesn't exist");

        if ( err_code )
        {
            *err_code = ENOENT;
        }

        isilonFreeHDFSObjs( 2, &exception, fstatus);

        return result;
    }

    ISILON_LOG( "\t\tObject info collected");

    return result;
}

/**
 * Create HDFS path
 */
ISILON_LOCAL irods::error isilonCreateHDFSPath( struct hdfs_namenode *nn,
                                                const char *path,
                                                const int mode,
                                                int *err_code)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "\tCreating path: %s", path);

    if ( err_code )
    {
        *err_code = 0;
    }

    struct hdfs_object *exception = 0;
    bool oper_status = hdfs_mkdirs( nn, path, mode, &exception);

    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_MKDIRS_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        if ( err_code )
        {
            isilonGetErrCodeFromException( exception, err_code);
        }

        isilonFreeHDFSObjs( 1, &exception);

        return result;
    }

    result = ISILON_ASSERT_ERROR( oper_status, ISILON_ERR_DIRS_NOT_CREATED, path);

    if ( !result.ok() )
    {
        if ( err_code )
        {
            /* Suppose that means that path already exists, but I'm not sure
               it's the only option */
            *err_code = EEXIST;
        }

        return result;
    }

    ISILON_LOG( "\t\tPath created");

    return result;
}

/**
 * Emulate "readdir" behavior
 */
ISILON_LOCAL irods::error isilonReaddir( int num,
                                         struct rodsDirent** de_ptr,
                                         bool *is_end)
{
    irods::error result = SUCCESS();

    if ( is_end )
    {
        *is_end = false;
    }

    isilonDirDesc *dd = 0;

    isilonGetDirDescByID( num, &dd);
    ISILON_LOG( "\tReading from directory with id %d", num);

    hdfs_object *dir_list = dd->getDirList();

    if ( dd->getOffset() == dir_list->ob_val._directory_listing._num_files )
    {
        ISILON_LOG( "\t\tCurrently at directory end. Nothing to read");

        if ( is_end )
        {
            *is_end = true;
        }

        return result;
    }

    if ( !(*de_ptr) )
    {
        /* "rodsDirent" object is allocated here, but there is no place
           where it will be deallocated. All such objects will be freed only
           when Agent execution is complete. Tracker 2218 reported to iRODS
           community addresses this issue. The tracker should be monitored
           and the issue should be resolved somehow */
        *de_ptr = new rodsDirent_t;
    }

    int off = dd->getOffset();

    ISILON_LOG( "\t\tReading item: %d", off);

    struct hdfs_object *status = dir_list->ob_val._directory_listing._files[off];
    struct hdfs_file_status *fstatus = &status->ob_val._file_status;

    strcpy( (*de_ptr)->d_name, fstatus->_file);
    /* Currently makes no sense for us */
    (*de_ptr)->d_ino = 0;
    /* Seems makes no sense for anybody */
    (*de_ptr)->d_offset = 0;
    (*de_ptr)->d_namlen = strlen( (*de_ptr)->d_name);
    (*de_ptr)->d_reclen = (*de_ptr)->d_namlen + offsetof( rodsDirent_t, d_name);
    ISILON_LOG( "\t\t\tname: %s", (*de_ptr)->d_name);
    ISILON_LOG( "\t\t\tnamlen: %d", (*de_ptr)->d_namlen);
    ISILON_LOG( "\t\t\treclen: %d", (*de_ptr)->d_reclen);
    dd->setOffset( off + 1);
    ISILON_LOG( "\tOffset of directory %d advanced. Currently: %lld", num,
                dd->getOffset());

    return result;
}

/**
 * Unlink HDFS object
 */
ISILON_LOCAL irods::error isilonUnlinkHDFSObj( irods::resource_plugin_context& _ctx,
                                               const char *path,
                                               int *status)
{
    irods::error result = SUCCESS();
    class isilonConnectionDesc *conn = 0;
    struct hdfs_namenode *nn = 0;

    if ( status )
    {
        *status = 0;
    }

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);
    nn = conn->getNameNode();
    ISILON_LOG( "\tObject to remove: %s", path);

    struct hdfs_object *exception = 0;

    /* Currently resursive deletion is not expected in iRODS. But
       we (potentially) may optimize some scenarios by using
       recursive deletion capability of HDFS */
    /* Should we check return status of "hdfs_delete"? Can it be
       "false"? */
    hdfs_delete( nn, path, false/*recurse*/, &exception);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_UNLINK_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        if ( status )
        {
            isilonGetErrCodeFromException( exception, status);
        }

        isilonFreeHDFSObjs( 1, &exception);

        return result;
    }

    isilonFreeHDFSObjs( 1, &exception);
    ISILON_LOG( "\t\tObject removed");

    return result;
}

/**
 * Commit a new block to a Data Node
 */
ISILON_LOCAL irods::error isilonCommitBufferToHDFS( struct hdfs_namenode *nn,
                                                    const char *path,
                                                    const char *buf,
                                                    int len,
                                                    struct hdfs_object *last_block,
                                                    int *status)
{
    irods::error result = SUCCESS();
    struct hdfs_object *exception = 0, *block = 0;

    *status = 0;

    if ( last_block )
    {
        block = last_block;
        ISILON_LOG( "\t\t\tUsing last block from HDFS");
    } else
    {
        block = hdfs_addBlock( nn, path, HDFS_CLIENT, 0, &exception);
        result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_ADD_BLOCK_FAIL,
                                      exception ? hdfs_exception_get_message( exception) : 0);

        if ( !result.ok() )
        {
            isilonGetErrCodeFromException( exception, status);
            isilonFreeHDFSObjs( 2, &exception, &block);

            return result;
        }

        ISILON_LOG( "\t\t\tBlock successfully added");
    }

    struct hdfs_datanode *data_node = 0;
    const char *err = 0;

    data_node = hdfs_datanode_new( block, HDFS_CLIENT, HDFS_DATANODE_AP_1_0, &err);
    result = ISILON_ASSERT_ERROR( data_node, ISILON_ERR_CONNECT_TO_DATANODE_FAIL, err,
                                  block->ob_val._located_block._locs[0]->ob_val._datanode_info._hostname,
                                  block->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

    if ( !result.ok() )
    {
        isilonFreeHDFSObjs( 2, &exception, &block);
        *status = EIO;

        return result;
    }

    ISILON_LOG( "\t\t\tData Node acquired: %s",
                 block->ob_val._located_block._locs[0]->ob_val._datanode_info._hostname);
    err = hdfs_datanode_write( data_node, buf, len, false/*crcs*/);
    result = ISILON_ASSERT_ERROR( !err, ISILON_ERR_WRITE_FAIL, err);
    hdfs_datanode_delete( data_node);
    isilonFreeHDFSObjs( 2, &exception, &block);

    if ( !result.ok() )
    {
        *status = EIO;
        
        return result;
    }

    ISILON_LOG( "\t\t\t%d bytes written", len);

    return result;
}

/**
 * Low-level part of "append" processing
 */
ISILON_LOCAL irods::error isilonAppendFileImpl( isilonConnectionDesc *conn,
                                                const char           *path,
                                                struct hdfs_object   **last_block,
                                                int                  *status)
{
    irods::error result = SUCCESS();
    struct hdfs_object *exception = 0, *lb = 0;

    ISILON_LOG( "\tOpening file for append");
    ISILON_LOG( "\t\tPath: %s", path);    
    lb = hdfs_append( conn->getNameNode(), path, HDFS_CLIENT, &exception);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_CREATE_FILE_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        isilonGetErrCodeFromException( exception, status);
        isilonFreeHDFSObjs( 1, &exception);

        return result;
    }

    ISILON_LOG( "\t\tAppend successful");
    isilonFreeHDFSObjs( 1, &exception);

    if ( (lb && lb->ob_type == H_NULL)
         || !lb->ob_val._located_block._len )
    {
        hdfs_object_free( lb);
        lb = 0;
    }

    *last_block = lb;

    return result;
}

/**
 * Open file in "append" mode
 */
ISILON_LOCAL irods::error isilonAppendFile( isilonConnectionDesc *conn,
                                            const char           *path,
                                            int                  *file_id,
                                            int                  *status)
{
    irods::error result = SUCCESS();
    /* check incoming parameters */
    bool check_expr = conn && path && status && file_id;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);
    *status = 0;

    struct hdfs_object *last_block = 0;

    result = isilonAppendFileImpl( conn, path, &last_block, status);
    ISILON_ERROR_CHECK_PASS( result);

    /* Write mode is implied for append */
    *file_id = isilonNewFileDesc( ISILON_MODE_WRITE, path, conn->getBuffSize(),
                                  last_block);
 
    return result;
}

/**
 * Create new file and open new file descriptor
 */
ISILON_LOCAL irods::error isilonCreateFile( isilonConnectionDesc *conn,
                                            const char           *path,
                                            int                  mode,
                                            bool                 overwrite,
                                            int                  *file_id,
                                            int                  *status)
{
    irods::error result = SUCCESS();
    /* check incoming parameters */
    bool check_expr = conn && path && status && file_id;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);

    struct hdfs_namenode *nn = conn->getNameNode();
    struct hdfs_object *exception = 0;

    *status = 0;
    ISILON_LOG( "\tFile creation requested");
    ISILON_LOG( "\t\tPath: %s", path);
    ISILON_LOG( "\t\tMode: 0x%x", mode);
    hdfs_create( nn, path, mode,
                 HDFS_CLIENT, overwrite, true/*createparent*/,
                 1/*replication*/, 4*1024*1024, &exception);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_CREATE_FILE_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        isilonGetErrCodeFromException( exception, status);
        isilonFreeHDFSObjs( 1, &exception);

        return result;
    }

    /* Write mode is implied for creation */
    *file_id = isilonNewFileDesc( ISILON_MODE_WRITE, path, conn->getBuffSize(), 0);
    isilonFreeHDFSObjs( 1, &exception);
 
    return result;
}

/**
 * Close opened file, flush its buffer and release file decriptor
 */
ISILON_LOCAL irods::error isilonCloseFile( class isilonConnectionDesc *conn,
                                           int                        file_id,
                                           int                        *status)
{
    irods::error result = SUCCESS();
    /* check incoming parameters */
    bool check_expr = conn && status;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);
    *status = 0;

    struct hdfs_namenode *nn = conn->getNameNode();
    struct hdfs_object *exception = 0;
    isilonFileDesc *fd = 0;
    
    result = isilonGetFileDescByID( file_id, &fd);
    ISILON_ERROR_CHECK_PASS( result);

    isilon_file_handle file_handle( file_id, isilonDestroyObjDesc);
    isilonFileMode mode = fd->getMode();
    const char *path = fd->getPath().c_str();

    ISILON_LOG( "\tFile to close: %s (id: %d)", path, file_id);

    if ( mode == ISILON_MODE_WRITE )
    {
        int buff_offset = fd->getBuffOffset();

        if ( buff_offset )
        {
            const char *buff = 0;

            /* Commit block to HDFS */
            ISILON_LOG( "\tFile buffer is not empty (%d bytes). Committing to HDFS",
                        buff_offset);
            result = fd->flushBuff( &buff);
            ISILON_ERROR_CHECK_PASS( result);
            result = isilonCommitBufferToHDFS( nn, path, buff, buff_offset,
                                               fd->getLastBlock(), status);

            if ( fd->getLastBlock() )
            {
                /* Actually may skip this, but do it for explicity */
                fd->releaseLastBlock();
            } 

            if ( !result.ok() )
            {
                *status = EIO;

                return PASS( result);
            }
        }

        bool is_ok = hdfs_complete( nn, path, HDFS_CLIENT, &exception);

        result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_COMPLETE_FAIL,
                                      exception ? hdfs_exception_get_message( exception) : 0);
        ISILON_LOG( "\tFile %s completed", path);

        if ( !result.ok() )
        {
            isilonGetErrCodeFromException( exception, status);
            isilonFreeHDFSObjs( 1, &exception);

            return result;
        }

        result = ISILON_ASSERT_ERROR( is_ok, ISILON_ERR_FILE_NOT_COMPLETED,
                                      path);

        if ( !result.ok() )
        {
            isilonFreeHDFSObjs( 1, &exception);
            *status = EIO;

            return result;
        }
    } else
    {
        result = ISILON_ASSERT_ERROR( mode == ISILON_MODE_READ,
                                      ISILON_ERR_MODE_NOT_SUPPORTED);

        if ( !result.ok() )
        {
            isilonFreeHDFSObjs( 1, &exception);

            return result;
        }
    }

    isilonFreeHDFSObjs( 1, &exception);

    return result;
}

/**
 * Write data to a buffer and commit to HDFS Data Node (if the buffer is filled)
 */
ISILON_LOCAL irods::error isilonWriteBuf( struct hdfs_namenode *nn,
                                          int id,
                                          const char *buf,
                                          int len,
                                          int *status)
{
    irods::error result = SUCCESS();
    /* check incoming parameters */
    bool check_expr = nn && buf && status;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);

    isilonFileDesc *fd = 0;

    result = isilonGetFileDescByID( id, &fd);
    ISILON_ERROR_CHECK_PASS( result);
    result = ISILON_ASSERT_ERROR( fd->getMode() == ISILON_MODE_WRITE,
                                  ISILON_ERR_UNEXPECTED_MODE, fd->getMode(),
                                  ISILON_MODE_WRITE);
    ISILON_ERROR_CHECK( result);
    result = ISILON_ASSERT_ERROR( fd->getFileSize() == fd->getOffset(),
                                  ISILON_ERR_UNEXPECTED_OFFSET, id,
                                  fd->getFileSize(), fd->getOffset());
    ISILON_ERROR_CHECK( result);

    int buf_offset = 0;

    while ( len )
    {
        const char *wbuff = 0;
        int wbuff_size = fd->getBuffSize();
        int wbuff_offset = fd->getBuffOffset();
        int bytes_added = 0;

        if ( len >= wbuff_size && wbuff_offset == 0 )
        {
            /* Size of data to write is bigger or equal to buffer size and
               the buffer is currently empty. So we can skip bufferization
               step and commit the data directly to HDFS */
            wbuff = buf + buf_offset;
            buf_offset += wbuff_size;
            bytes_added = wbuff_size;
            len -= wbuff_size;
        } else
        {
            int buff_avail = wbuff_size - wbuff_offset;

            if ( buff_avail > len )
            {
                result = fd->writeToBuff( buf, buf_offset, len);
                ISILON_ERROR_CHECK_PASS( result);
                buf_offset += len;
                bytes_added = len;
                len = 0;
            } else
            {
                result = fd->writeToBuff( buf, buf_offset, buff_avail);
                ISILON_ERROR_CHECK_PASS( result);
                len -= buff_avail;
                buf_offset += buff_avail;
                bytes_added = buff_avail;
                result = fd->flushBuff( &wbuff);
                ISILON_ERROR_CHECK_PASS( result);
            }

            /* We use 'wbuff_offset + bytes_added' for buffer payload size
               instead of 'fd->getBuffSize()' because the buffer might be flushed */
            ISILON_LOG( "\t\t%d bytes bufferized. Currently in buffer: %d",
                        bytes_added, wbuff_offset + bytes_added);
        }

        if ( wbuff )
        {
            /* Commit block to HDFS */
            ISILON_LOG( "\t\tBuffer is full. Committing to HDFS");
            result = isilonCommitBufferToHDFS( nn, fd->getPath().c_str(), wbuff,
                                               wbuff_size, fd->getLastBlock(), status);
            ISILON_ERROR_CHECK_PASS( result);

            if ( fd->getLastBlock() )
            {
                fd->releaseLastBlock();
            }
        }

        /* Only at this point we know that the data were placed to buffer
           or committed directly to HDFS. Only now we can adjust the file offset */
#ifdef ISILON_DEBUG
        isilonSetObjOffsetByID( id, fd->getOffset() + bytes_added);
#else
        fd->setOffset( fd->getOffset() + bytes_added);
#endif
        /* After any write operation made inside this plugin, file size
           should be equal to file offset, since only writes to the end
           of the file are allowed */
        fd->setFileSize( fd->getOffset());
    }

    return result;
}

/**
 * Write data to the specified file
 */
ISILON_LOCAL irods::error isilonWriteFile( isilonConnectionDesc *conn,
                                           int                  file_id,
                                           void                 *_buf,
                                           int                  _len,
                                           int                  *status)
{
    irods::error result = SUCCESS();

    /* check incoming parameters */
    bool check_expr = conn && _buf && status;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);
    *status = 0;

    struct hdfs_namenode *nn = conn->getNameNode();

#ifdef ISILON_DEBUG
    /* Sanity check. For now current file offset should be equal to
       file size + size of bufferized data */
    isilonFileDesc *fd = 0;

    result = isilonGetFileDescByID( file_id, &fd);
    ISILON_ERROR_CHECK_PASS( result);

    struct hdfs_object *fstatus = 0;

    result = isilonGetHDFSFileInfo( nn, (fd->getPath()).c_str(), &fstatus, 0);

    if ( !result.ok() )
    {
        isilonFreeHDFSObjs( 1, &fstatus);

        return PASS( result);
    }

    struct hdfs_file_status *f_stat = &fstatus->ob_val._file_status;

    result = ISILON_ASSERT_ERROR( f_stat->_size + fd->getBuffOffset() ==
                                      (unsigned long long)fd->getOffset(),
                                  ISILON_ERR_UNEXPECTED_OFFSET, file_id,
                                  fd->getFileSize(), fd->getOffset());
    isilonFreeHDFSObjs( 1, &fstatus);
    ISILON_ERROR_CHECK( result);
#endif

    ISILON_LOG( "\tWriting to file: %s (id: %d)", (fd->getPath()).c_str(), file_id);
    result = isilonWriteBuf( nn, file_id, (char *)_buf,
                             _len, status);

    ISILON_ERROR_CHECK_PASS( result);

    return result;
}

/**
 * Retrieve a new chunk of data from a Data Node
 */
ISILON_LOCAL irods::error isilonFillBufferFromHDFS( struct hdfs_namenode *nn,
                                                    const char *path,
                                                    char *buf,
                                                    long long offset,
                                                    int len,
                                                    int *status)
{
    irods::error result = SUCCESS();
    /* check incoming parameters */
    bool check_expr = nn && path && buf && status;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);

    struct hdfs_object *exception = 0;
    struct hdfs_object *block_seq = hdfs_getBlockLocations( nn,
                                                            path,
                                                            offset,
                                                            len,
                                                            &exception);

    *status = 0;
    ISILON_LOG( "\t\t\tBlock sequence obtained for %d bytes from offset %lld", len, offset);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_GET_BLOCK_LOCATIONS_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        isilonGetErrCodeFromException( exception, status);
        isilonFreeHDFSObjs( 2, &exception, &block_seq);

        return result;
    }

    result = ISILON_ASSERT_ERROR( block_seq->ob_type != H_NULL,
                                  ISILON_ERR_FILE_NOT_EXIST);

    if ( !result.ok() )
    {
        isilonFreeHDFSObjs( 2, &exception, &block_seq);
        *status = EIO;

        return result;
    }

    int block_num = block_seq->ob_val._located_blocks._num_blocks;
    /* We track the number of bytes remained to read. This is a workaround
       for a bug in Isilon 8. Sometimes Isilon returns redundant blocks
       at the end of the block sequence. Those redundant blocks are invalid and
       cannot be read. But we can skip them and stop reading, since all the
       requested data is already contained in previous - valid - blocks in
       the sequence. To skip redundant blocks, we use this 'to_read' variable.
       When no data remains to be read, we just break the reading loop.
       In absence of the bug we could just read the entire sequence of blocks
       without controlling the size of already transferred data */
    int to_read = len;

    ISILON_LOG( "\t\t\tBlocks in the sequence: %d", block_num);

    /* We may need to read multiple blocks to satisfy the read */
    for ( int i = 0; i < block_num; i++)
    {
        struct hdfs_object *block = block_seq->ob_val._located_blocks._blocks[i];
        const char *err = 0;
        int64_t block_begin = 0;
        int64_t block_end = block->ob_val._located_block._len;
        int64_t block_offset = block->ob_val._located_block._offset;

        ISILON_LOG( "\t\t\tReading from block %d in the sequence", i);
        result = ISILON_ASSERT_ERROR( block_offset + block_end > offset
                                      && block_offset < offset + len,
                                      ISILON_ERR_CORRUPTED_OR_INCORRECT_BLOCK, block_offset, block_end);

        if ( !result.ok() )
        {
            isilonFreeHDFSObjs( 2, &exception, &block_seq);
            *status = EIO;

            return result;
        }

        struct hdfs_datanode *dn = 0;

        dn = hdfs_datanode_new( block, HDFS_CLIENT, HDFS_DATANODE_AP_1_0, &err);
        result = ISILON_ASSERT_ERROR( dn, ISILON_ERR_CONNECT_TO_DATANODE_FAIL, err,
                                      block->ob_val._located_block._locs[0]->ob_val._datanode_info._hostname,
                                      block->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

        if ( !result.ok() )
        {
            isilonFreeHDFSObjs( 2, &exception, &block_seq);
            *status = EIO;

            return result;
        }

        ISILON_LOG( "\t\t\t\tData node discovered: %s",
                    block->ob_val._located_block._locs[0]->ob_val._datanode_info._hostname);

        /* For each block, read the relevant part into the buffer */
        if ( block_offset < offset )
        {
            block_begin = offset - block_offset;
        }

        if ( block_offset + block_end > offset + len)
        {
            block_end = offset + len - block_offset;
        }

        err = hdfs_datanode_read( dn, block_begin/* offset in block */,
                                  block_end - block_begin/* len */, buf, false);
        result = ISILON_ASSERT_ERROR( !err, ISILON_ERR_READ_FAIL, err);
        hdfs_datanode_delete( dn);

        if ( !result.ok() )
        {
            isilonFreeHDFSObjs( 2, &exception, &block_seq);
            *status = EIO;

            return result;
        }

        ISILON_LOG( "\t\t\t\tRead %ld bytes from block offset %ld",
                    block_end - block_begin, block_begin);
        buf = (char *)buf + (block_end - block_begin);
        to_read -= (block_end - block_begin);

        if ( !to_read )
        {
            break;
        }
    }

    return result;
}

/**
 * Read data from a local buffer and fetch them from
 * HDFS Data Node (if the buffer is empty)
 */
ISILON_LOCAL irods::error isilonReadBuf( struct hdfs_namenode *nn,
                                         int id,
                                         char *buf,
                                         int len,
                                         int *bytes_read,
                                         int *status)
{
    irods::error result = SUCCESS();

    isilonFileDesc *fd = 0;

    result = isilonGetFileDescByID( id, &fd);
    ISILON_ERROR_CHECK_PASS( result);
    result = ISILON_ASSERT_ERROR( fd->getMode() == ISILON_MODE_READ,
                                  ISILON_ERR_UNEXPECTED_MODE, fd->getMode(),
                                  ISILON_MODE_READ);
    ISILON_ERROR_CHECK( result);
    result = ISILON_ASSERT_ERROR( fd->getFileSize() >= fd->getOffset(),
                                  ISILON_ERR_UNEXPECTED_OFFSET, id,
                                  fd->getFileSize(), fd->getOffset());
    ISILON_ERROR_CHECK( result);

    const int rbuff_size = fd->getBuffSize();
    int buf_offset = 0;
    /* We've checked above that the file size is greater or
       equal to the offset. So the subtraction below is non-negative */
    long long to_read = fd->getFileSize() - fd->getOffset();

    to_read = (len < to_read) ? len : to_read;
    ISILON_LOG( "\t\tBytes requested: %d", len);
    ISILON_LOG( "\t\tBytes available for reading (in file): %lld", 
                fd->getFileSize() - fd->getOffset());
    ISILON_LOG( "\t\tBytes to read: %lld", to_read);

    while ( to_read )
    {
        char *rbuff = 0;
        /* When reading data "BuffOffset" property of file descriptor
           actually means "number of bytes available in a buffer" */
        int rbuff_offset = fd->getBuffOffset();
        int bytes_taken = 0;

        if ( to_read >= rbuff_size && rbuff_offset == 0 )
        {
            /* read buffer is empty and size of data to read is
               bigger than buffer size. Bufferization step can be skipped.
               The data will be transferred directly from HDFS to output buffer */
            rbuff = buf + buf_offset;
            buf_offset += rbuff_size;
            bytes_taken = rbuff_size;
            to_read -= rbuff_size;
        } else
        {
            if ( rbuff_offset > to_read )
            {
                result = fd->readFromBuff( buf, buf_offset, to_read);
                ISILON_ERROR_CHECK_PASS( result);
                buf_offset += to_read;
                bytes_taken = to_read;
                to_read = 0;
            } else
            {
                /* We fall here not only in case when there is some data in
                   the buffer but also when the buffer is not even allocated.
                   In the latter case a code below makes no harm to offsets
                   and buffers (becase call to "readFromBuff" actually reads
                   zero bytes) */
                result = fd->readFromBuff( buf, buf_offset, rbuff_offset);
                ISILON_ERROR_CHECK_PASS( result);
                to_read -= rbuff_offset;
                buf_offset += rbuff_offset;
                bytes_taken = rbuff_offset;
                result = fd->flushBuff( (const char **)&rbuff);
                ISILON_ERROR_CHECK_PASS( result);
            }

            /* We use 'rbuff_size - rbuff_offset - bytes_taken' for buffer payload
               size instead of 'fd->getBuffSize()' because the buffer might be flushed */
            ISILON_LOG( "\t\t%d bytes read. Currently remains in buffer: %d",
                        bytes_taken, rbuff_offset - bytes_taken);
        }

        if ( rbuff )
        {
            /* Fill block from HDFS */
            ISILON_LOG( "\t\tBuffer is empty. Filling from HDFS");

            long long offset = fd->getOffset();

            offset += fd->getBuffOffset() ? bytes_taken : 0;

            long long reminder = fd->getFileSize() - offset;
            int to_get = reminder > rbuff_size ? rbuff_size : reminder;

            if ( to_get )
            {
                result = isilonFillBufferFromHDFS( nn, (fd->getPath()).c_str(), rbuff,
                                                   offset, to_get, status);
                ISILON_ERROR_CHECK_PASS( result);
            }
        }

        /* Only at this point we can be sure that data were successfully read
           from temporary buffer or directly from HDFS. Only at this point we
           can adjust the file offset */
#ifdef ISILON_DEBUG
        result = isilonSetObjOffsetByID( id, fd->getOffset() + bytes_taken);
        ISILON_ERROR_CHECK_PASS( result);
#else
        fd->setOffset( fd->getOffset() + bytes_taken);
#endif
    }

    *bytes_read = buf_offset; 

    return result;
}

/**
 * Stat HDFS path
 */
ISILON_LOCAL irods::error isilonStatPath( isilonConnectionDesc* conn,
                                          const char *path,
                                          struct stat *_statbuf,
                                          int *status)
{
    irods::error result = SUCCESS();
    /* check incoming parameters */
    bool check_expr = conn && path && _statbuf && status;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);

    struct hdfs_namenode *nn = conn->getNameNode();
    struct hdfs_object *fstatus = 0;

    *status = 0;
    result = isilonGetHDFSFileInfo( nn, path, &fstatus, status);
    ISILON_ERROR_CHECK_PASS( result);

    struct hdfs_file_status *f_stat = &fstatus->ob_val._file_status;

    _statbuf->st_size = f_stat->_size;
    _statbuf->st_blksize = f_stat->_block_size;
    _statbuf->st_mode = f_stat->_permissions | (f_stat->_directory ? S_IFDIR : S_IFREG);
    _statbuf->st_nlink = 1;
    _statbuf->st_uid = getuid();
    _statbuf->st_gid = getgid();
    _statbuf->st_atim.tv_sec = f_stat->_atime / 1000;
    _statbuf->st_mtim.tv_sec = f_stat->_mtime / 1000;
    _statbuf->st_ctim.tv_sec = _statbuf->st_mtime;

    ISILON_LOG( "\t\tSize: %ld", _statbuf->st_size);
    ISILON_LOG( "\t\tBlock size: %ld", _statbuf->st_blksize);
    ISILON_LOG( "\t\tMode: 0x%x", _statbuf->st_mode);
    ISILON_LOG( "\t\tNLink: %lu Uid: %d Gid: %d",
                _statbuf->st_nlink, _statbuf->st_uid, _statbuf->st_gid);
    ISILON_LOG( "\t\tAccess time: %s", ctime( &_statbuf->st_atim.tv_sec));
    ISILON_LOG( "\t\tModification time: %s", ctime( &_statbuf->st_mtim.tv_sec));
    ISILON_LOG( "\t\tCreation time: %s", ctime( &_statbuf->st_ctim.tv_sec));
    isilonFreeHDFSObjs( 1, &fstatus);

    return result;
} // isilonStatPath

/**
 * Open HDFS file and return corresponding ID
 */
ISILON_LOCAL irods::error isilonOpenFile( isilonConnectionDesc *conn,
                                          const char           *path,
                                          int                  flags,
                                          int                  mode,
                                          int                  *file_id,
                                          int                  *status)
{
    irods::error result = SUCCESS();
    /* check incoming parameters */
    bool check_expr = conn && path && file_id && status;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);
    *status = 0;
    
    struct hdfs_namenode *nn = conn->getNameNode();
    struct hdfs_object *fstat = 0;
    
    /* Check if the file exists on the filesystem */
    result = isilonGetHDFSFileInfo( nn, path, &fstat, status);

    if ( !result.ok() )
    {
        isilonFreeHDFSObjs( 1, &fstat);

        return PASS( result);
    }

    if ( ((flags & O_RDWR) || (flags & O_WRONLY)) && (flags & O_TRUNC) )
    {
        result = isilonCreateFile( conn, path, mode, true, file_id, status);
        
        if ( !result.ok() )
        {
            isilonFreeHDFSObjs( 1, &fstat);

            return PASS( result);
        }

    } else
    {
        if ( flags & O_WRONLY )
        {
            result = isilonAppendFile( conn, path, file_id, status);

            if ( !result.ok() )
            {
                isilonFreeHDFSObjs( 1, &fstat);

                return PASS( result);
            }
        } else if ( flags == O_RDONLY )
        {
            *file_id = isilonNewFileDesc( ISILON_MODE_READ, path,
                                          conn->getBuffSize(), 0);
        } else
        {
            *file_id = isilonNewFileDesc( ISILON_MODE_UNKNOWN, path,
                                          conn->getBuffSize(), 0);
        }

        isilonFileDesc *fd = 0;

        /* Indirect access below doesn't look beautiful: just created an object and use
           indirect ID to access it. May be not very good design. May be we need a way
           to access file desctiptor structure after creation directly (without mapping
           its ID to the pointer) */
        isilonGetFileDescByID( *file_id, &fd);
        fd->setFileSize( fstat->ob_val._file_status._size);
    }

    isilonFreeHDFSObjs( 1, &fstat);

    return result;
} // isilonOpenFile

/**
 * Read from HDFS file
 */
ISILON_LOCAL irods::error isilonReadFile( isilonConnectionDesc* conn,
                                          int                   file_id,
                                          void                  *_buf,
                                          int                   _len,
                                          int                   *bytes_read,
                                          int                   *status)
{
    irods::error result = SUCCESS();
    struct hdfs_namenode *nn = conn->getNameNode();

#ifdef ISILON_DEBUG
    isilonFileDesc *fd = 0;

    result = isilonGetFileDescByID( file_id, &fd);
    ISILON_ERROR_CHECK_PASS( result);
    ISILON_LOG( "\tFile to read from: %s (id: %d)", fd->getPath().c_str(), file_id);
#endif
    result = isilonReadBuf( nn, file_id, (char *)_buf,
                            _len, bytes_read, status);
    ISILON_ERROR_CHECK_PASS( result);

    return result;
} // isilonReadFile

/**
 * Copy _src_file_name file contents to archive
 */
ISILON_LOCAL irods::error isilonCopyToArch( irods::resource_plugin_context& _ctx, 
                                            const char *_src_file_name)
{
    irods::error result = SUCCESS();
    struct stat statbuf;
    int status = stat( _src_file_name, &statbuf);

    ISILON_ASSERT_ERROR_CHECK( result, status >= 0, 
                               ISILON_ERR_LOCAL_FILE_STAT,
                               _src_file_name, 
                               errno);
    ISILON_ASSERT_ERROR_CHECK( result, statbuf.st_mode & S_IFREG,
                               ISILON_ERR_REGULAR_FILE_EXPECTED,
                               _src_file_name);
                               
    
    rodsLong_t bytesCopied = 0L;

    {
        unix_file_handle src( open( _src_file_name, O_RDONLY, 0), close);

        ISILON_ASSERT_ERROR_CHECK( result, src.get() >= 0,
                                   ISILON_ERR_LOCAL_FILE_OPEN, 
                                   _src_file_name, 
                                   errno);

        class isilonConnectionDesc *conn = 0;

        ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);

        irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());
        int status = 0, file_id = 0;

        result = isilonCreateFile( conn, fco->physical_path().c_str(), fco->mode(),
                                   true, &file_id, &status);
        ISILON_ERROR_CHECK_PASS( result);

        int bytesRead = 0;
        char *buf = (char *)malloc( conn->getBuffSize());

        result = ISILON_ASSERT_ERROR( buf, ISILON_ERR_NO_MEM);
        ISILON_ERROR_CHECK( result);

        while ( (bytesRead = read( src.get(), buf, conn->getBuffSize())) > 0 ) 
        {
            int status = 0;

            /* We do not return error status immediately, since we want to
               close allocated file descriptor first */
            if ( !isilonWriteFile( conn, file_id, buf, bytesRead, &status).ok() )
            {
                break;
            }

            bytesCopied += bytesRead;
        }

        free( buf);
        result = isilonCloseFile( conn, file_id, &status);
        ISILON_ERROR_CHECK_PASS( result);
    }

    result = ISILON_ASSERT_ERROR( bytesCopied == statbuf.st_size, 
                                  ISILON_ERR_SYNC_STAGE_INV_LEN,
                                  bytesCopied, 
                                  statbuf.st_size, 
                                  _src_file_name );

    return result;
} // isilonCopyToArch

/**
 * Copy file from HDFS to _dst_file_name on local File system
 */
ISILON_LOCAL irods::error isilonCopyFromArch( irods::resource_plugin_context& _ctx,
                                              const char *_dst_file_name)
{
    irods::error result = SUCCESS();

    class isilonConnectionDesc *conn = 0;

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);

    irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco());
    struct stat statbuf;
    const char *path = fco->physical_path().c_str();
    int status = 0;

    result = isilonStatPath( conn, path, &statbuf, &status);
    ISILON_ERROR_CHECK_PASS( result);
    ISILON_ASSERT_ERROR_CHECK( result, (statbuf.st_mode & S_IFREG), 
                               ISILON_ERR_REGULAR_FILE_EXPECTED,
                               path);

    rodsLong_t bytesCopied = 0L;

    {
        const int flags = O_WRONLY | O_CREAT | O_TRUNC;
        unix_file_handle dst( open( _dst_file_name, flags, fco->mode()), close);

        ISILON_ASSERT_ERROR_CHECK( result, dst.get() >= 0,
                                   ISILON_ERR_LOCAL_FILE_OPEN, 
                                   _dst_file_name, 
                                   errno);

        int status = 0, file_id = 0;

        result = isilonOpenFile( conn, fco->physical_path().c_str(), O_RDONLY,
                                 /* Passing zero as a mode, since this parameter
                                    is not used under "O_RDONLY" flag */
                                 0,
                                 &file_id, &status); 
        ISILON_ERROR_CHECK_PASS( result);

        rodsLong_t bytesLeft = statbuf.st_size;
        int buf_size = conn->getBuffSize();
        char *buf = (char *)malloc( buf_size);

        result = ISILON_ASSERT_ERROR( buf, ISILON_ERR_NO_MEM);
        ISILON_ERROR_CHECK( result);

        while( bytesLeft > 0 )
        {
            int toWrite = bytesLeft < buf_size ? bytesLeft : buf_size;
            int status = 0, bytes_read = 0;

            /* We do not return error status immediately, since we want to
               close allocated file descriptor first */
            if ( !isilonReadFile( conn, file_id, buf, toWrite,
                                  &bytes_read, &status).ok()
                 || (bytes_read != toWrite) )
            {
                break;
            }

            int bytesWritten = write( dst.get(), buf, toWrite);

            if ( bytesWritten <= 0 )
            {
                break;
            }
            
            bytesLeft -= toWrite;
            bytesCopied += bytesWritten;
        }

        free( buf);
        result = isilonCloseFile( conn, file_id, &status);
        ISILON_ERROR_CHECK_PASS( result); 
    }

    result = ISILON_ASSERT_ERROR( bytesCopied == statbuf.st_size, 
                                  ISILON_ERR_SYNC_STAGE_INV_LEN,
                                  bytesCopied, 
                                  statbuf.st_size, 
                                  path);

    return result;
} // isilonCopyFromArch

/**
 * Calculate vote for creating a particular object on this resource
 */
ISILON_LOCAL irods::error isilonRedirectCreate( irods::plugin_property_map&   _prop_map,
                                                irods::file_object_ptr        _file_obj,
                                                const std::string&            _resc_name,
                                                const std::string&            _curr_host,
                                                float&                        _out_vote)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "\tRedirect Create executed");

    /* Check if the resource is down */
    int resc_status = 0;
    irods::error get_ret = _prop_map.get<int>( irods::RESOURCE_STATUS, resc_status);

    result = ISILON_ASSERT_PASS( get_ret, ISILON_ERR_GET_RESC_STATUS_FAIL);
    ISILON_ERROR_CHECK( result);

    /* If the status is down, vote no */
    if ( INT_RESC_STATUS_DOWN == resc_status )
    {
        _out_vote = 0.0;
        result.code( SYS_RESC_IS_DOWN );
    } else
    {
        _out_vote = 1.0;
    }

    ISILON_LOG( "\tRedirect Create completed with vote: %4.2f", _out_vote);

    return result;
} // isilonRedirectCreate

/**
 * Calculate vote for opening particular object on this resource
 */
ISILON_LOCAL irods::error isilonRedirectOpen( irods::plugin_property_map&   _prop_map,
                                              irods::file_object_ptr        _file_obj,
                                              const std::string&            _resc_name,
                                              const std::string&            _curr_host,
                                              float&                        _out_vote)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "\tRedirect Open executed");

    /* initially set a conservative default */
    _out_vote = 0.0;

    /* determine if the resource is down */
    int resc_status = 0;
    irods::error ret = _prop_map.get<int>( irods::RESOURCE_STATUS, resc_status);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_GET_RESC_STATUS_FAIL);
    ISILON_ERROR_CHECK( result);

    /* if the status is down, vote no. */
    if ( INT_RESC_STATUS_DOWN == resc_status )
    {
        result.code( SYS_RESC_IS_DOWN);
        result = PASS( result);

        return result;
    }

    /* make some flags to clarify decision making */
    bool need_repl = (_file_obj->repl_requested() > -1);

    /* set up variables for iteration */
    std::vector<irods::physical_object> objs = _file_obj->replicas();
    std::vector<irods::physical_object>::iterator itr = objs.begin();

    /* check to see if the replica is in this resource, if one is requested */
    for ( ; itr != objs.end(); ++itr )
    {
        /* run the hier string through the parser and get the last entry */
        std::string last_resc;
        irods::hierarchy_parser parser;

        parser.set_string( itr->resc_hier());
        parser.last_resc( last_resc);

        /* more flags to simplify decision making */
        bool repl_eq  = (_file_obj->repl_requested() == itr->repl_num());
        bool resc_us  = (_resc_name == last_resc);
        bool is_dirty = ( itr->is_dirty() != 1 );

        if ( !resc_us )
        {
            continue;
        }

        /* success - correct resource and don't need a specific
           replication, or the repl nums match */

        /* if a specific replica is requested then we
           ignore all other criteria */
        if ( need_repl )
        {
            if ( repl_eq )
            {
                _out_vote = 1.0;
            } else
            {
                /* repl requested and we are not it, vote very low */
                _out_vote = 0.25;
            }
        } else
        {
            /* if no repl is requested consider dirty flag */
            if ( is_dirty )
            {
                /* repl is dirty, vote very low */
                _out_vote = 0.25;
            } else
            {
                _out_vote = 1.0;
            }
        }
    }

    ISILON_LOG( "\tRedirect Open completed with vote: %4.2f", _out_vote);

    return result;
} // isilonRedirectOpen

/**
 * END: Auxiliary functions
 */

/**
 * BEGIN: Plugin Interfaces
 */

extern "C" {

/**
 * Interface for Plugin Start
 */
irods::error isilonStartOperation( irods::plugin_property_map& _prop_map,
                                   irods::resource_child_map& _child_map)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Start operation executed");
    ISILON_LOG( "Start operation completed");

    return result;
}

/**
 * Interface for Plugin Stop
 */
irods::error isilonStopOperation( irods::plugin_property_map& _prop_map,
                                  irods::resource_child_map& _child_map)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Stop operation executed");

    result = isilonCleanObjDescTable();
    ISILON_ERROR_CHECK_PASS( result);

    result = isilonReleaseConnections();
    /* Returning just before another "return" so that successful
       operation completion wouldn't appear in the log */
    ISILON_ERROR_CHECK_PASS( result);

    ISILON_LOG( "Stop operation completed");

    return result;
}

/**
 * Interface for file registration
 */
irods::error isilonRegisteredPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Registered operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);

    ISILON_LOG( "Registered operation completed");

    return result;
}

/**
 * Interface for file unregistration
 */
irods::error isilonUnregisteredPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Unregistered operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);

    ISILON_LOG( "Unregistered operation completed");

    return result;
}

/**
 * Interface for file modification
 */
irods::error isilonModifiedPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Modified operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);

    ISILON_LOG( "Modified operation completed");

    return result;
}

/**
 * Interface to notify of a file operation
 */
irods::error isilonNotifyPlugin( irods::resource_plugin_context& _ctx,
                                 const std::string*              _opr)
{
    irods::error result = SUCCESS();

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);

    return result;
}

/**
 * Interface for POSIX create
 */
irods::error isilonFileCreatePlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Create operation executed");

    class isilonConnectionDesc *conn = 0;
    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);

    int status = 0, file_id = 0;

    result = isilonCreateFile( conn, fco->physical_path().c_str(),
                               fco->mode(), false, &file_id, &status);

    if ( !result.ok() )
    {
        status = ISILON_ERR_CODE_FILE_CREATE_ERR - status;
        result.code( status);
        fco->file_descriptor( status);

        return PASS( result);
    }

    result.code( file_id);
    fco->file_descriptor( result.code());
    ISILON_LOG( "Create operation completed");    

    return result;
}

/**
 * Interface for POSIX Open
 */
irods::error isilonFileOpenPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Open operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    /* get ref to fco */
    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());

    class isilonConnectionDesc *conn = 0;

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);

    int status = 0, file_id = 0;
   
    result = isilonOpenFile( conn, fco->physical_path().c_str(),
                             fco->flags(), fco->mode(), &file_id, &status);

    if ( !result.ok() )
    {
        fco->file_descriptor( -1);
        result.code( ISILON_ERR_CODE_FILE_OPEN_ERR - status);

        return PASS( result);
    }

    result.code( file_id);
    fco->file_descriptor( result.code());
    ISILON_LOG( "Open operation completed");

    return result;
}

/**
 * Interface for POSIX Read
 */
irods::error isilonFileReadPlugin( irods::resource_plugin_context& _ctx,
                                   void*                           _buf,
                                   int                             _len)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Read operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    class isilonConnectionDesc *conn = 0; 

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);
      
    /* get ref to fco */
    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());

    int status = 0;
    isilonFileDesc *fd = 0;

    result = isilonGetFileDescByID( fco->file_descriptor(), &fd);
    ISILON_ERROR_CHECK_PASS( result);

    /* This is the cost of allowing RW mode at file open.
       We are forced to do this check for every read operation :( */
    if ( fd->getMode() == ISILON_MODE_UNKNOWN )
    {
#ifdef ISILON_DEBUG
        result = fd->setMode( ISILON_MODE_READ);
        ISILON_ERROR_CHECK_PASS( result);
#else
        fd->setMode( ISILON_MODE_READ);
#endif
    }

    int bytes_read = 0;

    result = isilonReadFile( conn, fco->file_descriptor(), _buf, _len,
                             &bytes_read, &status);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_READ_ERR - status);

        return PASS( result);
    }

    result.code( bytes_read);
    ISILON_LOG( "Read operation completed");

    return result;
}

/**
 * Interface for POSIX Write
 */
irods::error isilonFileWritePlugin( irods::resource_plugin_context& _ctx,
                                    void                            *_buf,
                                    int                             _len)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Write operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);
    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    class isilonConnectionDesc *conn = 0;

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);
    
    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());
    int status = 0;
    isilonFileDesc *fd = 0;
    struct hdfs_object *last_block = 0;

    result = isilonGetFileDescByID( fco->file_descriptor(), &fd);
    ISILON_ERROR_CHECK_PASS( result);

    /* This the cost of allowing RW mode at file open.
       We are forced to do this check for every white operation :( */
    if ( fd->getMode() == ISILON_MODE_UNKNOWN )
    {
        result = isilonAppendFileImpl( conn, fd->getPath().c_str(),
                                       &last_block, &status);

        if ( !result.ok() )
        {
            /* Yes, ignore "status" code from "isilonAppendFileImpl" */
            result.code( ISILON_ERR_CODE_FILE_WRITE_ERR - EIO);

            return PASS( result);
        }

#ifdef ISILON_DEBUG
        result = fd->setLastBlock( last_block);
        ISILON_ERROR_CHECK_PASS( result);
        result = fd->setMode( ISILON_MODE_WRITE);
        ISILON_ERROR_CHECK_PASS( result);
#else
        fd->setLastBlock( last_block);
        fd->setMode( ISILON_MODE_WRITE);
#endif 
    }

    result = isilonWriteFile( conn, fco->file_descriptor(), _buf, _len, &status);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_WRITE_ERR - status);

        return PASS( result);
    }

    result.code( _len);
    ISILON_LOG( "Write operation completed");
    
    return result;
}

/**
 * Interface for POSIX Close
 */
irods::error isilonFileClosePlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Close operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    /* get ref to fco */
    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());
    class isilonConnectionDesc *conn = 0; 

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn); 

    int status = 0;

    result = isilonCloseFile( conn, fco->file_descriptor(), &status);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_CLOSE_ERR - status);

        return PASS( result);
    }

    result.code( 0);
    ISILON_LOG( "Close operation completed");

    return result;
}

/**
 * Interface for Unlink
 *
 * Not a POSIX Unlink. Just removal of a file
 */
irods::error isilonFileUnlinkPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Unlink operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());
    const char *path = fco->physical_path().c_str();
    int status = 0;

    result = isilonUnlinkHDFSObj( _ctx, path, &status);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_UNLINK_ERR - status);

        return PASS( result);
    }

    result.code( 0);
    ISILON_LOG( "Unlink operation completed");

    return result;
}

/**
 * Interface for POSIX Stat
 */
irods::error isilonFileStatPlugin( irods::resource_plugin_context& _ctx,
                                   struct stat*                    _statbuf)
{ 
    irods::error result = SUCCESS();

    ISILON_LOG( "Stat operation executed");

    irods::error ret = _ctx.valid();

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_CONTEXT);
    ISILON_ERROR_CHECK( result);

    irods::data_object_ptr fco = boost::dynamic_pointer_cast<irods::data_object>( _ctx.fco());
    class isilonConnectionDesc *conn = 0; 

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);

    int status = 0;

    result = isilonStatPath( conn, fco->physical_path().c_str(), _statbuf, &status);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_STAT_ERR - status);

        return PASS( result);
    }

    result.code( 0);
    ISILON_LOG( "Stat operation completed");

    return result;
}
    
/**
 * Interface for POSIX lseek
 */
irods::error isilonFileLseekPlugin( irods::resource_plugin_context& _ctx, 
                                    long long                       _offset, 
                                    int                             _whence )
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Lseek operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());
    int file_id = fco->file_descriptor();
    int status = 0;
    long long offset = 0;
    isilonFileDesc *fd = 0;

    result = isilonGetFileDescByID( file_id, &fd);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_LSEEK_ERR - EINVAL);

        return PASS( result);
    }

    long long file_size = fd->getFileSize();

    switch ( _whence )
    {
        case SEEK_SET:
            /* Nothing to do: base offset is already at zero */

            break;
        case SEEK_CUR:
            offset = fd->getOffset();

            break;
        case SEEK_END:
            offset = file_size;

            break;
        default:
            status = EINVAL;

            break;
    }

    if ( status )
    {
        result.code( ISILON_ERR_CODE_FILE_LSEEK_ERR - status);

        return PASS( result);
    }

    offset += _offset;

#ifdef ISILON_DEBUG
    std::string whence_str;

    whence_str = _whence == SEEK_SET ? "SEEK_SET" :
        _whence == SEEK_CUR ? "SEEK_CUR" :
        "SEEK_END";

    ISILON_LOG( "\tSeeking file %s (id: %d)", fd->getPath().c_str(), file_id);
    ISILON_LOG( "\t\tOffset: %lld", _offset);
    ISILON_LOG( "\t\tWhence: %s", whence_str.c_str());
    ISILON_LOG( "\t\tCalculated offset: %lld", offset);
#endif

    if ( offset < 0 || offset > file_size )
    {
        result.code( ISILON_ERR_CODE_FILE_LSEEK_ERR - EINVAL);

        return PASS( result);
    }

    /* Check if we should reload a buffer */
    long long delta_in_file = offset - fd->getOffset();
    int buff_offset = fd->getBuffOffset();
    int new_buff_offset = 0;

    if ( buff_offset )
    {
        if ( delta_in_file > 0 )
        {
            if ( buff_offset > delta_in_file )
            {
                new_buff_offset = buff_offset - delta_in_file;
            }
        } else
        {
            if ( buff_offset - delta_in_file <= (long)fd->getBuffSize() )
            {
                new_buff_offset = buff_offset - delta_in_file;
            }
        }

        fd->seekBuff( new_buff_offset);
    }

    ISILON_LOG( "\t\tBuff offset before: %d", buff_offset);
    ISILON_LOG( "\t\tBuff offset after: %d", new_buff_offset);
    result.code( offset);
#ifdef ISILON_DEBUG
    isilonSetObjOffsetByID( file_id, offset);
#else
    fd->setOffset( offset);
#endif
    ISILON_LOG( "Lseek operation completed");

    return result;
}

/**
 * Interface for POSIX mkdir
 *
 * It's assumed inside this function that complete physical path
 * was provided as a parameter. No vault path prepending is required
 * under this assumption
 */
irods::error isilonFileMkdirPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Mkdir operation executed");

    irods::error ret = _ctx.valid<irods::collection_object>();

    if( !(result = ISILON_ASSERT_PASS(ret, ISILON_ERR_INVALID_CONTEXT)).ok() )
    {
        return result;
    }

    /* cast down the chain to our understood object type */
    irods::collection_object_ptr fco = boost::dynamic_pointer_cast<irods::collection_object>( _ctx.fco());
    class isilonConnectionDesc *conn = 0;
    struct hdfs_namenode *nn = 0;

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);
    nn = conn->getNameNode();

    int status = 0;

    result = isilonCreateHDFSPath( nn, fco->physical_path().c_str(),
                                   fco->mode(), &status);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_MKDIR_ERR - status);

        return PASS( result);
    }

    result.code( 0); 
    ISILON_LOG( "Mkdir operation completed");

    return result;
}

/**
 * Interface for POSIX rmdir
 */
irods::error isilonFileRmdirPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Rmdir operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    /* cast down the chain to our understood object type */
    irods::collection_object_ptr fco = boost::dynamic_pointer_cast<irods::collection_object>( _ctx.fco());
    const char *path = fco->physical_path().c_str();
    int status = 0;

    result = isilonUnlinkHDFSObj( _ctx, path, &status);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_RMDIR_ERR - status);

        return PASS( result);
    }

    result.code( 0);
    ISILON_LOG( "Rmdir operation completed");

    return result;
}

/**
 * Interface for POSIX opendir
 */
irods::error isilonFileOpendirPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Opendir operation executed");

    irods::error ret = isilonCheckParamsAndPath<irods::collection_object>( _ctx);

    if( !(result = ISILON_ASSERT_PASS(ret, ISILON_ERR_INVALID_PARAMS)).ok() )
    {
        return result;
    }

    /* cast down the chain to our understood object type */
    irods::collection_object_ptr fco = boost::dynamic_pointer_cast<irods::collection_object>( _ctx.fco());
    class isilonConnectionDesc *conn = 0;
    struct hdfs_namenode *nn = 0;

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);
    nn = conn->getNameNode();
    
    struct hdfs_object *exception = 0, *dir_list = 0;
    const char *path = fco->physical_path().c_str();

    ISILON_LOG( "\tAcquiring directory listing for path %s", path);
    dir_list = hdfs_getListing( nn, path, 0, &exception);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_DIR_LIST_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        int status = 0;

        isilonGetErrCodeFromException( exception, &status);
        result.code( ISILON_ERR_CODE_FILE_OPENDIR_ERR - status);
        isilonFreeHDFSObjs( 2, &exception, &dir_list);

        return result;
    }

    result = ISILON_ASSERT_ERROR( dir_list->ob_type != H_NULL, ISILON_ERR_PATH_NOT_EXIST,
                                  path);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_OPENDIR_ERR - ENOENT);
        isilonFreeHDFSObjs( 2, &exception, &dir_list);
        
        return result;
    }

    ISILON_LOG( "\t\tList acquired");
    /* ATTENTION! iRODS expects that DIR* will be returned. But on Linux
       DIR declaration if opaque. So from formal point of view, casting
       an integer to DIR* is not much worse than casting any other type.
       It can be expected (at least we hope so!) that on Linux iRODS will not
       alter DIR structures manually: a pointer to DIR created inside a plugin
       will be transferred for altering to plugin only.
       Arbitrary value 1 is added to an actual descriptor because zero
       descriptor is possible and may compromise DIR pointer that is set here */
    fco->directory_pointer( (DIR *)((long long)(1 + isilonNewDirDesc( path, dir_list))));
    result.code( 0);
    /* Should be careful not to free dir_list object */
    isilonFreeHDFSObjs( 1, &exception);
    ISILON_LOG( "Opendir operation completed");

    return result;
}

/**
 * Interface for POSIX closedir
 */
irods::error isilonFileClosedirPlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Closedir operation executed");

    irods::error ret = isilonCheckParamsAndPath<irods::collection_object>( _ctx);

    if( !(result = ISILON_ASSERT_PASS(ret, ISILON_ERR_INVALID_PARAMS)).ok() )
    {
        return result;
    }

    /* cast down the chain to our understood object type */
    irods::collection_object_ptr fco = boost::dynamic_pointer_cast<irods::collection_object>( _ctx.fco());   
    /* 1 is subtracted because it was artificially added inside
       "isilonFileOpendirPlugin". See comment inside that function for details */
    int dir_id = (long long)fco->directory_pointer() - 1;

    ISILON_LOG( "Id of directory to close: %d", dir_id);
    result = isilonDestroyObjDesc( dir_id);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_CLOSEDIR_ERR - EINVAL);

        return PASS( result);
    }

    result.code( 0);
    ISILON_LOG( "Closedir operation completed");

    return result;
}

/**
 * Interface for POSIX readdir
 */
irods::error isilonFileReaddirPlugin( irods::resource_plugin_context& _ctx,
                                      struct rodsDirent**     _dirent_ptr)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Readdir operation executed");

    irods::error ret = isilonCheckParamsAndPath<irods::collection_object>( _ctx);

    if( !(result = ISILON_ASSERT_PASS(ret, ISILON_ERR_INVALID_PARAMS)).ok() )
    {
        return result;
    }

    /* cast down the chain to our understood object type */
    irods::collection_object_ptr fco = boost::dynamic_pointer_cast<irods::collection_object>( _ctx.fco());
    /* 1 is subtracted because it was artificially added inside
       "isilonFileOpendirPlugin". See comment inside that function for details */
    int dir_id = (long long)fco->directory_pointer() - 1;
    bool is_end = false;

    result = isilonReaddir( dir_id, _dirent_ptr, &is_end);

    if ( !result.ok() )
    {
        result.code( ISILON_ERR_CODE_FILE_READDIR_ERR - EIO);

        return PASS( result);
    }

    if ( is_end )
    {
        /* Yes we return non-zero error code in case of successful
           execution (when the end of a directory was reached). That's
           because of a bug in iRODS. See issue #2226 reported to
           iRODS community for explanation. When the issue is fixed,
           this place should also be fixed */
        result.code( -1);
    } else
    {
        result.code( 0);
    }

    ISILON_LOG( "Readdir operation completed");

    return result;
}

/**
 * Interface for POSIX rename
 */
irods::error isilonFileRenamePlugin( irods::resource_plugin_context& _ctx,
                                     const char*                     _new_file_name)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Rename operation executed");

    /* Check the operation parameters and update the physical path */
    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    std::string new_path;
    ret = isilonGenerateFullPath( _ctx.prop_map(),
                                  _new_file_name,
                                  new_path);
    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_GEN_FULL_PATH_FAIL);
    ISILON_ERROR_CHECK( result);


    irods::file_object_ptr fco = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());
    const char *path = fco->physical_path().c_str();

    ISILON_LOG( "\tOld path: %s", path);
    ISILON_LOG( "\tNew path: %s", new_path.c_str());

    class isilonConnectionDesc *conn = 0;
    struct hdfs_namenode *nn = 0;

    ISILON_GET_CONNECTION( _ctx.prop_map(), &conn);
    nn = conn->getNameNode();

    std::string dirs_only_path = new_path;

    dirs_only_path.erase( dirs_only_path.find_last_of( '/'));
    /* When iRODS bug #2201 is fixed, mode constant value "0750" should be
       replaced with a parameterized value */
    result = isilonCreateHDFSPath( nn, dirs_only_path.c_str(), 0750, 0);
    ISILON_ERROR_CHECK_PASS( result);

    struct hdfs_object *exception = 0;
    bool oper_status = true;
    int status = 0;

    ISILON_LOG( "\tRenaming file");
    oper_status = hdfs_rename( nn, path, new_path.c_str(), &exception);
    result = ISILON_ASSERT_ERROR( !exception, ISILON_ERR_RENAME_FAIL,
                                  exception ? hdfs_exception_get_message( exception) : 0);

    if ( !result.ok() )
    {
        isilonGetErrCodeFromException( exception, &status);
        result.code( ISILON_ERR_CODE_FILE_RENAME_ERR - status);
        isilonFreeHDFSObjs( 1, &exception);

        return result;
    }

    result = ISILON_ASSERT_ERROR( oper_status, ISILON_ERR_FILE_NOT_RENAMED,
                                  path);

    if ( !result.ok() )
    {
        isilonFreeHDFSObjs( 1, &exception);
        result.code( ISILON_ERR_CODE_FILE_RENAME_ERR - EIO);

        return result;
    }

    ISILON_LOG( "\t\tRenamed");
    result.code( 0);
    isilonFreeHDFSObjs( 1, &exception);
    ISILON_LOG( "Rename operation completed");

    return result;
}

/**
 * Interface for POSIX truncate
 *
 * TODO: implement this interface
 */
irods::error isilonFileTruncatePlugin( irods::resource_plugin_context& _ctx)
{ 
    return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__);
}


/**
 * Interface to determine free space on a device given a path
 *
 * TODO: implement this interface
 */
irods::error isilonFileGetFsFreeSpacePlugin( irods::resource_plugin_context& _ctx)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "GetFsFreeSpace operation executed");

    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);

    ISILON_LOG( "GetFsFreeSpace operation completed");

    return result;
}

/**
 * Interface for copying the file to cache-type resource
 */
irods::error isilonStageToCachePlugin( irods::resource_plugin_context& _ctx,
                                       char* _cache_file_name)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "StageToCache operation executed");

    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);
    
    result = isilonCopyFromArch( _ctx, _cache_file_name);
    ISILON_ERROR_CHECK_PASS( result);
    
    ISILON_LOG( "StageToCache operation completed");

    return result;
}

/**
 * Interface for copying the file to archive
 */
irods::error isilonSyncToArchPlugin( irods::resource_plugin_context& _ctx,
                                     char* _cache_file_name)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "SyncToArch operation executed");

    irods::error ret = isilonCheckParamsAndPath( _ctx);

    result = ISILON_ASSERT_PASS( ret, ISILON_ERR_INVALID_PARAMS);
    ISILON_ERROR_CHECK( result);
    
    result = isilonCopyToArch( _ctx, _cache_file_name);
    ISILON_ERROR_CHECK_PASS( result);

    ISILON_LOG( "SyncToArch operation completed");

    return result;
}

/**
 * This function is used to allow the upper-level resource to determine which
 * underlying resource should provide the requested operation
 */
irods::error isilonRedirectPlugin( irods::resource_plugin_context& _ctx,
                                   const std::string*              _opr,
                                   const std::string*              _curr_host,
                                   irods::hierarchy_parser*        _out_parser,
                                   float*                          _out_vote)
{
    irods::error result = SUCCESS();

    ISILON_LOG( "Redirect operation executed");

    irods::error ret;

    /* check the context validity */
    ret = _ctx.valid<irods::file_object>();

    if( !(result = ISILON_ASSERT_PASS(ret, ISILON_ERR_INVALID_CONTEXT)).ok() )
    {
        return result;
    }

    /* check incoming parameters */
    bool check_expr = _opr && _curr_host && _out_parser && _out_vote;

    result = ISILON_ASSERT_ERROR( check_expr, ISILON_ERR_NULL_ARGS);
    ISILON_ERROR_CHECK( result);

    std::string resc_name;
    /* cast down the chain to our understood object type */
    irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>( _ctx.fco());
    /* get the name of this resource */
    ret = _ctx.prop_map().get<std::string>( irods::RESOURCE_NAME, resc_name);

    if ( !(result = ISILON_ASSERT_PASS( ret, ISILON_ERR_GET_RESC_NAME_FAIL)).ok() )
    {
        return result;
    }

    /* add ourselves to the hierarchy parser by default */
    _out_parser->add_child( resc_name);

    /* test the operation to determine which choices to make */
    if( irods::OPEN_OPERATION == (*_opr)
        || irods::WRITE_OPERATION == (*_opr) )
    {
        /* call redirect determination for 'get' operation */
        result = isilonRedirectOpen( _ctx.prop_map(), file_obj, resc_name,
                                     (*_curr_host), (*_out_vote));
    } else if( irods::CREATE_OPERATION == (*_opr) )
    {
        /* call redirect determination for 'create' operation */
        result = isilonRedirectCreate( _ctx.prop_map(), file_obj, resc_name,
                                       (*_curr_host), (*_out_vote));
    } else
    {
        result = ISILON_ASSERT_ERROR( false, ISILON_ERR_INVALID_REDIRECT_OPERATION,
                                      _opr->c_str());
    }

    ISILON_ERROR_CHECK_PASS( result);

    ISILON_LOG( "Redirect operation completed");
    
    return result;
} // isilonRedirectPlugin

/**
 * Rebalance the subtree
 */
irods::error isilonRebalancePlugin( irods::resource_plugin_context& _ctx)
{
    ISILON_LOG( "Rebalance operation executed");
    
    irods::error result = update_resource_object_count( _ctx.comm(), _ctx.prop_map());
    
    ISILON_ERROR_CHECK( result);
    ISILON_LOG( "Rebalance operation completed");
    
    return result;
}

/**
 * Class representing Isilon resource inside iRODS
 */
class isilon_resource : public irods::resource
{
    public:
        isilon_resource( const std::string& _inst_name,
                         const std::string& _context) : irods::resource( _inst_name, _context)
        {    
            /* parse context string into property pairs assuming a ; as a separator */
            std::vector<std::string> props;

            ISILON_LOG( "\t\tContext at creation: %s", _context.c_str());

            irods::string_tokenize( _context, ";", props);

            /* parse key/property pairs using = as a separator and
               add them to the property list */
            std::vector<std::string>::iterator itr = props.begin();

            for( ; itr != props.end(); ++itr )
            {
                /* break up key and value into two strings */
                std::vector<std::string> vals;
                irods::string_tokenize( *itr, "=", vals);

                /* break up key and value into two strings */
                ISILON_LOG( "\t\t\t%s -> %s", vals[0].c_str(), vals[1].c_str());
                properties_[vals[0]] = vals[1];
            }

            std::string host_name, user_name;
            unsigned long port_num = 0;
            unsigned long buf_size = 0;

            isilonParseConnectionProps( properties_, host_name, &port_num,
                                        user_name, &buf_size);

            properties_[ISILON_HOST_KEY] = host_name;
            properties_[ISILON_PORT_KEY] = port_num;
            properties_[ISILON_USER_KEY] = user_name;
            properties_[ISILON_BUFSIZE_KEY] = buf_size;

            /* Add start and stop operations */
            set_start_operation( "isilonStartOperation" );
            set_stop_operation( "isilonStopOperation" );
        }

        irods::error need_post_disconnect_maintenance_operation( bool& _b)
        {
            _b = false;

            return SUCCESS();
        }

        /* pass along a functor for maintenance work after
           the client disconnects, uncomment the first two lines for effect */
        irods::error post_disconnect_maintenance_operation( irods::pdmo_type& _op)
        {
            return SUCCESS();
        }
}; // class isilon_resource

/**
 * Create the plugin factory function which will return a dynamically
 * instantiated object of the previously defined derived resource.  use
 * the add_operation member to associate a 'call name' to the interfaces
 * defined above. For resource plugins these call names are standardized
 * as used by the irods facing interface defined in
 * server/drivers/src/fileDriver.c
 */
irods::resource* plugin_factory(const std::string& _inst_name, const std::string& _context)
{
    ISILON_LOG( "Plugin factory executed");

#ifdef ISILON_DEBUG
    if ( !isilonCheckErrTable() )
    {
        return 0;
    }
#endif

    ISILON_LOG( "\tCreating resource object");

    isilon_resource* resc = new isilon_resource( _inst_name, _context);

    resc->add_operation( irods::RESOURCE_OP_CREATE,       "isilonFileCreatePlugin");
    resc->add_operation( irods::RESOURCE_OP_OPEN,         "isilonFileOpenPlugin");
    resc->add_operation( irods::RESOURCE_OP_READ,         "isilonFileReadPlugin");
    resc->add_operation( irods::RESOURCE_OP_WRITE,        "isilonFileWritePlugin");
    resc->add_operation( irods::RESOURCE_OP_CLOSE,        "isilonFileClosePlugin");
    resc->add_operation( irods::RESOURCE_OP_UNLINK,       "isilonFileUnlinkPlugin");
    resc->add_operation( irods::RESOURCE_OP_STAT,         "isilonFileStatPlugin");
    resc->add_operation( irods::RESOURCE_OP_LSEEK,        "isilonFileLseekPlugin");
    resc->add_operation( irods::RESOURCE_OP_MKDIR,        "isilonFileMkdirPlugin");
    resc->add_operation( irods::RESOURCE_OP_RMDIR,        "isilonFileRmdirPlugin");
    resc->add_operation( irods::RESOURCE_OP_OPENDIR,      "isilonFileOpendirPlugin");
    resc->add_operation( irods::RESOURCE_OP_CLOSEDIR,     "isilonFileClosedirPlugin");
    resc->add_operation( irods::RESOURCE_OP_READDIR,      "isilonFileReaddirPlugin");
    resc->add_operation( irods::RESOURCE_OP_RENAME,       "isilonFileRenamePlugin");
    resc->add_operation( irods::RESOURCE_OP_TRUNCATE,     "isilonFileTruncatePlugin");
    resc->add_operation( irods::RESOURCE_OP_FREESPACE,    "isilonFileGetFsFreeSpacePlugin");
    resc->add_operation( irods::RESOURCE_OP_STAGETOCACHE, "isilonStageToCachePlugin");
    resc->add_operation( irods::RESOURCE_OP_SYNCTOARCH,   "isilonSyncToArchPlugin");
    resc->add_operation( irods::RESOURCE_OP_REGISTERED,   "isilonRegisteredPlugin");
    resc->add_operation( irods::RESOURCE_OP_UNREGISTERED, "isilonUnregisteredPlugin");
    resc->add_operation( irods::RESOURCE_OP_MODIFIED,     "isilonModifiedPlugin");
    resc->add_operation( irods::RESOURCE_OP_NOTIFY,       "isilonNotifyPlugin" );
    resc->add_operation( irods::RESOURCE_OP_RESOLVE_RESC_HIER, "isilonRedirectPlugin");
    resc->add_operation( irods::RESOURCE_OP_REBALANCE,    "isilonRebalancePlugin" );

    /* set some properties necessary for backporting to iRODS legacy code */
    resc->set_property<int>( irods::RESOURCE_CHECK_PATH_PERM, DO_CHK_PATH_PERM);
    resc->set_property<int>( irods::RESOURCE_CREATE_PATH,     CREATE_PATH);

    ISILON_LOG( "Plugin factory completed");

    return dynamic_cast<irods::resource *> ( resc);
} // plugin_factory

/**
 * END: Plugin Interfaces
 */
}; // extern "C"

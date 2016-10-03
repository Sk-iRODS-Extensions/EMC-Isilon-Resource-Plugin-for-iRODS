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

#ifndef _LIBIRODS_ISILON_H_
#define _LIBIRODS_ISILON_H_

#include "utils.hpp"

// =-=-=-=-=-=-=-
// Isilon-specific includes 
extern "C"
{
#include <hadoofus/highlevel.h>
}

// =-=-=-=-=-=-=-
// irods includes
#include "irods_resource_plugin.hpp"

// =-=-=-=-=-=-=-
// STL includes
#include <string>

// =-=-=-=-=-=-=-
// System includes
#ifdef ISILON_DEBUG
#ifdef ISILON_DUMP_THR_ID
#include <sys/types.h>
#include <sys/syscall.h>
#endif
#endif

static const std::string ISILON_HOST_KEY( "isi_host");
static const std::string ISILON_PORT_KEY( "isi_port");
static const std::string ISILON_USER_KEY( "isi_user");
static const std::string ISILON_BUFSIZE_KEY( "isi_buf_size");

#define ISILON_LOCAL static inline

#ifdef ISILON_DEBUG
#ifdef ISILON_DUMP_THR_ID
#define ISILON_LOG( format_, ...) \
            fprintf( stderr, "ISILON RESC (pid: %6ld): ", syscall( SYS_gettid)); \
            fprintf( stderr, format_, ##__VA_ARGS__); \
            fprintf( stderr, "\n")
#else /* ISILON_DUMP_THR_ID */
#define ISILON_LOG( format_, ...) \
            fprintf( stderr, "ISILON RESC: "); \
            fprintf( stderr, format_, ##__VA_ARGS__); \
            fprintf( stderr, "\n")
#endif /* ISILON_DUMP_THR_ID */
#else /* ISILON_DEBUG */
#define ISILON_LOG( format_, ...)
#endif /* ISILON_DEBUG */

#ifdef ISILON_DEBUG
/** 
 *  Check the following things:
 *   1) we should not be out of the range of possible errors
 *   2) For PASS asserts "code" field should be zero
 *   3) For ERROR asserts "code" field should be non-zero
 */
#define ISILON_CHECK_ERROR_CODE( code_, is_pass_) \
            (code_ >= ISILON_ERR_ERROR_TYPES_NUM) \
            || (isilonErrMesgList[code_].code && is_pass_) \
            || (!isilonErrMesgList[code_].code && !is_pass_) ? \
                ASSERT_ERROR( false, \
                              SYS_INVALID_INPUT_PARAM, \
                              "Error code is out of range or doesn't " \
                              "match assert type") :
#else
#define ISILON_CHECK_ERROR_CODE( code_, is_pass_)
#endif

#define ISILON_ASSERT_ERROR( expr_, code_, ...) \
            ISILON_CHECK_ERROR_CODE( code_, 0) \
            ASSERT_ERROR( expr_, \
                          isilonErrMesgList[code_].code, \
                          isilonErrMesgList[code_].mesg, \
                          ##__VA_ARGS__)

#define ISILON_ASSERT_PASS( prev_error_, code_, ...) \
            ISILON_CHECK_ERROR_CODE( code_, 1) \
            ASSERT_PASS( prev_error_, \
                         isilonErrMesgList[code_].mesg, \
                         ##__VA_ARGS__)

#define ISILON_ERROR_RETURN( result_) \
            if ( !(result_).ok() ) return result_

#define ISILON_ERROR_CHECK( var_) \
            ISILON_ERROR_RETURN ( var_)

#define ISILON_ERROR_CHECK_PASS( var_) \
            ISILON_ERROR_RETURN ( PASS( var_))

#define ISILON_ASSERT_ERROR_CHECK( result_, expr_, code_, ...) \
            result_ = ISILON_ASSERT_ERROR( expr_, code_, ##__VA_ARGS__); \
            ISILON_ERROR_RETURN( result_)

enum isilonErrorType
{
    ISILON_ERR_NULL_ARGS,
    ISILON_ERR_NULL_HOST,
    ISILON_ERR_UNKNOWN_OBJ_DESC,
    ISILON_ERR_UNKNOWN_CONNECTION_DESC,
    ISILON_ERR_UNEXPECTED_OBJECT_TYPE,
    ISILON_ERR_INVALID_PARAMS,
    ISILON_ERR_INVALID_CONTEXT,
    ISILON_ERR_INVALID_PORT,
    ISILON_ERR_INVALID_BUFSIZE,
    ISILON_ERR_INVALID_HDFS_PROTOCOL_VERSION,
    ISILON_ERR_INVALID_REDIRECT_OPERATION,
    ISILON_ERR_MODE_NOT_SUPPORTED,
    ISILON_ERR_UNEXPECTED_MODE,
    ISILON_ERR_UNEXPECTED_OFFSET,
    ISILON_ERR_NO_VAULT_PATH,
    ISILON_ERR_NO_MEM,
    ISILON_ERR_NO_ENOUGH_BUFF_SPACE,
    ISILON_ERR_NO_ENOUGH_BUFF_DATA,
    ISILON_ERR_FILE_NOT_EXIST,
    ISILON_ERR_PATH_NOT_EXIST,
    ISILON_ERR_CORRUPTED_OR_INCORRECT_BLOCK,
    ISILON_ERR_FILE_NOT_COMPLETED,
    ISILON_ERR_FILE_NOT_RENAMED,
    ISILON_ERR_FILE_NOT_OPEN_FOR_WRITE,
    ISILON_ERR_FILE_NOT_OPEN_FOR_READ,
    ISILON_ERR_DIRS_NOT_CREATED,
    ISILON_ERR_GET_RESC_STATUS_FAIL,
    ISILON_ERR_GET_RESC_NAME_FAIL,
    ISILON_ERR_GEN_FULL_PATH_FAIL,
    ISILON_ERR_CAST_FCO_FAIL,
    ISILON_ERR_NEW_NAME_NODE_FAIL,
    ISILON_ERR_GET_PROTOCOL_VERSION_FAIL,
    ISILON_ERR_CREATE_FILE_FAIL,
    ISILON_ERR_APPEND_FILE_FAIL,
    ISILON_ERR_GET_BLOCK_LOCATIONS_FAIL,
    ISILON_ERR_CONNECT_TO_DATANODE_FAIL,
    ISILON_ERR_READ_FAIL,
    ISILON_ERR_WRITE_FAIL,
    ISILON_ERR_COMPLETE_FAIL,
    ISILON_ERR_UNLINK_FAIL,
    ISILON_ERR_RENAME_FAIL,
    ISILON_ERR_MKDIRS_FAIL,
    ISILON_ERR_DIR_LIST_FAIL,
    ISILON_ERR_GET_FILE_INFO_FAIL,
    ISILON_ERR_ADD_BLOCK_FAIL,
    ISILON_ERR_SETTING_LAST_BLOCK_FAIL,
    ISILON_ERR_SYNC_STAGE_INV_LEN,
    ISILON_ERR_LOCAL_FILE_OPEN,
    ISILON_ERR_LOCAL_FILE_STAT,
    ISILON_ERR_REGULAR_FILE_EXPECTED,
    ISILON_ERR_ERROR_TYPES_NUM
};

typedef struct isilonErrMesg
{
    long long code;
    const char *mesg;
#ifdef ISILON_DEBUG
    int err_num;
#endif
} isilonErrMesg;

#ifdef ISILON_DEBUG
#define ISILON_ERR_NUM( code) ,code
#else
#define ISILON_ERR_NUM( code)
#endif

/**
 * Error codes specific to Isilon plugin
 *
 * The errors below will be reported by iRODS as "UNKNOWN"
 */
#define ISILON_ERR_CODE_HDFS_INVALID_PROTOCOL_VERSION              -15000000
#define ISILON_ERR_CODE_NO_ENOUGH_MEMORY                           -15000001
#define ISILON_ERR_CODE_HDFS_FILE_NOT_EXIST                        -15000002
#define ISILON_ERR_CODE_HDFS_CORRUPTED_OR_INCORRECT_BLOCK          -15000003
#define ISILON_ERR_CODE_HDFS_FILE_NOT_COMPLETED                    -15000004
#define ISILON_ERR_CODE_HDFS_FILE_NOT_RENAMED                      -15000006
#define ISILON_ERR_CODE_HDFS_DIRS_NOT_CREATED                      -15000007
#define ISILON_ERR_CODE_HDFS_NEW_NAME_NODE_FAIL                    -15000008
#define ISILON_ERR_CODE_HDFS_GET_PROTOCOL_VERSION_FAIL             -15000009
#define ISILON_ERR_CODE_HDFS_CREATE_FILE_FAIL                      -15000010
#define ISILON_ERR_CODE_HDFS_APPEND_FILE_FAIL                      -15000011
#define ISILON_ERR_CODE_HDFS_GET_BLOCK_LOCATIONS_FAIL              -15000012
#define ISILON_ERR_CODE_HDFS_CONNECT_TO_DATANODE_FAIL              -15000013
#define ISILON_ERR_CODE_HDFS_READ_FAIL                             -15000014
#define ISILON_ERR_CODE_HDFS_WRITE_FAIL                            -15000015
#define ISILON_ERR_CODE_HDFS_COMPLETE_FAIL                         -15000016
#define ISILON_ERR_CODE_HDFS_UNLINK_FAIL                           -15000017
#define ISILON_ERR_CODE_HDFS_RENAME_FAIL                           -15000018
#define ISILON_ERR_CODE_HDFS_MKDIRS_FAIL                           -15000019
#define ISILON_ERR_CODE_HDFS_DIR_LIST_FAIL                         -15000020
#define ISILON_ERR_CODE_HDFS_GET_FILE_INFO_FAIL                    -15000021
#define ISILON_ERR_CODE_HDFS_ADD_BLOCK_FAIL                        -15000022
#define ISILON_ERR_CODE_SETTING_LAST_BLOCK_FAIL                    -15000023
#define ISILON_ERR_CODE_REGULAR_FILE_EXPECTED                      -15000024

/**
 * The error codes below signal about general fail of the resource
 * plugin interfaces. It's not good to leave them with error
 * codes unknown to iRODS. Currently we use HDFS-related error
 * codes to refer to these errors
 */
#define ISILON_ERR_CODE_FILE_CREATE_ERR     HDFS_FILE_CREATE_ERR
#define ISILON_ERR_CODE_FILE_OPEN_ERR       HDFS_FILE_OPEN_ERR
#define ISILON_ERR_CODE_FILE_READ_ERR       HDFS_FILE_READ_ERR
#define ISILON_ERR_CODE_FILE_WRITE_ERR      HDFS_FILE_WRITE_ERR
#define ISILON_ERR_CODE_FILE_CLOSE_ERR      HDFS_FILE_CLOSE_ERR
#define ISILON_ERR_CODE_FILE_UNLINK_ERR     HDFS_FILE_UNLINK_ERR
#define ISILON_ERR_CODE_FILE_STAT_ERR       HDFS_FILE_STAT_ERR
#define ISILON_ERR_CODE_FILE_LSEEK_ERR      HDFS_FILE_LSEEK_ERR
#define ISILON_ERR_CODE_FILE_MKDIR_ERR      HDFS_FILE_MKDIR_ERR
#define ISILON_ERR_CODE_FILE_RMDIR_ERR      HDFS_FILE_RMDIR_ERR
#define ISILON_ERR_CODE_FILE_OPENDIR_ERR    HDFS_FILE_OPENDIR_ERR
#define ISILON_ERR_CODE_FILE_CLOSEDIR_ERR   HDFS_FILE_CLOSEDIR_ERR
#define ISILON_ERR_CODE_FILE_READDIR_ERR    HDFS_FILE_READDIR_ERR
#define ISILON_ERR_CODE_FILE_RENAME_ERR     HDFS_FILE_RENAME_ERR

/**
 * Some error codes below were chosen arbitrarily to some degree.
 * There are no strict conventions (or at least weren't at the time this
 * plugin was created) in iRODS about error codes
 */
const isilonErrMesg isilonErrMesgList[ISILON_ERR_ERROR_TYPES_NUM] =
                        {{SYS_INVALID_INPUT_PARAM,
                          "One or more NULL pointer arguments"
                          ISILON_ERR_NUM( ISILON_ERR_NULL_ARGS)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Host name (or IP) should be provided"
                          ISILON_ERR_NUM( ISILON_ERR_NULL_HOST)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Unknown descriptor of File System object: %d"
                          ISILON_ERR_NUM( ISILON_ERR_UNKNOWN_OBJ_DESC)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Unknown connection descriptor"
                          ISILON_ERR_NUM( ISILON_ERR_UNKNOWN_CONNECTION_DESC)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Unexpected type of File System object"
                          ISILON_ERR_NUM( ISILON_ERR_UNEXPECTED_OBJECT_TYPE)},
                         {0,
                          "Invalid parameters or physical path"
                          ISILON_ERR_NUM( ISILON_ERR_INVALID_PARAMS)},
                         {0,
                          "Resource context is invalid"
                          ISILON_ERR_NUM( ISILON_ERR_INVALID_CONTEXT)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Port value %s is invalid"
                          ISILON_ERR_NUM( ISILON_ERR_INVALID_PORT)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Buffer size value %s is invalid, should be an "
                          "integer between 1 and 256"
                          ISILON_ERR_NUM( ISILON_ERR_INVALID_BUFSIZE)},
                         {ISILON_ERR_CODE_HDFS_INVALID_PROTOCOL_VERSION,
                          "HDFS protocol version %d is not supported"
                          ISILON_ERR_NUM( ISILON_ERR_INVALID_HDFS_PROTOCOL_VERSION)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Unknown redirect operation: \"%s\""
                          ISILON_ERR_NUM( ISILON_ERR_INVALID_REDIRECT_OPERATION)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Unsupported file acess mode"
                          ISILON_ERR_NUM( ISILON_ERR_MODE_NOT_SUPPORTED)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Unexpected mode %d. Expecting %d mode"
                          ISILON_ERR_NUM( ISILON_ERR_UNEXPECTED_MODE)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Unexpected offset for file descriptor %d "
                          "(file size: %lld, offset: %lld)"
                          ISILON_ERR_NUM( ISILON_ERR_UNEXPECTED_OFFSET)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Resource has no vault path"
                          ISILON_ERR_NUM( ISILON_ERR_NO_VAULT_PATH)},
                         {ISILON_ERR_CODE_NO_ENOUGH_MEMORY,
                          "Not enough memory"
                          ISILON_ERR_NUM( ISILON_ERR_NO_MEM)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Buffer has %d bytes of free space, while "
                          "%d bytes are requested"
                          ISILON_ERR_NUM( ISILON_ERR_NO_ENOUGH_BUFF_SPACE)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Buffer has %d bytes of available data, while "
                          "%d bytes are requested"
                          ISILON_ERR_NUM( ISILON_ERR_NO_ENOUGH_BUFF_DATA)},
                         {ISILON_ERR_CODE_HDFS_FILE_NOT_EXIST,
                          "%s: file or file region doesn't exist"
                          ISILON_ERR_NUM( ISILON_ERR_FILE_NOT_EXIST)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Path doesn't exist: %s"
                          ISILON_ERR_NUM( ISILON_ERR_PATH_NOT_EXIST)},
                         {ISILON_ERR_CODE_HDFS_CORRUPTED_OR_INCORRECT_BLOCK,
                          "Corrupted or incorrect HDFS block received. Offset: %lld, len: %lld"
                          ISILON_ERR_NUM( ISILON_ERR_CORRUPTED_OR_INCORRECT_BLOCK)},
                         {ISILON_ERR_CODE_HDFS_FILE_NOT_COMPLETED,
                          "File %s was not completed"
                          ISILON_ERR_NUM( ISILON_ERR_FILE_NOT_COMPLETED)},
                         {ISILON_ERR_CODE_HDFS_FILE_NOT_RENAMED,
                          "File %s was not renamed"
                          ISILON_ERR_NUM( ISILON_ERR_FILE_NOT_RENAMED)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Attempt to write to a file which is not "
                          "opened in WRITE mode"
                          ISILON_ERR_NUM( ISILON_ERR_FILE_NOT_OPEN_FOR_WRITE)},
                         {SYS_INVALID_INPUT_PARAM,
                          "Attempt to read from a file which is not "
                          "opened in READ mode"
                          ISILON_ERR_NUM( ISILON_ERR_FILE_NOT_OPEN_FOR_READ)},
                         {ISILON_ERR_CODE_HDFS_DIRS_NOT_CREATED,
                          "Path %s was not created"
                          ISILON_ERR_NUM( ISILON_ERR_DIRS_NOT_CREATED)},
                         {0,
                          "Failed to get \"status\" property of resource"
                          ISILON_ERR_NUM( ISILON_ERR_GET_RESC_STATUS_FAIL)},
                         {0,
                          "Failed to get resource \"name\" property"
                          ISILON_ERR_NUM( ISILON_ERR_GET_RESC_NAME_FAIL)},
                         {0,
                          "Failed to generate full path for object"
                          ISILON_ERR_NUM( ISILON_ERR_GEN_FULL_PATH_FAIL)}, 
                         {SYS_INVALID_INPUT_PARAM,
                          "Failed to cast fco to data_object"
                          ISILON_ERR_NUM( ISILON_ERR_CAST_FCO_FAIL)},
                         {ISILON_ERR_CODE_HDFS_NEW_NAME_NODE_FAIL,
                          "Cannot establish connection with HDFS name node: %s"
                          ISILON_ERR_NUM( ISILON_ERR_NEW_NAME_NODE_FAIL)},
                         {ISILON_ERR_CODE_HDFS_GET_PROTOCOL_VERSION_FAIL,
                          "Error aquiring protocol version: %s"
                          ISILON_ERR_NUM( ISILON_ERR_GET_PROTOCOL_VERSION_FAIL)},
                         {ISILON_ERR_CODE_HDFS_CREATE_FILE_FAIL,
                          "Error creating file: %s"
                          ISILON_ERR_NUM( ISILON_ERR_CREATE_FILE_FAIL)},
                         {ISILON_ERR_CODE_HDFS_APPEND_FILE_FAIL,
                          "Error opening file for append: %s"
                          ISILON_ERR_NUM( ISILON_ERR_APPEND_FILE_FAIL)},
                         {ISILON_ERR_CODE_HDFS_GET_BLOCK_LOCATIONS_FAIL,
                          "Error getting HDFS block locations: %s"
                          ISILON_ERR_NUM( ISILON_ERR_GET_BLOCK_LOCATIONS_FAIL)},
                         {ISILON_ERR_CODE_HDFS_CONNECT_TO_DATANODE_FAIL,
                          "Error connecting to Data Node: %s (%s:%s)"
                          ISILON_ERR_NUM( ISILON_ERR_CONNECT_TO_DATANODE_FAIL)},
                         {ISILON_ERR_CODE_HDFS_READ_FAIL,
                          "Error reading block: %s"
                          ISILON_ERR_NUM( ISILON_ERR_READ_FAIL)},
                         {ISILON_ERR_CODE_HDFS_WRITE_FAIL,
                          "Error writing HDFS block: %s"
                          ISILON_ERR_NUM( ISILON_ERR_WRITE_FAIL)},
                         {ISILON_ERR_CODE_HDFS_COMPLETE_FAIL,
                          "Error completing file: %s"
                          ISILON_ERR_NUM( ISILON_ERR_COMPLETE_FAIL)},
                         {ISILON_ERR_CODE_HDFS_UNLINK_FAIL,
                          "Error removing object: %s"
                          ISILON_ERR_NUM( ISILON_ERR_UNLINK_FAIL)},
                         {ISILON_ERR_CODE_HDFS_RENAME_FAIL,
                          "Error renaming file: %s"
                          ISILON_ERR_NUM( ISILON_ERR_RENAME_FAIL)},
                         {ISILON_ERR_CODE_HDFS_MKDIRS_FAIL,
                          "Error creating path: %s"
                          ISILON_ERR_NUM( ISILON_ERR_MKDIRS_FAIL)},
                         {ISILON_ERR_CODE_HDFS_DIR_LIST_FAIL,
                          "Error acquiring directory listing: %s"
                          ISILON_ERR_NUM( ISILON_ERR_DIR_LIST_FAIL)},
                         {ISILON_ERR_CODE_HDFS_GET_FILE_INFO_FAIL,
                          "Error getting stat info: %s"
                          ISILON_ERR_NUM( ISILON_ERR_GET_FILE_INFO_FAIL)},
                         {ISILON_ERR_CODE_HDFS_ADD_BLOCK_FAIL,
                          "Error adding HDFS block: %s"
                          ISILON_ERR_NUM( ISILON_ERR_ADD_BLOCK_FAIL)},
                         {ISILON_ERR_CODE_SETTING_LAST_BLOCK_FAIL,
                          "Last block is already set or file mode "
                          "differs from UNKNOWN"
                          ISILON_ERR_NUM( ISILON_ERR_SETTING_LAST_BLOCK_FAIL)},
                         {SYS_COPY_LEN_ERR,
                          "Copied size %lld does not match source size %lld of %s"
                          ISILON_ERR_NUM( ISILON_ERR_SYNC_STAGE_INV_LEN)},
                         {UNIX_FILE_OPEN_ERR,
                          "Open error for _src_file name \"%s\", errno = %d"
                          ISILON_ERR_NUM( ISILON_ERR_LOCAL_FILE_OPEN)},
                         {UNIX_FILE_STAT_ERR, 
                          "Stat of \"%s\" error, errno = %d"
                          ISILON_ERR_NUM( ISILON_ERR_LOCAL_FILE_STAT)},
                         {ISILON_ERR_CODE_REGULAR_FILE_EXPECTED,
                          "\"%s\" is not a regular file"
                          ISILON_ERR_NUM( ISILON_ERR_REGULAR_FILE_EXPECTED)}};

#ifdef ISILON_DEBUG
/**
 * Check that error descriptions appear in correct order
 */
bool isilonCheckErrTable()
{
    ISILON_LOG( "\tChecking error table...");

    for ( int i = 0; i < ISILON_ERR_ERROR_TYPES_NUM; i++ )
    {
        if ( isilonErrMesgList[i].err_num != i )
        {
            ISILON_LOG( "\t\tMessage with number %d is incorrect", i);

            return false;
        }
    }

    ISILON_LOG( "\t\tThe table is in consistent state");

    return true;
}
#endif

typedef enum isilonFileMode
{
    ISILON_MODE_READ,
    ISILON_MODE_WRITE,
    ISILON_MODE_UNKNOWN,
    ISILON_MODE_ERROR
} isilonFileMode;

/* Base class for File System objects */
typedef class isilonObjectDesc
{
    protected:
        /* Offset iside object */
        long long offset;
        /* Path to object */
        std::string path;

    public:
        isilonObjectDesc( const char *path)
        {
            (this->path).assign( path);
            /* During creation of a file descriptor an offset
               can be only zero */
            this->offset = 0;
        }

        virtual ~isilonObjectDesc() {}

        long long getOffset() { return offset; }
        void setOffset( long long new_offset) { offset = new_offset; }
        std::string getPath() { return path; }
} isilonObjectDesc;

/* Class representing a file */
typedef class isilonFileDesc : public isilonObjectDesc
{
    private:
        /* Mode in which a file was opened */
        isilonFileMode mode;
        /* Read or write buffer. The type of the buffer depends on 
           'mode' in which a file is opened. Memory for the buffer should
           be allocated on first demand. We don't allocate it on object creation */
        char *buff;
        /* Buffer size. For files opened in READ mode it represents a
           read buffer size. For files open in WRITE mode it represents
           a write buffer size */
        unsigned long buff_size;
        unsigned long buff_offset;
        long long file_size;
        /* Last block that needs to be filled, when a file is opened
           in append mode */
        struct hdfs_object *last_block;

    public:
        isilonFileDesc( isilonFileMode mode, const char *path,
                        unsigned long buff_size, struct hdfs_object *last_block) :
            isilonObjectDesc( path), buff( 0), buff_offset( 0), file_size( 0)
        {
            this->mode = mode;
            this->buff_size = buff_size;
            this->last_block = last_block;
        }

        ~isilonFileDesc( )
        {
            if ( buff )
            {
                free( buff);
            }

            if ( last_block )
            {
                hdfs_object_free( last_block);
            }
        }

        isilonFileMode getMode() { return mode; }

#ifdef ISILON_DEBUG
        irods::error
#else
        void
#endif
        setMode( isilonFileMode mode)
        {
#ifdef ISILON_DEBUG
            irods::error result = SUCCESS();

            /* Changing mode is allowed for files
               opened in UNKNOWN mode only */
            result = ISILON_ASSERT_ERROR( this->mode == ISILON_MODE_UNKNOWN,
                                          ISILON_ERR_UNEXPECTED_MODE,
                                          this->mode, ISILON_MODE_UNKNOWN);
            ISILON_ERROR_CHECK( result);
#endif
            this->mode = mode;

#ifdef ISILON_DEBUG
            return result;
#endif
        }

        unsigned long getBuffSize()
        {
            if ( last_block )
            {
                return buff_size - last_block->ob_val._located_block._len;
            } else
            {
                return buff_size;
            }
        }

        unsigned long getBuffOffset() { return buff_offset; }
        long long getFileSize() { return file_size; }
        void setFileSize( long long file_size) { this->file_size = file_size; }
        struct hdfs_object *getLastBlock() { return last_block; }
        void seekBuff( int offset) { buff_offset = offset; }

#ifdef ISILON_DEBUG
        irods::error
#else
        void
#endif
        setLastBlock( struct hdfs_object *last_block)
        {
#ifdef ISILON_DEBUG
            irods::error result = SUCCESS();

            result = ISILON_ASSERT_ERROR( !this->last_block
                                          && (this->mode == ISILON_MODE_UNKNOWN),
                                          ISILON_ERR_SETTING_LAST_BLOCK_FAIL);
            ISILON_ERROR_CHECK( result);
#endif
            this->last_block = last_block;
            
#ifdef ISILON_DEBUG
            return result;
#endif
        }

        void releaseLastBlock()
        {
            if ( last_block )
            {
                hdfs_object_free( last_block);
            }

            last_block = 0;
        }

        irods::error flushBuff( const char **output_buff)
        {
            irods::error result = SUCCESS();

            if ( mode == ISILON_MODE_READ )
            {
                buff_offset = buff_size;
            } else
            {
                result = ISILON_ASSERT_ERROR( mode == ISILON_MODE_WRITE,
                                              ISILON_ERR_MODE_NOT_SUPPORTED);

                if ( !result.ok() )
                {
                    return result;
                }

                buff_offset = 0;
            }

            *output_buff = buff;

            return result;
        }

        irods::error writeToBuff( const char *buf, unsigned long offset,
                                  unsigned long len)
        {
            irods::error result = SUCCESS();

            result = ISILON_ASSERT_ERROR( mode == ISILON_MODE_WRITE,
                                          ISILON_ERR_FILE_NOT_OPEN_FOR_WRITE);
            ISILON_ERROR_CHECK( result);

            /* Allocate buffer memory if not done yet */
            if ( !buff )
            {
                buff = (char *)malloc( buff_size);
                result = ISILON_ASSERT_ERROR( buff, ISILON_ERR_NO_MEM);
                ISILON_ERROR_CHECK( result);
            }

            result = ISILON_ASSERT_ERROR( buff_offset + len <= buff_size,
                                          ISILON_ERR_NO_ENOUGH_BUFF_SPACE,
                                          buff_size - buff_offset, len);
            ISILON_ERROR_CHECK( result);

            memcpy( buff + buff_offset, buf + offset, len);
            buff_offset += len;

            return result;
        }

        irods::error readFromBuff( char *buf, unsigned long offset,
                                   unsigned long len)
        {       
            irods::error result = SUCCESS();

            result = ISILON_ASSERT_ERROR( mode == ISILON_MODE_READ,
                                          ISILON_ERR_FILE_NOT_OPEN_FOR_READ);
            ISILON_ERROR_CHECK( result);

            /* Allocate buffer memory if not done yet */
            if ( !buff )
            {
                buff = (char *)malloc( buff_size);
                result = ISILON_ASSERT_ERROR( buff, ISILON_ERR_NO_MEM);
                ISILON_ERROR_CHECK( result);
            }

            result = ISILON_ASSERT_ERROR( len <= buff_offset,
                                          ISILON_ERR_NO_ENOUGH_BUFF_DATA,
                                          buff_offset, len);
            ISILON_ERROR_CHECK( result);

            memcpy( buf + offset, buff + buff_size - buff_offset, len);
            buff_offset -= len;

            return result; 
        }
} isilonFileDesc;

/**
 * Class representing a directory
 *
 * "offset" attribute keeps a number of next object to be
 * traversed inside the directory
 */
typedef class isilonDirDesc : public isilonObjectDesc
{
    private:
        hdfs_object *dir_list;

    public:
        isilonDirDesc( const char *path, hdfs_object *dir_list) :
            isilonObjectDesc( path), dir_list( 0)
        {
            this->dir_list = dir_list;
        }

        ~isilonDirDesc()
        {
            hdfs_object_free( dir_list);
        }

        /* No setter counter-part is available. Directory list
           can be set by constructor only */
        hdfs_object *getDirList() { return dir_list; } 
} isilonDirDesc;

/**
 * Connection descriptor
 */
typedef class isilonConnectionDesc
{
    private:
        long port;
        std::string host;
        std::string user;
        struct hdfs_namenode *name_node;
        unsigned long buff_size;

    public:
        isilonConnectionDesc( std::string host,
                              long port,
                              std::string user,
                              struct hdfs_namenode *name_node,
                              unsigned long buff_size)
        {
            this->host = host;
            this->port = port;
            this->user = user;
            this->name_node = name_node;
            this->buff_size = buff_size;
        }

        ~isilonConnectionDesc()
        {
            hdfs_namenode_delete( name_node);
        }

        /* Getters only. No properties can be changed for
           already created connection */
        const std::string& getHost( ) { return host; }
        long getPort( ) { return port; }
        const std::string& getUser( ) { return user; }
        struct hdfs_namenode *getNameNode( ) { return name_node; }
        int getBuffSize() { return buff_size; }
} isilonConnectionDesc;

#endif // _LIBIRODS_ISILON_H_

// -------------------------------------------------------------------
//
// innostore: Simple Erlang API to Embedded Inno DB
//
// Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
//
// innostore is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
//
// innostore is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with innostore.  If not, see <http://www.gnu.org/licenses/>.
//
// -------------------------------------------------------------------

#include "innostore_drv.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <stdarg.h>
#include <termios.h>

/**
 * Erlang driver functions
 */
static int innostore_drv_init();

static ErlDrvData innostore_drv_start(ErlDrvPort port, char* buffer);

static void innostore_drv_stop(ErlDrvData handle);

static void innostore_drv_finish();

static int innostore_drv_control(ErlDrvData handle, unsigned int cmd,
                              char* inbuf, int inbuf_sz,
                              char** outbuf, int outbuf_sz);

/**
 * Erlang Driver Entry
 */
ErlDrvEntry innostore_drv_entry =
{
    innostore_drv_init,         /* F_PTR init, called when library loaded */
    innostore_drv_start,        /* L_PTR start, called when port is opened */
    innostore_drv_stop,		/* F_PTR stop, called when port is closed */
    NULL,			/* F_PTR output, called when erlang has sent */
    NULL,                       /* F_PTR ready_input, called when input descriptor ready */
    NULL,			/* F_PTR ready_output, called when output descriptor ready */
    "innostore_drv",            /* driver_name */
    innostore_drv_finish,       /* F_PTR finish, called when unloaded */
    NULL,			/* handle */
    innostore_drv_control,      /* F_PTR control, port_command callback */
    NULL,			/* F_PTR timeout, reserved */
    NULL,                       /* F_PTR outputv, reserved */
    NULL,                       /* F_PTR ready_async */
    NULL,                       /* F_PTR flush */
    NULL,                       /* F_PTR call */
    NULL,                       /* F_PTR event */
    ERL_DRV_EXTENDED_MARKER,
    ERL_DRV_EXTENDED_MAJOR_VERSION,
    ERL_DRV_EXTENDED_MINOR_VERSION,
    ERL_DRV_FLAG_USE_PORT_LOCKING,
    NULL,                        /* Reserved */
    NULL                         /* F_PTR process_exit */
};


/**
 * Core function prototypes
 */
static void* innostore_worker(void* arg);

static void do_set_cfg(void* arg);
static void do_start(void* arg);
static void do_init_table(void* arg);
static void do_get(void* arg);
static void do_put(void* arg);
static void do_delete(void* arg);

static void do_list_tables(void* arg);
static int  do_list_tables_cb(void* arg, const char* tablename, int tablename_sz);

static void do_cursor_open(void* arg);
static void do_cursor_move(void* arg);
static void do_cursor_read(unsigned int content_flag, PortState* state);
static void do_cursor_close(void* arg);

static void do_drop_table(void* arg);

static void send_ok(PortState* state);
static void send_ok_atom(PortState* state, const char* atom);
static void send_error_atom(PortState* state, const char* atom);
static void send_error_str(PortState* state, const char* str);

/**
 * Compression layer functions and defines
 */
#define COMPRESSION_NONE 0

static int compress(unsigned int cflag,
                    PortState* state, char* in_value, unsigned int in_value_sz,
                    char** out_value, unsigned int* out_value_sz);

static int decompress(PortState* state, char* in_value, unsigned int in_value_sz,
                      char** out_value, unsigned int* out_value_sz);

/**
 * Logging
 */
#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) || defined(__GNUC__)
#  define log(...) G_LOGGER_FN(G_LOGGER_FH, __VA_ARGS__)
#else
#  define log(X) G_LOGGER_FN(G_LOGGER_FH, X)
#endif

static int set_log_file(const char* filename);
static int raw_logger(ib_msg_stream_t stream, const char* fmt, ...);


/**
 * Globals for inno mgmt -- need to ensure engine only gets started/configured once per VM.
 */
#define ENGINE_STOPPED  0
#define ENGINE_STARTING 1
#define ENGINE_STARTED  2

static int          G_ENGINE_STATE = 0;
static ErlDrvMutex* G_ENGINE_STATE_LOCK;

/**
 * Globals for inno logging */

static ErlDrvMutex* G_LOGGER_LOCK;
static char*        G_LOGGER_BUF = NULL;
static size_t       G_LOGGER_SIZE = 0;
ib_msg_log_t        G_LOGGER_FN = (ib_msg_log_t) raw_logger;
static FILE*        G_LOGGER_FH = NULL;

/**
 * Column IDs
 */
#define KEY_COL   0
#define VALUE_COL 1

/**
 * Helper macros for txn and failure management
 */
#define FAIL_ON_ERROR(fn, block)                        \
    if (fn != DB_SUCCESS)                               \
        block

#define ROLLBACK_RETURN(txn) {                          \
    if (ib_trx_state(txn) != IB_TRX_NOT_STARTED)        \
        ib_trx_rollback(txn);                           \
    else                                                \
        ib_trx_release(txn);                            \
    return; }

#define DRIVER_FREE_IF_NE(ptr, origin, origin_sz) {     \
        if (ptr < origin || ptr > (origin + origin_sz)) \
            driver_free(ptr); }



DRIVER_INIT(innostore_drv)
{
    return &innostore_drv_entry;
}

static int innostore_drv_init()
{
    char log_filename[_POSIX_PATH_MAX];
    size_t log_filename_size = sizeof(log_filename);
    ErlDrvSysInfo sys_info;

    G_ENGINE_STATE_LOCK = erl_drv_mutex_create("innostore_state_lock");
    G_LOGGER_LOCK = erl_drv_mutex_create("innostore_logger_lock");

    // Check if this is beam.smp - cannot run under beam 
    // due to restrictions with driver_send_term
    driver_system_info(&sys_info, sizeof(sys_info));
    if (sys_info.smp_support == 0)
    {
        log("Innostore only supports the SMP runtime, add -smp enable");
        return -1;
    }

    // Initialize Inno's memory subsystem
    if (ib_init() != DB_SUCCESS)
    {
        return -1;
    }

    // Set up the logger
    if (erl_drv_getenv("INNOSTORE_LOG", log_filename, &log_filename_size) == 0)
    {
        set_log_file(log_filename);
    }
    else
    {
        set_log_file(NULL);
    }

    return 0;
}

static void innostore_drv_finish()
{
    erl_drv_mutex_destroy(G_ENGINE_STATE_LOCK);

    // Shutdown the engine, if it's running -- note that this blocks the
    // the calling VM thread and may be a long running operation.
    if (G_ENGINE_STATE == ENGINE_STARTED)
    {
        ib_err_t result = ib_shutdown(IB_SHUTDOWN_NORMAL);
        if (result != DB_SUCCESS)
        {
            log("ib_shutdown failed: %s\n", ib_strerror(result));
        }
    }

    // Clean up logging after inno has shutdown completely
    erl_drv_mutex_destroy(G_LOGGER_LOCK); 
    if (G_LOGGER_BUF != NULL)
    {
        driver_free(G_LOGGER_BUF);
        G_LOGGER_BUF = NULL;
        G_LOGGER_SIZE = 0;
    }
    if (G_LOGGER_FH != NULL)
    {
        fclose(G_LOGGER_FH);
        G_LOGGER_FH = NULL;
        G_LOGGER_FN = raw_logger;
    }

    G_ENGINE_STATE = ENGINE_STOPPED;
}

static ErlDrvData innostore_drv_start(ErlDrvPort port, char* buffer)
{
    PortState* state = (PortState*)driver_alloc(sizeof(PortState));

    memset(state, '\0', sizeof(PortState));

    // Save handle to the port
    state->port = port;

    // Save the owner PID
    state->port_owner = driver_connected(port);

    // Initialize in the READY state
    state->port_state = STATE_READY;

    // Allocate a mutex and condition variable for the worker
    state->worker_lock = erl_drv_mutex_create("innostore_worker_lock");
    state->worker_cv   = erl_drv_cond_create("innostore_worker_cv");

    // Make sure port is running in binary mode
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    // Spin up the worker
    erl_drv_thread_create("innostore_worker", &(state->worker),
                          &innostore_worker, state, 0);

    return (ErlDrvData)state;
}

static void innostore_drv_stop(ErlDrvData handle)
{
    PortState* state = (PortState*)handle;

    // Grab the worker lock, in case we have an job running
    erl_drv_mutex_lock(state->worker_lock);

    // Signal the shutdown and wait until the current operation has completed
    state->shutdown_flag = 1;
    erl_drv_cond_signal(state->worker_cv);

    while (state->op)
    {
        erl_drv_cond_wait(state->worker_cv, state->worker_lock);
    }

    // If the port state is not marked as READY, close the cursor and abort the txn
    if (state->port_state != STATE_READY)
    {
        ib_cursor_close(state->cursor);
        ib_trx_rollback(state->txn);
    }

    // No pending jobs and we have the lock again -- join our worker thread
    erl_drv_cond_signal(state->worker_cv);
    erl_drv_mutex_unlock(state->worker_lock);
    erl_drv_thread_join(state->worker, 0);

    // Cleanup
    erl_drv_cond_destroy(state->worker_cv);
    erl_drv_mutex_destroy(state->worker_lock);
    driver_free(handle);
}


#define is_cmd(bitmask) ((cmd & (bitmask)) == cmd)

static int innostore_drv_control(ErlDrvData handle, unsigned int cmd,
                              char* inbuf, int inbuf_sz,
                              char** outbuf, int outbuf_sz)
{
    PortState* state = (PortState*)handle;

    // Grab the worker lock.
    erl_drv_mutex_lock(state->worker_lock);

    assert(state->op == 0);

    // Verify that caller is not attempting cursor operation when no cursor is
    // active (or vice-versa)
    if (state->port_state == STATE_READY && is_cmd(CMD_CURSOR_OPS))
    {
        erl_drv_mutex_unlock(state->worker_lock);
        send_error_atom(state, "cursor_not_open");
        return 0;
    }
    else if (state->port_state == STATE_CURSOR && !is_cmd(CMD_CURSOR_OPS))
    {
        erl_drv_mutex_unlock(state->worker_lock);
        send_error_atom(state, "cursor_is_open");
        return 0;
    }

    // Copy inbuf into our work buffer
    if (inbuf_sz > 0)
    {
        state->work_buffer = driver_alloc(inbuf_sz);
        memcpy(state->work_buffer, inbuf, inbuf_sz);
    }

    // Select the appropriate async op
    switch (cmd)
    {
    case CMD_SET_CFG:
        state->op = &do_set_cfg;
        break;

    case CMD_START:
        state->op = &do_start;
        break;

    case CMD_INIT_TABLE:
        state->op = &do_init_table;
        break;

    case CMD_IS_STARTED:
        // Check the global state -- prior to doing that, release our worker lock
        // so as to avoid weird locking overlaps. Store the true/false value as
        // a single byte in outbuf. No need to do any allocation as the VM always
        // provides a 64-byte outbuf buffer by default.
        assert(outbuf_sz > 0);
        erl_drv_mutex_unlock(state->worker_lock);
        erl_drv_mutex_lock(G_ENGINE_STATE_LOCK);
        (*outbuf)[0] = (G_ENGINE_STATE == ENGINE_STARTED);
        erl_drv_mutex_unlock(G_ENGINE_STATE_LOCK);
        return 1;

    case CMD_GET:
        state->op = &do_get;
        break;

    case CMD_PUT:
        state->op = &do_put;
        break;

    case CMD_DELETE:
        state->op = &do_delete;
        break;

    case CMD_LIST_TABLES:
        state->op = &do_list_tables;
        break;

    case CMD_CURSOR_OPEN:
        state->op = &do_cursor_open;
        break;

    case CMD_CURSOR_MOVE:
        state->op = &do_cursor_move;
        break;

    case CMD_CURSOR_CLOSE:
        state->op = &do_cursor_close;
        break;

    case CMD_DROP_TABLE:
        state->op = &do_drop_table;
        break;
    }

    // Signal the worker
    erl_drv_cond_signal(state->worker_cv);
    erl_drv_mutex_unlock(state->worker_lock);
    *outbuf = 0;
    return 0;
}


static void* innostore_worker(void* arg)
{
    PortState* state = (PortState*)arg;
    erl_drv_mutex_lock(state->worker_lock);
    while (1)
    {
        //
        // NOTE: Holds the worker lock for the duration of the loop !!
        //
        if (state->shutdown_flag)
        {
            driver_free(state->work_buffer);
            state->work_buffer = 0;
            erl_drv_cond_signal(state->worker_cv);
            erl_drv_mutex_unlock(state->worker_lock);
            break;
        }

        if (state->op)
        {
            state->op(state);
            state->op = 0;
            driver_free(state->work_buffer);
            state->work_buffer = 0;
        }
        else
        {
            erl_drv_cond_wait(state->worker_cv, state->worker_lock);
        }
    }
    return 0;
}

static void do_set_cfg(void* arg)
{
    PortState* state = (PortState*)arg;

    erl_drv_mutex_lock(G_ENGINE_STATE_LOCK);
    if (G_ENGINE_STATE == ENGINE_STOPPED)
    {
        char* key   = UNPACK_STRING(state->work_buffer, 0);
        const char* value = UNPACK_STRING(state->work_buffer, strlen(key)+1);

        if (strcmp(key, "error_log") == 0)
        {
            if (set_log_file(value) == 0)
            {
                send_ok(state);
            }
            else
            {
                send_error_atom(state, "einval");
            }
        }
        else
        {
            // Check the expected type of the provided key so as to 1. validate it's a good key
            // and 2. know what setter to use.
            ib_cfg_type_t key_type;
            ib_err_t error = ib_cfg_var_get_type(key, &key_type);
            if (error == DB_SUCCESS)
            {
                if (key_type == IB_CFG_TEXT)
                {
                    // HACK: Semantics of setting a text configuration value for innodb changed
                    // to be pointer assignment (from copy) for vsn 1.0.6.6750. So, we strdup the
                    // value to ensure everything works as expected.
                    // TODO: Setup some sort of list of strdup'd values to ensure they all get
                    // cleaned up properly. In typical usage, this isn't going to be a problem
                    // as you only initialize once per run, but it bothers me just the same.
                    error = ib_cfg_set(key, strdup(value));
                }
                else
                {
                    ErlDrvUInt value_i;
                    UNPACK_INT(state->work_buffer, strlen(key)+1, &value_i);
                    error = ib_cfg_set(key, value_i);
                }

            }

            if (error == DB_SUCCESS)
            {
                send_ok(state);
            }
            else
            {
                send_error_str(state, ib_strerror(error));
            }
        }
    }
    else
    {
        send_error_atom(state, "starting");
    }

    erl_drv_mutex_unlock(G_ENGINE_STATE_LOCK);
}

static void do_start(void* arg)
{
    PortState* state = (PortState*)arg;

    erl_drv_mutex_lock(G_ENGINE_STATE_LOCK);
    if (G_ENGINE_STATE == ENGINE_STOPPED)
    {
        // Engine was stopped; set the flag, unlock and run the start
        G_ENGINE_STATE = ENGINE_STARTING;
        erl_drv_mutex_unlock(G_ENGINE_STATE_LOCK);

        // Run the startup without holding any lock -- this can take a while
        // if we are recovering from previous errors
        ib_err_t error = ib_startup("barracuda");

        // Relock and sort out results
        erl_drv_mutex_lock(G_ENGINE_STATE_LOCK);
        if (error == DB_SUCCESS)
        {
            G_ENGINE_STATE = ENGINE_STARTED;
            send_ok(state);
        }
        else
        {
            G_ENGINE_STATE = ENGINE_STOPPED;
            send_error_str(state, ib_strerror(error));
        }
    }
    else if (G_ENGINE_STATE == ENGINE_STARTED)
    {
        // another thread has already completed startup
        send_ok(state);
    }
    else
    {
        // Engine was not in stopped state when do_start was called.
        // Probably due to multiple threads trying to start at the
        // same time.
        assert(G_ENGINE_STATE == ENGINE_STARTING);
        send_error_atom(state, "starting");        
    }
        
    erl_drv_mutex_unlock(G_ENGINE_STATE_LOCK);
}

static void do_init_table(void* arg)
{
    PortState* state = (PortState*)arg;

    // Unpack the table name (pre-formatted to be Database/TableName)
    char* table = UNPACK_STRING(state->work_buffer, 0);
    ib_id_t table_id;

    // If the table doesn't exist, create it
    if (ib_table_get_id(table, &table_id) != DB_SUCCESS)
    {
        // Start a txn for schema access and be sure to make it serializable
        ib_trx_t txn = ib_trx_begin(IB_TRX_SERIALIZABLE);
        ib_schema_lock_exclusive(txn);

        // Make sure the innokeystore database exists
        // TODO: Avoid hard-coding the db name here...
        ib_database_create("innokeystore");

        // Create the table schema
        ib_tbl_sch_t schema;
        ib_table_schema_create(table, &schema, IB_TBL_COMPACT, 0);
        ib_table_schema_add_col(schema, "key", IB_VARBINARY, IB_COL_NONE, 0, 255);
        ib_table_schema_add_col(schema, "value", IB_BLOB, IB_COL_NONE, 0, 0);

        // Create primary index on key
        ib_idx_sch_t index;
        ib_table_schema_add_index(schema, "PRIMARY_KEY", &index);
        ib_index_schema_add_col(index, "key", 0);
        ib_index_schema_set_clustered(index);

        // Create the actual table
        ib_err_t rc = ib_table_create(txn, schema, &table_id);

        // Release the schema -- doesn't matter if table was created or not at this point
        ib_schema_unlock(txn);

        if (rc == DB_SUCCESS)
        {
            // Commit changes to schema (if any)
            ib_trx_commit(txn);
        }
        else
        {
            // Failed to create table -- rollback and exit
            ib_trx_rollback(txn);
            send_error_str(state, ib_strerror(rc));
            return;
        }
    }

    // Guaranteed at this point to have a valid table_id
    ErlDrvTermData response[] = { ERL_DRV_ATOM,   driver_mk_atom("innostore_ok"),
                                  ERL_DRV_BUF2BINARY, (ErlDrvTermData)&table_id,
                                                      (ErlDrvUInt)sizeof(table_id),
                                  ERL_DRV_TUPLE, 2};
    driver_send_term(state->port, state->port_owner, response,
                     sizeof(response) / sizeof(response[0]));
}

static void do_get(void* arg)
{
    PortState* state = (PortState*)arg;

    // Unpack key from work buffer
    // table - 8 bytes
    // keysz - 1 byte
    // key   - variable
    ib_id_t table      ; UNPACK_INT(state->work_buffer, 0, &table);
    unsigned char keysz = UNPACK_BYTE(state->work_buffer, sizeof(table));
    char* key          = UNPACK_BLOB(state->work_buffer, sizeof(table)+1);

    ib_trx_t txn = ib_trx_begin(IB_TRX_REPEATABLE_READ);

    ib_crsr_t cursor;
    FAIL_ON_ERROR(ib_cursor_open_table_using_id(table, txn, &cursor),
                  ROLLBACK_RETURN(txn));

    ib_tpl_t key_tuple = ib_clust_search_tuple_create(cursor);
    ib_col_set_value(key_tuple, KEY_COL, key, keysz);

    int searchloc;
    ib_err_t error = ib_cursor_moveto(cursor, key_tuple, IB_CUR_GE, &searchloc);

    // Drop the key tuple -- cursor is at desired location
    ib_tuple_delete(key_tuple);

    // If we encountered an error, bail
    if (error != DB_SUCCESS && error != DB_RECORD_NOT_FOUND && error != DB_END_OF_INDEX)
    {
        ib_cursor_close(cursor);
        send_error_str(state, ib_strerror(error));
        ROLLBACK_RETURN(txn);
    }

    // Found it, read the value and send back to caller
    if (searchloc == 0)
    {
        ib_tpl_t tuple = ib_clust_read_tuple_create(cursor);

        // TODO: May need better error handling here
        FAIL_ON_ERROR(ib_cursor_read_row(cursor, tuple),
                      {
                          ib_tuple_delete(tuple);
                          ib_cursor_close(cursor);
                          ROLLBACK_RETURN(txn);
                      });

        // Get the size of the value
        ib_col_meta_t value_meta;
        int   raw_value_sz = ib_col_get_meta(tuple, VALUE_COL, &value_meta);
        char* raw_value    = (char*)ib_col_get_value(tuple, VALUE_COL);

        unsigned int value_sz = 0;
        char* value = 0;

        // Decompress the raw value as necessary. If it fails, that layer
        // generates an error -- we just need to cleanup
        if (decompress(state, raw_value, raw_value_sz, &value, &value_sz))
        {
            ib_tuple_delete(tuple);
            ib_cursor_close(cursor);
            ROLLBACK_RETURN(txn);
        }

        // Send back the response
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("innostore_ok"),
                                      ERL_DRV_BUF2BINARY, (ErlDrvTermData)value,
                                                          (ErlDrvUInt)value_sz,
                                      ERL_DRV_TUPLE, 2};
        driver_send_term(state->port, state->port_owner,
                         response, sizeof(response) / sizeof(response[0]));

        // Cleanup
        DRIVER_FREE_IF_NE(value, raw_value, raw_value_sz);
        ib_tuple_delete(tuple);
    }
    else
    {
        // Not found
        send_ok_atom(state, "not_found");
    }

    ib_cursor_close(cursor);
    ib_trx_commit(txn);
}

static void do_put(void* arg)
{
    PortState* state = (PortState*)arg;

    // Unpack key/value from work buffer:
    // table   - 8 bytes
    // cflag   - 1 byte (compression flag)
    // keysz   - 1 byte
    // key     - variable
    // valuesz - 4 bytes
    // value   - variable
    ib_id_t table            ; UNPACK_INT(state->work_buffer, 0, &table);
    unsigned char cflag      = UNPACK_BYTE(state->work_buffer, sizeof(table));
    unsigned char keysz      = UNPACK_BYTE(state->work_buffer, sizeof(table) + 1);
    char* key                = UNPACK_BLOB(state->work_buffer, sizeof(table) + 2);
    unsigned int raw_valuesz;  UNPACK_INT(state->work_buffer,  sizeof(table) + 2 + keysz, &raw_valuesz);
    char* raw_value          = UNPACK_BLOB(state->work_buffer, sizeof(table) + 2 + keysz + 4);

    unsigned int valuesz;
    char*        value;

    // Compress the data as requested -- returns non-zero if compression failed. Also,
    // the underlying compression mechanism/function is responsible for sending an error
    // message in that situation.
    if (compress(cflag, state, raw_value, raw_valuesz, &value, &valuesz))
    {
        return;
    }

    ib_trx_t txn = ib_trx_begin(IB_TRX_SERIALIZABLE);

    ib_crsr_t cursor;
    FAIL_ON_ERROR(ib_cursor_open_table_using_id(table, txn, &cursor),
                  {
                      DRIVER_FREE_IF_NE(value, raw_value, raw_valuesz);
                      ROLLBACK_RETURN(txn);
                  });

    // Lock the cursor for exclusive access to our row
    FAIL_ON_ERROR(ib_cursor_set_lock_mode(cursor, IB_LOCK_X),
                  {
                      ib_cursor_close(cursor);
                      DRIVER_FREE_IF_NE(value, raw_value, raw_valuesz);
                      ROLLBACK_RETURN(txn);
                  });

    // Setup the search tuple and copy our key into it -- then move to the desired
    // location
    int searchloc;
    ib_tpl_t key_tuple = ib_clust_search_tuple_create(cursor);
    ib_col_set_value(key_tuple, KEY_COL, key, keysz);
    ib_err_t error = ib_cursor_moveto(cursor, key_tuple, IB_CUR_GE, &searchloc);

    // Drop the key tuple -- no longer important
    ib_tuple_delete(key_tuple);

    // If we didn't find it or reach the end of the index looking for it, fail
    if (error != DB_SUCCESS && error != DB_END_OF_INDEX)
    {
        ib_cursor_close(cursor);
        send_error_str(state, ib_strerror(error));
        DRIVER_FREE_IF_NE(value, raw_value, raw_valuesz);
        ROLLBACK_RETURN(txn);
    }

    // Exact match, update it
    if (searchloc == 0)
    {
        ib_tpl_t old = ib_clust_read_tuple_create(cursor);
        ib_tpl_t new = ib_clust_read_tuple_create(cursor);

        FAIL_ON_ERROR(ib_cursor_read_row(cursor, old),
                      {
                          ib_tuple_delete(old);
                          ib_tuple_delete(new);
                          ib_cursor_close(cursor);
                          DRIVER_FREE_IF_NE(value, raw_value, raw_valuesz);
                          ROLLBACK_RETURN(txn);
                      });

        ib_tuple_copy(new, old);
        ib_col_set_value(new, VALUE_COL, value, valuesz);

        FAIL_ON_ERROR(ib_cursor_update_row(cursor, old, new),
                      {
                          ib_tuple_delete(old);
                          ib_tuple_delete(new);
                          ib_cursor_close(cursor);
                          DRIVER_FREE_IF_NE(value, raw_value, raw_valuesz);
                          ROLLBACK_RETURN(txn);
                      });

        ib_tuple_delete(old);
        ib_tuple_delete(new);
    }
    else
    {
        // No match -- insert a new row
        ib_tpl_t new = ib_clust_read_tuple_create(cursor);
        ib_col_set_value(new, KEY_COL, key, keysz);
        ib_col_set_value(new, VALUE_COL, value, valuesz);

        FAIL_ON_ERROR(ib_cursor_insert_row(cursor, new),
                      {
                          ib_tuple_delete(new);
                          ib_cursor_close(cursor);
                          DRIVER_FREE_IF_NE(value, raw_value, raw_valuesz);
                          ROLLBACK_RETURN(txn);
                      });

        ib_tuple_delete(new);
    }

    // All done -- cleanup cursor and txn
    ib_cursor_close(cursor);
    DRIVER_FREE_IF_NE(value, raw_value, raw_valuesz);
    ib_trx_commit(txn);
    send_ok(state);
}

static void do_delete(void* arg)
{
    PortState* state = (PortState*)arg;

    // Unpack key/value from work buffer:
    // table   - 8 bytes
    // keysz   - 1 byte
    // key     - variable
    ib_id_t table        ; UNPACK_INT(state->work_buffer, 0, &table);
    unsigned char keysz  = UNPACK_BYTE(state->work_buffer, sizeof(table));
    char* key            = UNPACK_BLOB(state->work_buffer, sizeof(table) + 1);

    ib_trx_t txn = ib_trx_begin(IB_TRX_SERIALIZABLE);

    ib_crsr_t cursor;
    FAIL_ON_ERROR(ib_cursor_open_table_using_id(table, txn, &cursor),
                  ROLLBACK_RETURN(txn));

    // Lock the cursor for exclusive access to our row
    FAIL_ON_ERROR(ib_cursor_set_lock_mode(cursor, IB_LOCK_X),
                  {
                      ib_cursor_close(cursor);
                      ROLLBACK_RETURN(txn);
                  });

    // Setup the search tuple and copy our key into it -- then move to the desired
    // location
    int searchloc;
    ib_tpl_t key_tuple = ib_clust_search_tuple_create(cursor);
    ib_col_set_value(key_tuple, KEY_COL, key, keysz);
    ib_err_t error = ib_cursor_moveto(cursor, key_tuple, IB_CUR_GE, &searchloc);

    // Drop the key tuple -- no longer important
    ib_tuple_delete(key_tuple);

    // If we encountered an error, bail
    if (error != DB_SUCCESS && error != DB_END_OF_INDEX)
    {
        ib_cursor_close(cursor);
        send_error_str(state, ib_strerror(error));
        ROLLBACK_RETURN(txn);
    }

    // Exact match, delete it. If it wasn't found noop.
    if (searchloc == 0)
    {
        FAIL_ON_ERROR(ib_cursor_delete_row(cursor),
                      {
                          ib_cursor_close(cursor);
                          ROLLBACK_RETURN(txn);
                      });
    }

    // All done -- cleanup cursor and txn and notify caller
    ib_cursor_close(cursor);
    ib_trx_commit(txn);
    send_ok(state);
}

static void do_list_tables(void* arg)
{
    PortState* state = (PortState*)arg;

    // Spin up a transaction and lock the schema. Then use the iteration visitor
    // function to stream the list of names back out to the caller.
    ib_trx_t txn = ib_trx_begin(IB_TRX_SERIALIZABLE);

    FAIL_ON_ERROR(ib_schema_lock_exclusive(txn),
                  ROLLBACK_RETURN(txn));

    ib_err_t error = ib_schema_tables_iterate(txn, &do_list_tables_cb, state);
    if (error == DB_SUCCESS)
    {
        send_ok(state);
    }
    else
    {
        send_error_str(state, ib_strerror(error));
    }

    ib_schema_unlock(txn);
    ib_trx_commit(txn);
}

static int do_list_tables_cb(void* arg, const char* tablename, int tablename_sz)
{
    PortState* state = (PortState*)arg;

    // Only send tables that are not prefixed w/ SYS_
    if (strncmp("SYS_", tablename, 4) != 0)
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM,   driver_mk_atom("innostore_table_name"),
                                      ERL_DRV_STRING, (ErlDrvTermData)tablename, (ErlDrvUInt)tablename_sz,
                                      ERL_DRV_TUPLE, 2};
        driver_send_term(state->port, state->port_owner, response,
                         sizeof(response) / sizeof(response[0]));
    }
    return 0;
}

static void do_cursor_open(void* arg)
{
    PortState* state = (PortState*)arg;

    // Unpack the table ID to scan
    ib_id_t table ; UNPACK_INT(state->work_buffer, 0, &table);

    // Start the txn and open a cursor for the table.
    state->txn = ib_trx_begin(IB_TRX_READ_UNCOMMITTED);
    FAIL_ON_ERROR(ib_cursor_open_table_using_id(table, state->txn, &(state->cursor)),
                  ROLLBACK_RETURN(state->txn));

    // Update port state
    state->port_state = STATE_CURSOR;

    // Notify caller
    send_ok(state);
}

static void do_cursor_move(void* arg)
{
    PortState* state = (PortState*)arg;

    // Unpack the type of move, and what data to return (key or key+data)
    // movement - 1 byte
    // content - 1 byte
    unsigned char movement = UNPACK_BYTE(state->work_buffer, 0);
    unsigned char content  = UNPACK_BYTE(state->work_buffer, 1);

    ib_err_t error;
    switch(movement)
    {
    case CURSOR_FIRST: error = ib_cursor_first(state->cursor); break;
    case CURSOR_NEXT : error = ib_cursor_next(state->cursor); break;
    case CURSOR_PREV : error = ib_cursor_prev(state->cursor); break;
    case CURSOR_LAST : error = ib_cursor_last(state->cursor); break;
    }

    if (error == DB_SUCCESS)
    {
        do_cursor_read(content, state);
    }
    else if (error == DB_END_OF_INDEX)
    {
        send_ok_atom(state, "eof");
    }
    else
    {
        send_error_str(state, ib_strerror(error));
        ib_cursor_close(state->cursor);
        ib_trx_t txn = state->txn;
        state->cursor = 0;
        state->txn = 0;
        state->port_state = STATE_READY;
        ROLLBACK_RETURN(txn);
    }
}

static void do_cursor_read(unsigned int content_flag, PortState* state)
{
    ib_tpl_t tuple = ib_clust_read_tuple_create(state->cursor);
    ib_err_t error = ib_cursor_read_row(state->cursor, tuple);
    if (error == DB_SUCCESS)
    {
        // Pull out the key
        ib_col_meta_t key_col_meta;
        int key_sz = ib_col_get_meta(tuple, KEY_COL, &key_col_meta);
        char* key = (char*) ib_col_get_value(tuple, KEY_COL);

        // Only send back the key if the caller isn't interested in value
        if (content_flag == CONTENT_KEY_ONLY)
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("innostore_ok"),
                                          ERL_DRV_BUF2BINARY, (ErlDrvTermData)key, (ErlDrvUInt)key_sz,
                                          ERL_DRV_TUPLE, 2};
            driver_send_term(state->port, state->port_owner, response,
                             sizeof(response) / sizeof(response[0]));
        }
        else
        {
            // Caller wants key and values. Need to pull out the value, decompress it, etc.
            ib_col_meta_t value_col_meta;
            int raw_value_sz = ib_col_get_meta(tuple, VALUE_COL, &value_col_meta);
            char* raw_value = (char*) ib_col_get_value(tuple, VALUE_COL);

            unsigned int value_sz = 0;
            char* value = 0;

            // If decompress fails, that layer generates an error -- we just need to cleanup
            if (decompress(state, raw_value, raw_value_sz, &value, &value_sz))
            {
                ib_tuple_delete(tuple);
                ib_cursor_close(state->cursor);
                ib_trx_t txn = state->txn;
                state->cursor = 0;
                state->txn = 0;
                state->port_state = STATE_READY;
                ROLLBACK_RETURN(txn);
            }

            // Send back the key and value
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("innostore_ok"),
                                          ERL_DRV_BUF2BINARY, (ErlDrvTermData)key, (ErlDrvUInt)key_sz,
                                          ERL_DRV_BUF2BINARY, (ErlDrvTermData)value, (ErlDrvUInt)value_sz,
                                          ERL_DRV_TUPLE, 3};
            driver_send_term(state->port, state->port_owner,
                             response, sizeof(response) / sizeof(response[0]));

            // Cleanup
            DRIVER_FREE_IF_NE(value, raw_value, raw_value_sz);
        }
    }
    else
    {
        // Notify caller and then clean everything up -- tear down cursor, etc.
        send_error_str(state, ib_strerror(error));
        ib_tuple_delete(tuple);
        ib_cursor_close(state->cursor);
        ib_trx_t txn = state->txn;
        state->cursor = 0;
        state->txn = 0;
        state->port_state = STATE_READY;
        ROLLBACK_RETURN(txn);
    }

    // Cleanup the tuple
    ib_tuple_delete(tuple);
}

static void do_cursor_close(void* arg)
{
    PortState* state = (PortState*)arg;

    // Cleanup the cursor and transaction and reset our state
    ib_cursor_close(state->cursor);
    ib_trx_commit(state->txn);
    state->cursor = 0;
    state->txn = 0;
    state->port_state = STATE_READY;

    // Notify caller
    send_ok(state);
}

static void do_drop_table(void* arg)
{
    PortState* state = (PortState*)arg;

    char* table = UNPACK_STRING(state->work_buffer, 0);

    // Use a serializable txn as this is a schema op. Also be sure to lock
    // our schema exclusively, per docs.
    ib_trx_t txn = ib_trx_begin(IB_TRX_SERIALIZABLE);

    ib_err_t rc = ib_schema_lock_exclusive(txn);
    FAIL_ON_ERROR(rc,
                  {
                      send_error_str(state, ib_strerror(rc));
                      ROLLBACK_RETURN(txn);
                  });

    // Do the actual drop
    rc = ib_table_drop(txn, table);

    // Go ahead and unlock the schema -- this has to be done no matter the outcome.
    ib_schema_unlock(txn);

    // If everything went well, commit; otherwise rollback
    if (rc == DB_SUCCESS)
    {
        ib_trx_commit(txn);
        send_ok(state);
    }
    else
    {
        ib_trx_rollback(txn);
        send_error_str(state, ib_strerror(rc));
    }
}


static int compress(unsigned int cflag,
                    PortState* state, char* in_value, unsigned int in_value_sz,
                    char** out_value, unsigned int* out_value_sz)
{
    // Copy the buffer of data into the temporary space so we can prefix it
    // with the compression marker byte
    // TODO: Add support for other compression mechanisms
    *out_value = driver_alloc(in_value_sz + 1);
    memcpy((*out_value) + 1, in_value, in_value_sz);
    (*out_value)[0] = COMPRESSION_NONE;
    *out_value_sz = in_value_sz + 1;
    return 0;
}

static int decompress(PortState* state, char* in_value, unsigned int in_value_sz,
                      char** out_value, unsigned int* out_value_sz)
{
    if (in_value[0] == COMPRESSION_NONE)
    {
        // Skip the compression header (1 byte) and return a pointer into in_value. The
        // cleanup macro DRIVER_FREE_IF_NE deals with this and ensures we don't free
        // the same chunk of memory twice
        *out_value = in_value + 1;
        *out_value_sz = in_value_sz - 1;
        return 0;
    }
    else
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("innostore_error"),
                                      ERL_DRV_ATOM, driver_mk_atom("unknown_compression_method"),
                                      ERL_DRV_INT,  (ErlDrvUInt)in_value[0],
                                      ERL_DRV_TUPLE, 2,
                                      ERL_DRV_TUPLE, 2};
        driver_send_term(state->port, state->port_owner, response,
                         sizeof(response) / sizeof(response[0]));
        return -1;
    }
}

static int set_log_file(const char* filename)
{
    int ret = 0;
    int open_errno = 0;

    erl_drv_mutex_lock(G_LOGGER_LOCK);

    /* Close any open log file */
    if (G_LOGGER_FH != NULL)
    {
        fclose(G_LOGGER_FH);
        G_LOGGER_FH = NULL;
    }

    /* If filename is non-NULL and non-empty, log to that file
    */
    if (filename != NULL && *filename != '\0')
    {
        G_LOGGER_FH = fopen(filename, "a");
        if (G_LOGGER_FH == NULL)
        {
            open_errno = errno;
            ret = -1;
        }
        else
        {
            setvbuf(G_LOGGER_FH, NULL, _IONBF, 0);
        }
    }
    
    /* If no log file given or open failed, log on stderr.  Need to decide if the terminal
    ** is in raw mode or not.  If in raw mode need to output \r\n at the end
    ** of lines instead of just \r
    **/
    if (G_LOGGER_FH == NULL)
    {
        struct termios termios;

        if (tcgetattr(fileno(stderr), &termios) == 0)
        {
            if ((termios.c_oflag & OPOST) == 0)
            {
                G_LOGGER_FN = (ib_msg_log_t) raw_logger;
            }
        }
    }

    /* Finally, let InnoDb know the logging function and file handle */
    ib_logger_set(G_LOGGER_FN, G_LOGGER_FH);

    erl_drv_mutex_unlock(G_LOGGER_LOCK);

    /* If a filename was passed in above but could not be opened, log
    ** the error so it will be formatted nicely on the erlang console
    */
    if (open_errno != 0)
    {
        log("Innostore: Could not open log file \"%s\" - %s\n", 
                  filename, strerror(open_errno));
        log("Innostore: Logging to stderr\n");
    }


    return ret;
}


/* Raw tty mode logger - convert all \n to \r\n.  This is often called multiple times per log
** line - once for the timestamp and again for the message.
**
** No error checking on i/o calls - if stderr is gone theres not much else to do
*/
static int raw_logger(ib_msg_stream_t stream, const char*fmt, ...)
{
    int len;
    va_list ap;
    int done;
    char *ptr;
    char *eol;

    erl_drv_mutex_lock(G_LOGGER_LOCK);

    // Resize the log buffer until the message fits.
    va_start(ap, fmt);
    do
    {
        done = 1;
        len = vsnprintf(G_LOGGER_BUF, G_LOGGER_SIZE, fmt, ap);
        if (len >= G_LOGGER_SIZE)
        {
            G_LOGGER_SIZE = len + 128;
            G_LOGGER_BUF = driver_realloc(G_LOGGER_BUF, G_LOGGER_SIZE);
            done = 0;
        }
    } while(!done);
    va_end(ap);

    // Scan through the log message and break on \n
    ptr = G_LOGGER_BUF;
    eol = ptr - 1; // catch messages starting with\n 
    while (ptr != NULL && *ptr != '\0')
    {
        eol = strchr(eol+1, '\n');
        if (eol == NULL)
        {
            fputs(ptr, stderr);
            ptr = NULL;
        }
        else
        {
            size_t len = eol - ptr; // length of chars up to next LF
            if (len > 0)
            {
                fwrite(ptr, len, 1, stderr);
            }
            fputc('\r', stderr);
            ptr = eol; // ptr starts with next \n
        }
    }

    erl_drv_mutex_unlock(G_LOGGER_LOCK);

    return len;
}


static void send_ok(PortState* state)
{
    ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("innostore_ok") };
    driver_send_term(state->port, state->port_owner, response,
                     sizeof(response) / sizeof(response[0]));
}

static void send_ok_atom(PortState* state, const char* atom)
{
    ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("innostore_ok"),
                                  ERL_DRV_ATOM, driver_mk_atom((char*)atom),
                                  ERL_DRV_TUPLE, 2};
    driver_send_term(state->port, state->port_owner,
                     response, sizeof(response) / sizeof(response[0]));
}

static void send_error_atom(PortState* state, const char* atom)
{
    ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("innostore_error"),
                                  ERL_DRV_ATOM, driver_mk_atom((char*)atom),
                                  ERL_DRV_TUPLE, 2};
    driver_send_term(state->port, state->port_owner, response,
                     sizeof(response) / sizeof(response[0]));
}

static void send_error_str(PortState* state, const char* str)
{
    ErlDrvTermData response[] = { ERL_DRV_ATOM,   driver_mk_atom("innostore_error"),
                                  ERL_DRV_STRING, (ErlDrvTermData)str, (ErlDrvUInt)strlen(str),
                                  ERL_DRV_TUPLE, 2};
    driver_send_term(state->port, state->port_owner, response,
                     sizeof(response) / sizeof(response[0]));
}


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
#ifndef INNOSTORE_DRV_H
#define INNOSTORE_DRV_H

#include "erl_driver.h"
#include <embedded_innodb-1.0/innodb.h>

/**
 * Commands
 */

#define CMD_SET_CFG       1
#define CMD_START        (1 << 1)
#define CMD_INIT_TABLE   (1 << 2)
#define CMD_IS_STARTED   (1 << 3)
#define CMD_GET          (1 << 4)
#define CMD_PUT          (1 << 5)
#define CMD_DELETE       (1 << 6)
#define CMD_LIST_TABLES  (1 << 7)
#define CMD_CURSOR_OPEN  (1 << 8)
#define CMD_CURSOR_MOVE  (1 << 9)
#define CMD_CURSOR_CLOSE (1 << 10)
#define CMD_DROP_TABLE   (1 << 11)
#define CMD_STATUS       (1 << 12)

#define CMD_CURSOR_OPS   CMD_CURSOR_MOVE | CMD_CURSOR_CLOSE

/**
 * States
 */
#define STATE_READY  0
#define STATE_CURSOR 1

/**
 * Cursor movements/content requests
 */
#define CURSOR_FIRST 0
#define CURSOR_NEXT  1
#define CURSOR_PREV  2
#define CURSOR_LAST  4

#define CONTENT_KEY_ONLY  0
#define CONTENT_KEY_VALUE 1

/**
 * Operation function ptr
 */
typedef void (*op_fn_t)(void* arg);


/**
 * Port State
 */
typedef struct
{
    ErlDrvPort port;

    ErlDrvTermData port_owner;  /* Pid of the port owner */

    ErlDrvTid worker;           /* Worker thread associated with this port */

    ErlDrvMutex* worker_lock;

    ErlDrvCond* worker_cv;

    int port_state;

    int shutdown_flag;

    op_fn_t op;

    char* work_buffer;

    unsigned int work_buffer_sz;

    ib_trx_t txn;

    ib_crsr_t cursor;

} PortState;


/**
 * Helpful macros (from github/toland/bdberl)
 */
#define UNPACK_BYTE(_buf, _off) (_buf[_off])
#define UNPACK_INT(_buf, _off, _int_ptr)    (memcpy((void*)_int_ptr, (_buf+_off), sizeof(*_int_ptr)))
#define UNPACK_STRING(_buf, _off) (char*)(_buf+(_off))
#define UNPACK_BLOB(_buf, _off) (void*)(_buf+(_off))

#endif

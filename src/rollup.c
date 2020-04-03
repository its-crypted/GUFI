/*
This file is part of GUFI, which is part of MarFS, which is released
under the BSD license.


Copyright (c) 2017, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
Copyright 2017. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "BottomUp.h"
#include "dbutils.h"
#include "SinglyLinkedList.h"
#include "utils.h"

extern int errno;

const char SUBDIR_ATTACH_NAME[] = "subdir";

struct RollUp {
    struct BottomUp data;
    int rolledup;
};

/* check if the current directory can be rolled up */
int can_rollup(struct RollUp * rollup) {
    /* ********************* */
    /* need more checks here */
    /* ********************* */

    /* check if subdirectories have been rolled up */
    size_t total = 0;
    size_t rolledup = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct RollUp * child = (struct RollUp *) sll_node_data(node);
        rolledup += child->rolledup;
        total++;
    }

    return (total == rolledup);
}

/*
< 0 - could not open database
  0 - success
> 0 - number of subdirectories that failed to be moved
*/
int do_rollup(struct RollUp * rollup) {
    /* assume that this directory can be rolled up */
    /* can_rollup should have been called earlier  */

    int rc = 0;

    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, rollup->data.name);

    /* if there is no database file in this directory, don't create one         */
    /* don't use opendb to avoid creating a new database file and hiding errors */
    sqlite3 * dst = NULL;
    if (sqlite3_open_v2(dbname, &dst, SQLITE_OPEN_READWRITE, GUFI_SQLITE_VFS) == SQLITE_OK) {
        /* apply database optimizations */
        set_pragmas(dst);

        /* keep track of failed roll ups */
        int failed_rollup = 0;

        /* process each subdirectory */
        sll_loop(&rollup->data.subdirs, node) {
            struct BottomUp * child = (struct BottomUp *) sll_node_data(node);

            /* save this separately for remove */
            char child_db_name[MAXPATH];
            SNPRINTF(child_db_name, MAXPATH, "%s/" DBNAME, child->name);

            size_t child_failed = 0;

            /* attach subdir */
            {
                if (!attachdb(child_db_name, dst, SUBDIR_ATTACH_NAME, SQLITE_OPEN_READONLY)) {
                    child_failed = 1;
                }
            }

            /* copy all entries rows */
            /* this query will fail if the child's summary table is empty */
            if (!child_failed) {
                char copy[MAXSQL];
                sqlite3_snprintf(MAXSQL, copy, "INSERT INTO entries SELECT NULL, s.name || '/' || e.name, e.type, e.inode, e.mode, e.nlink, e.uid, e.gid, e.size, e.blksize, e.blocks, e.atime, e.mtime, e.ctime, e.linkname, e.xattrs, e.crtime, e.ossint1, e.ossint2, e.ossint3, e.ossint4, e.osstext1, e.osstext2 FROM %s.summary as s, %s.entries as e", SUBDIR_ATTACH_NAME, SUBDIR_ATTACH_NAME);

                char * err = NULL;
                if (sqlite3_exec(dst, copy, NULL, NULL, &err) != SQLITE_OK) {
                    fprintf(stderr, "Error: Failed to copy rows from \"%s\" to \"%s\": %s\n", child->name, rollup->data.name, err);
                    child_failed = 1;
                }
                sqlite3_free(err);
                err = NULL;
            }

            /* probably want to update summary table */

            /* detach subdir */
            {
                /* ignore errors - next attach will fail */
                detachdb(child_db_name, dst, SUBDIR_ATTACH_NAME);
            }

            failed_rollup += child_failed;

            /* if the roll up succeeded, remove the child database and directory */
            if (!child_failed) {
                if (unlink(child_db_name) != 0) {
                    fprintf(stderr, "Warning: Failed to delete \"%s\": %s\n", child_db_name, strerror(errno));
                }

                if (rmdir(child->name) != 0) {
                    fprintf(stderr, "Warning: Failed to remove \"%s\": %s\n", child->name, strerror(errno));
                }

                /* either of these failing leaves unneeded filesystem */
                /* entries but does not affect the roll up status     */
            }
        }

        if (failed_rollup) {
            rc = (int) failed_rollup;
        }
        else {
            rollup->rolledup = 1;
        }
    }
    else {
        /* this error message should only be seen if there are        */
        /* directories without database files at the top of the index */
        fprintf(stderr, "Warning: Could not open database at \"%s\": %s\n", rollup->data.name, sqlite3_errmsg(dst));
        rc = -1;
    }

    closedb(dst);

    return rc;
}

void rollup(void * args) {
    struct RollUp * dir = (struct RollUp *) args;
    dir->rolledup = 0;
    if (can_rollup(dir)) {
        do_rollup(dir);
    }
}

void sub_help() {
   printf("GUFI_index        GUFI index to roll up\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:", 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    epoch = since_epoch(&now);
    #endif

    struct OutputBuffers debug_buffers;
    debug_init(&debug_buffers, in.maxthreads + 1, 1024 * 1024);

    const int rc = parallel_bottomup(argv + idx, argc - idx,
                                     in.maxthreads,
                                     sizeof(struct RollUp), rollup,
                                     0
                                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                     , &debug_buffers
                                     #endif
        );

    debug_destroy(&debug_buffers, in.maxthreads + 1);

    return rc;
}

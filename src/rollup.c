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
#include <sys/types.h>
#include <unistd.h>

#include "bf.h"
#include "BottomUp.h"
#include "dbutils.h"
#include "debug.h"
#include "SinglyLinkedList.h"
#include "utils.h"

extern int errno;

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#define timestamp_args , id, timestamp_buffers
#else
#define timestamp_args
#endif

const char SUBDIR_ATTACH_NAME[] = "subdir";

#ifdef DEBUG
/* per thread stats */
struct RollUpStats {
    /* no need for mutex */
    size_t not_processed;
    size_t successful_rollup;
    size_t failed_rollup;
    size_t not_rolledup;
};
#endif

struct RollUp {
    struct BottomUp data;
    int rolledup;
};

const char PERM_SQL[] = "SELECT mode, uid, gid FROM summary";

struct Permissions {
    mode_t mode;
    uid_t uid;
    gid_t gid;
};

int get_permissions(void * args, int count, char ** data, char ** columns) {
    if (count != 3) {
        return 1;
    }

    struct Permissions * perms = (struct Permissions *) args;

    perms->mode = atoi(data[0]);
    perms->uid  = atoi(data[1]);
    perms->gid  = atoi(data[2]);

    return 0;
}

/*
@return -1 - failed to open a database
         0 - do not roll up
         1 - self and subdirectories are all o+rx
         2 - self and subdirectories have same user and group permissions, uid, and gid, and top is o-rx
         3 - self and subdirectories have same user permissions, go-rx, uid
         4 - self and subdirectories have same user, group, and others permissions, uid, and gid
*/
int check_permissions(struct Permissions * curr, const size_t child_count, struct sll * child_list timestamp_sig) {
    timestamp_create_buffer(4096);

    struct Permissions * child_perms = malloc(sizeof(struct Permissions) * sll_get_size(child_list));
    size_t idx = 0;

    /* get permissions of each child */
    sll_loop(child_list, node) {
        struct RollUp * child = (struct RollUp *) sll_node_data(node);

        char dbname[MAXPATH];
        SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, child->data.name);

        timestamp_start(open_child_db);
        sqlite3 * db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0
                              , NULL, NULL
                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                              , NULL, NULL
                              , NULL, NULL
                              #endif
            );
        timestamp_end(timestamp_buffers, id, ts_buf, "open_child_db", open_child_db);

        if (!db) {
            break;
        }

        /* get the child directory's permissions */
        timestamp_start(get_child_perms);
        char * err = NULL;
        const int exec_rc = sqlite3_exec(db, PERM_SQL, get_permissions, &child_perms[idx], &err);
        timestamp_end(timestamp_buffers, id, ts_buf, "get_child_perms", get_child_perms);

        timestamp_start(close_child_db);
        closedb(db);
        timestamp_end(timestamp_buffers, id, ts_buf, "close_child_db", close_child_db);

        if (exec_rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not get permissions of child directory \"%s\": %s\n", child->data.name, err);

            sqlite3_free(err);
            break;
        }

        sqlite3_free(err);

        idx++;
    }

    if (child_count != idx) {
        return -1;
    }

    size_t o_plus_rx = 0;
    size_t ugo = 0;
    size_t ug = 0;
    size_t u = 0;
    for(size_t i = 0; i < child_count; i++) {
        /* self and subdirectories are all o+rx */
        o_plus_rx += ((curr->mode & 0005) &&
                      (child_perms[i].mode & 0005) &&
                      (curr->uid == child_perms[i].uid) &&
                      (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user, group, and others permissions, uid, and gid */
        ugo += ((curr->mode == child_perms[i].mode) &&
                (curr->uid == child_perms[i].uid) &&
                (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user and group permissions, uid, and gid, and top is o-rx */
        ug += (((curr->mode & 0770) == (child_perms[i].mode & 0770)) &&
                (curr->uid == child_perms[i].uid) &&
                (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user permissions, go-rx, uid */
        u += (((curr->mode & 0700) == (child_perms[i].mode & 0700)) &&
              !(child_perms[i].mode & 0055) &&
              (curr->uid == child_perms[i].uid));

    }

    if (o_plus_rx == idx) {
      return 1;
    }

    if (ugo == idx) {
      return 4;
    }

    if (ug == idx) {
      return 2;
    }

    if (u == idx) {
      return 3;
    }

    return 0;
}

/* check if the current directory can be rolled up */
/*
@return   0 - cannot rollup
        > 0 - rollup score
*/
int can_rollup(struct RollUp * rollup,
               sqlite3 * dst
               timestamp_sig) {
    /* if (!rollup || !dst) { */
    /*     return -1; */
    /* } */

    timestamp_create_buffer(4096);
    timestamp_start(can_roll_up);

    /* default to cannot roll up */
    int legal = 0;

    /* all subdirectories are expected to pass */
    size_t total_subdirs = 0;

    /* check if ALL subdirectories have been rolled up */
    timestamp_start(check_subdirs_rolledup);
    size_t rolledup = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct RollUp * child = (struct RollUp *) sll_node_data(node);
        rolledup += child->rolledup;
        total_subdirs++;
    }
    timestamp_end(timestamp_buffers, id, ts_buf, "check_subdirs_rolledup", check_subdirs_rolledup);

    /* not all subdirectories were rolled up, so cannot roll up */
    if (total_subdirs != rolledup) {
        goto end_can_rollup;
    }

    /* get permissions of the current directory */
    timestamp_start(get_perms);
    struct Permissions perms;
    char * err = NULL;
    const int exec_rc = sqlite3_exec(dst, PERM_SQL, get_permissions, &perms, &err);
    timestamp_end(timestamp_buffers, id, ts_buf, "get_perms", get_perms);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get permissions of current directory \"%s\": %s\n", rollup->data.name, err);
        sqlite3_free(err);
        legal = 0;
        goto end_can_rollup;
    }

    sqlite3_free(err);

    /* check if the permissions of this directory and its subdirectories match */
    timestamp_start(check_perms);
    legal = check_permissions(&perms, total_subdirs, &rollup->data.subdirs timestamp_args);
    timestamp_end(timestamp_buffers, id, ts_buf, "check_perms", check_perms);

end_can_rollup:

    timestamp_end(timestamp_buffers, id, ts_buf, "can_rollup", can_roll_up);

    return legal;
}

/*
@return < 0 - bad arguments
          0 - success
        > 0 - number of subdirectories that failed to be moved
*/
int do_rollup(struct RollUp * rollup,
              sqlite3 *dst
              timestamp_sig) {
    /* assume that this directory can be rolled up */
    /* can_rollup should have been called earlier  */

    /* if (!rollup || !dst) { */
    /*     return -1; */
    /* } */

    timestamp_create_buffer(4096);
    timestamp_start(do_roll_up);

    int rc = 0;

    /* keep track of failed roll ups */
    int failed_rollup = 0;

    /* process each subdirectory */
    timestamp_start(rollup_subdirs);
    sll_loop(&rollup->data.subdirs, node) {
        timestamp_start(rollup_subdir);

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

        /* always detach subdir */
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
        timestamp_end(timestamp_buffers, id, ts_buf, "rollup_subdir", rollup_subdir);
    }
    timestamp_end(timestamp_buffers, id, ts_buf, "rollup_subdirs", rollup_subdirs);

    if (failed_rollup) {
        rc = (int) failed_rollup;
    }
    else {
        rollup->rolledup = 1;
    }

    timestamp_end(timestamp_buffers, id, ts_buf, "do_rollup", do_roll_up);
    return rc;
}

void rollup(void * args timestamp_sig) {
    struct RollUp * dir = (struct RollUp *) args;
    dir->rolledup = 0;

    timestamp_create_buffer(4096);

    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, dir->data.name);

    /* open the database file here to reduce number of open calls */
    timestamp_start(open_curr_db);
    sqlite3 * dst = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0
                           , NULL, NULL
                           #if defined(DEBUG) && defined(PER_THREAD_STATS)
                           , NULL, NULL
                           , NULL, NULL
                           #endif
        );

    timestamp_end(timestamp_buffers, id, ts_buf, "opendb", open_curr_db);

    #ifdef DEBUG
    struct RollUpStats * stats = (struct RollUpStats *) dir->data.extra_args;
    #endif

    if (dst) {
        if (can_rollup(dir, dst timestamp_args)) {
            if (do_rollup(dir, dst timestamp_args) > 0) {
            #ifdef DEBUG
                stats[dir->data.tid.up].successful_rollup++;
            }
            else {
                stats[dir->data.tid.up].failed_rollup++;
            #endif
            }
        }
        #ifdef DEBUG
        else {
            stats[dir->data.tid.up].not_rolledup++;
        }
        #endif
    }
    else {
        /* this error message should only be seen */
        /* at directories without database files  */
        fprintf(stderr, "Warning: Could not open database at \"%s\": %s\n", dir->data.name, sqlite3_errmsg(dst));
        #ifdef DEBUG
        stats[dir->data.tid.up].not_processed++;
        #endif
    }

    closedb(dst);
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
    epoch = since_epoch(NULL);

    timestamp_init(timestamp_buffers, in.maxthreads + 1, 1024 * 1024, NULL);
    #endif

    void * extra_args = NULL;

    #ifdef DEBUG
    struct RollUpStats * debug_stats = calloc(in.maxthreads, sizeof(struct RollUpStats));
    extra_args = debug_stats;
    #endif

    const int rc = parallel_bottomup(argv + idx, argc - idx,
                                     in.maxthreads,
                                     sizeof(struct RollUp), rollup,
                                     0,
                                     extra_args
                                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                     , timestamp_buffers
                                     #endif
        );

    #ifdef DEBUG
    size_t not_processed = 0;
    size_t successful_rollups = 0;
    size_t failed_rollups = 0;
    size_t not_rolledup = 0;
    for(size_t i = 0; i < in.maxthreads; i++) {
        not_processed      += debug_stats[i].not_processed;
        successful_rollups += debug_stats[i].successful_rollup;
        failed_rollups     += debug_stats[i].failed_rollup;
        not_rolledup       += debug_stats[i].not_rolledup;
    }
    free(debug_stats);

    fprintf(stderr, "Directories:\n");
    fprintf(stderr, "    Not processed:          %zu\n", not_processed);
    fprintf(stderr, "    Successfully rolled up: %zu\n", successful_rollups);
    fprintf(stderr, "    Failed to roll up:      %zu\n", failed_rollups);
    fprintf(stderr, "    Not rolled up:          %zu\n", not_rolledup);
    fprintf(stderr, "    Total:                  %zu\n", not_processed +
                                                         successful_rollups +
                                                         failed_rollups +
                                                         not_rolledup);

    #ifdef PER_THREAD_STATS
    timestamp_destroy(timestamp_buffers);
    #endif

    #endif

    return rc;
}

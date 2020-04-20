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

#define SUBDIR_ATTACH_NAME "subdir"

/* per thread stats */
struct RollUpStats {
    /* level of a successful rollup */
    struct sll levels;

    /* number of directories immediately under the top level of a roll up */
    /* sum this to get final count of dirs that need processing */
    struct sll subdirs;

    /* counts */
    size_t empty_dirs;
    size_t not_processed;
    size_t not_rolledup;
    size_t successful_rollup[5];
    size_t failed_rollup[5];

    #ifdef DEBUG
    struct OutputBuffers * print_buffers;
    #endif
};

size_t rollup_distribution(const char * name, size_t * distribution) {
    size_t total = 0;
    fprintf(stderr, "%s:\n", name);
    for(size_t i = 1; i < 5; i++) {
        fprintf(stderr, "    %zu: %20zu\n", i, distribution[i]);
        total += distribution[i];
    }
    fprintf(stderr, "    Total: %16zu\n", total);
    return total;
}

/* compare function for qsort */
int compare_size_t(const void * lhs, const void * rhs) {
    return (*(size_t *) lhs - *(size_t *) rhs);
}

#define sll_size_t_stats(name, stats, var, threads, width)              \
do {                                                                    \
    struct sll all;                                                     \
    sll_init(&all);                                                     \
    for(int i = 0; i < threads; i++) {                                  \
        sll_move_append(&all, &stats[i].var);                           \
    }                                                                   \
                                                                        \
    const size_t count = sll_get_size(&all);                            \
    if (count == 0) {                                                   \
        fprintf(stderr, "    No " name " stats\n");                     \
        break;                                                          \
    }                                                                   \
    size_t * array = malloc(count * sizeof(size_t));                    \
    size_t min = (size_t) -1;                                           \
    size_t max = 0;                                                     \
    size_t sum = 0;                                                     \
    size_t i = 0;                                                       \
    sll_loop(&all, node) {                                              \
        const size_t value = (size_t) (uintptr_t) sll_node_data(node);  \
        if (value < min) {                                              \
            min = value;                                                \
        }                                                               \
                                                                        \
        if (value > max) {                                              \
            max = value;                                                \
        }                                                               \
                                                                        \
        sum += value;                                                   \
        array[i++] = value;                                             \
    }                                                                   \
                                                                        \
    qsort(array, count, sizeof(size_t), compare_size_t);                \
    const size_t half = count / 2;                                      \
    double median = array[half];                                        \
    if (count % 2 == 0) {                                               \
        median += array[half + 1];                                      \
        median /= 2;                                                    \
    }                                                                   \
                                                                        \
    free(array);                                                        \
                                                                        \
    const double average = ((double) sum) / count;                      \
                                                                        \
    fprintf(stderr, "    " name ":\n");                                 \
    fprintf(stderr, "        count:      %" #width "zu\n", count);      \
    fprintf(stderr, "        min:        %" #width "zu\n", min);        \
    fprintf(stderr, "        max:        %" #width "zu\n", max);        \
    fprintf(stderr, "        median:     %" #width ".2f\n", median);    \
    fprintf(stderr, "        sum:        %" #width "zu\n", sum);        \
    fprintf(stderr, "        average:    %" #width ".2f\n", average);   \
                                                                        \
    sll_destroy(&all, 0);                                               \
} while (0)

void print_stats(struct RollUpStats * stats, const size_t threads) {
    size_t empty_dirs = 0;
    size_t not_processed = 0;
    size_t not_rolledup = 0;
    size_t successful_rollups[5] = {0};
    size_t failed_rollups[5] = {0};
    for(size_t i = 0; i < threads; i++) {
        empty_dirs += stats[i].empty_dirs;
        not_processed += stats[i].not_processed;
        not_rolledup  += stats[i].not_rolledup;
        for(size_t j = 1; j < 5; j++) {
            successful_rollups[j] += stats[i].successful_rollup[j];
            failed_rollups[j]     += stats[i].failed_rollup[j];
        }
    }

    fprintf(stderr, "Not processed: %12zu\n", not_processed);
    fprintf(stderr, "Not rolled up: %12zu\n", not_rolledup);
    const size_t successful = rollup_distribution("Successful rollups", successful_rollups);
    fprintf(stderr, "\n");
    sll_size_t_stats("Rollup occuring at a level", stats, levels, threads, 7);
    fprintf(stderr, "\n");
    /* the entries stat includes cumulative rollups, not just stats from the top level of a rolled up subtree */
    sll_size_t_stats("Remaining subdirs", stats, subdirs, threads, 7);
    const size_t failed = rollup_distribution("Failed rollups", failed_rollups);
    fprintf(stderr, "Total: %20zu (%zu empty)\n", not_processed +
                                                  not_rolledup +
                                                  successful +
                                                  failed,
                                                  empty_dirs);
}

/* main data being passed around during walk */
struct RollUp {
    struct BottomUp data;
    int rolledup;
    size_t entries;
};

/* ************************************** */
/* get permissions from directory entries */
const char PERM_SQL[] = "SELECT mode, uid, gid FROM summary WHERE isroot == 1";

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
int check_permissions(struct Permissions * curr, const size_t child_count, struct sll * child_list, const size_t id timestamp_sig) {
    timestamp_create_buffer(4096);

    struct Permissions * child_perms = malloc(sizeof(struct Permissions) * sll_get_size(child_list));

    /* get permissions of each child */
    size_t idx = 0;
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
        free(child_perms);
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
        ugo += (((curr->mode & 0555) == (child_perms[i].mode & 0555)) &&
                (curr->uid == child_perms[i].uid) &&
                (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user and group permissions, uid, and gid, and top is o-rx */
        ug += (((curr->mode & 0550) == (child_perms[i].mode & 0550)) &&
                (curr->uid == child_perms[i].uid) &&
                (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user permissions, go-rx, uid */
        u += (((curr->mode & 0500) == (child_perms[i].mode & 0500)) &&
              !(child_perms[i].mode & 0055) &&
              (curr->uid == child_perms[i].uid));

    }

    free(child_perms);

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
/* ************************************** */

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
    char * err = NULL;

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
    timestamp_end(timestamp_buffers, rollup->data.tid.up, ts_buf, "check_subdirs_rolledup", check_subdirs_rolledup);

    /* not all subdirectories were rolled up, so cannot roll up */
    if (total_subdirs != rolledup) {
        goto end_can_rollup;
    }

    /* get permissions of the current directory */
    timestamp_start(get_perms);
    struct Permissions perms;
    const int exec_rc = sqlite3_exec(dst, PERM_SQL, get_permissions, &perms, &err);
    timestamp_end(timestamp_buffers, rollup->data.tid.up, ts_buf, "get_perms", get_perms);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get permissions of current directory \"%s\": %s\n", rollup->data.name, err);
        legal = 0;
        goto end_can_rollup;
    }

    /* check if the permissions of this directory and its subdirectories match */
    timestamp_start(check_perms);
    legal = check_permissions(&perms, total_subdirs, &rollup->data.subdirs, rollup->data.tid.up timestamp_args);
    timestamp_end(timestamp_buffers, rollup->data.tid.up, ts_buf, "check_perms", check_perms);

end_can_rollup:
    sqlite3_free(err);

    timestamp_end(timestamp_buffers, rollup->data.tid.up, ts_buf, "can_rollup", can_roll_up);

    return legal;
}

/* drop pentries view */
/* create pentries table */
/* copy entries + pinode into pentries */
/* define here to be able to duplicate the SQL at compile time */
#define ROLLUP_CURRENT_DIR \
    "DROP VIEW IF EXISTS pentries;" \
    "CREATE TABLE pentries AS SELECT entries.*, summary.inode AS pinode FROM summary, entries;" /* "CREATE TABLE AS" already inserts rows, so no need to explicitly insert rows */ \
    "UPDATE summary SET rollupscore = 0;"

/* location of the 0 in ROLLUP_CURRENT_DIR */
static const size_t rollup_score_offset = sizeof(ROLLUP_CURRENT_DIR) - sizeof("0;");

/* copy subdirectory pentries into pentries */
/* copy subdirectory summary into summary */
static const char rollup_subdir[] =
    "INSERT INTO pentries SELECT * FROM " SUBDIR_ATTACH_NAME ".pentries;"
    "INSERT INTO summary  SELECT NULL, s.name || '/' || sub.name, sub.type, sub.inode, sub.mode, sub.nlink, sub.uid, sub.gid, sub.size, sub.blksize, sub.blocks, sub.atime, sub.mtime, sub.ctime, sub.linkname, sub.xattrs, sub.totfiles, sub.totlinks, sub.minuid, sub.maxuid, sub.mingid, sub.maxgid, sub.minsize, sub.maxsize, sub.totltk, sub.totmtk, sub.totltm, sub.totmtm, sub.totmtg, sub.totmtt, sub.totsize, sub.minctime, sub.maxctime, sub.minmtime, sub.maxmtime, sub.minatime, sub.maxatime, sub.minblocks, sub.maxblocks, sub.totxattr, sub.depth, sub.mincrtime, sub.maxcrtime, sub.minossint1, sub.maxossint1, sub.totossint1, sub.minossint2, sub.maxossint2, sub.totossint2, sub.minossint3, sub.maxossint3, sub.totossint3, sub.minossint4, sub.maxossint4, sub.totossint4, sub.rectype, sub.pinode, 0, sub.rollupscore FROM summary as s, " SUBDIR_ATTACH_NAME ".summary as sub WHERE s.isroot == 1;";

/*
@return < 0 - could not move entries into pentries
          0 - success
        > 0 - at least one subdirectory failed to be moved
*/
int do_rollup(struct RollUp * rollup,
              sqlite3 *dst,
              const int rollup_score
              timestamp_sig) {
    /* assume that this directory can be rolled up */
    /* can_rollup should have been called earlier  */

    /* if (!rollup || !dst) { */
    /*     return -1; */
    /* } */

    timestamp_create_buffer(4096);
    timestamp_start(do_roll_up);

    int rc = 0;
    char * err = NULL;
    int exec_rc = SQLITE_OK;

    /* set the rollup score in the SQL statement */
    char rollup_current_dir[] = ROLLUP_CURRENT_DIR;
    rollup_current_dir[rollup_score_offset] += rollup_score;

    timestamp_start(rollup_current_dir);
    exec_rc = sqlite3_exec(dst, rollup_current_dir, NULL, NULL, &err);
    timestamp_end(timestamp_buffers, rollup->tid.up, ts_buf, "rollup_current_dir", rollup_current_dir);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Failed to copy \"%s\" entries into pentries table: %s\n", rollup->data.name, err);
        rc = -1;
        goto end_rollup;
    }

    /* if any subdir fails to roll up, record it */
    int failed_rollup = 0;

    /* process each subdirectory */
    timestamp_start(rollup_subdirs);

    sll_loop(&rollup->data.subdirs, node) {
        timestamp_start(rollup_subdir);

        struct BottomUp * child = (struct BottomUp *) sll_node_data(node);

        char child_db_name[MAXPATH];
        SNPRINTF(child_db_name, MAXPATH, "%s/" DBNAME, child->name);

        size_t child_failed = 0;

        /* attach subdir database file as 'SUBDIR_ATTACH_NAME' */
        child_failed = !attachdb(child_db_name, dst, SUBDIR_ATTACH_NAME, SQLITE_OPEN_READONLY);

        /* roll up the subdir into this dir */
        if (!child_failed) {
            timestamp_start(rollup_subdir);
            exec_rc = sqlite3_exec(dst, rollup_subdir, NULL, NULL, &err);
            timestamp_end(timestamp_buffers, rollup->tid.up, ts_buf, "rollup_subdir", rollup_subdir);
            if (exec_rc != SQLITE_OK) {
                fprintf(stderr, "Error: Failed to copy \"%s\" subdir pentries into pentries table: %s\n", child->name, err);
                child_failed = 1;
            }
        }

        /* always detach subdir */
        detachdb(child_db_name, dst, SUBDIR_ATTACH_NAME);

        timestamp_end(timestamp_buffers, rollup->tid.up, ts_buf, "rollup_subdir", rollup_subdir);

        if ((failed_rollup = child_failed)) {
            break;
        }
    }

    timestamp_end(timestamp_buffers, rollup->tid.up, ts_buf, "rollup_subdirs", rollup_subdirs);

    if (failed_rollup) {
        rc = (int) failed_rollup;
    }
    else {
        rollup->rolledup = rollup_score;
    }

end_rollup:
    sqlite3_free(err);

    timestamp_end(timestamp_buffers, rollup->tid.up, ts_buf, "do_rollup", do_roll_up);
    return rc;
}

void rollup(void * args timestamp_sig) {
    struct RollUp * dir = (struct RollUp *) args;
    dir->rolledup = 0;

    const size_t id = dir->data.tid.up;

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
        const int rollup_score = can_rollup(dir, dst timestamp_args);

        #ifdef DEBUG
        #ifdef PRINT_ROLLUP_SCORE
        struct OutputBuffer * obuf = &(stats->print_buffers->buffers[id]);

        pthread_mutex_lock(&stats->print_buffers->mutex);
        const size_t len = strlen(dir->data.name) + 3;
        if ((obuf->capacity - obuf->filled) < len) {
           fwrite(obuf->buf, sizeof(char), obuf->filled, stderr);
           obuf->filled = 0;
        }

        if (len >= obuf->capacity) {
           fwrite(obuf->buf, sizeof(char), obuf->filled, stderr);
        }
        pthread_mutex_unlock(&stats->print_buffers->mutex);

        memcpy(obuf->buf + obuf->filled, dir->data.name, len - 3);

        char score[] = " 0\n";
        score[1] += rollup_score;
        memcpy(obuf->buf + obuf->filled + len - 3, score, 3);

        obuf->filled += len;
        #endif
        #endif

        if (rollup_score > 0) {
            if (in.dry_run || (do_rollup(dir, dst, rollup_score timestamp_args) == 0)) {
                dir->rolledup = rollup_score;
                sll_push(&stats[id].levels, (void *) (uintptr_t) dir->data.level);
                stats[id].successful_rollup[rollup_score]++;
            }
            else {
                stats[id].failed_rollup[rollup_score]++;
            }
        }
        else {
            stats[id].not_rolledup++;
        }

        /* if roll up failed, then all subdirs are the top of their subtrees */
        /* also handle root directory if it was rolled up */
        if ((rollup_score == 0) || ((rollup_score > 0) && !dir->data.parent)) {
            sll_loop(&dir->data.subdirs, node) {
                struct RollUp * child = (struct RollUp *) sll_node_data(node);
                size_t value = 0;
                if (child->rolledup) {
                    value = 1;
                }
                else {
                    value = child->data.subdir_count;
                }

                sll_push(&stats[id].subdirs, (void *) (uintptr_t) value);
            }
        }
    }
    else {
        stats[id].not_processed++;
    }

    closedb(dst);
}

void sub_help() {
   printf("GUFI_index        GUFI index to roll up\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:X", 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    epoch = since_epoch(NULL);

    timestamp_init(timestamp_buffers, in.maxthreads + 1, 1024 * 1024);
    #endif

    struct RollUpStats * stats = calloc(in.maxthreads, sizeof(struct RollUpStats));
    for(int i = 0; i < in.maxthreads; i++) {
        sll_init(&stats[i].levels);
        sll_init(&stats[i].subdirs);
    }

    #ifdef DEBUG
    struct OutputBuffers print_buffers;
    OutputBuffers_init(&print_buffers, in.maxthreads, 1024 * 1024);

    for(int i = 0; i < in.maxthreads; i++) {
        stats[i].print_buffers = &print_buffers;
    }
    #endif

    const int rc = parallel_bottomup(argv + idx, argc - idx,
                                     in.maxthreads,
                                     sizeof(struct RollUp), rollup,
                                     0,
                                     stats
                                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                     , timestamp_buffers
                                     #endif
        );

    #ifdef DEBUG
    OutputBuffers_flush_single(&print_buffers, in.maxthreads, stderr);
    OutputBuffers_destroy(&print_buffers, in.maxthreads);

    #ifdef PER_THREAD_STATS
    timestamp_destroy(timestamp_buffers, in.maxthreads + 1);
    #endif

    #endif

    print_stats(stats, in.maxthreads);

    for(int i = 0; i < in.maxthreads; i++) {
        #ifdef DEBUG
        stats[i].print_buffers = NULL;
        #endif
        sll_destroy(&stats[i].levels, 0);
        sll_destroy(&stats[i].subdirs, 0);
    }
    free(stats);

    return rc;
}

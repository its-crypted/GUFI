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



/*
iterate through all directories
wait until all subdirectories have been
processed before processing the current one
*/

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "bf.h"
#include "dbutils.h"
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "utils.h"

extern int errno;

const char SUBDIR_ATTACH_NAME[] = "subdir";

struct Rollup {
    char name[MAXPATH];
    struct stat stat;
    struct {
        pthread_mutex_t mutex;
        size_t count;
    } refs;
    int rolledup;
    struct sll subdirs;
    struct Rollup * parent;
};

/* check if the current directory can be rolled up */
int can_rollup(struct Rollup * rollup) {
    /* ********************* */
    /* need more checks here */
    /* ********************* */

    /* check if subdirectories have been rolled up */
    size_t total = 0;
    size_t rolledup = 0;
    sll_loop(&rollup->subdirs, node) {
        struct Rollup * child = (struct Rollup *) sll_node_data(node);
        rolledup += child->rolledup;
        total++;
    }

    return (total == rolledup);
}

/*
< 0 - could not open database or failed to update rolledup column
  0 - success
> 0 - number of subdirectories that failed to be moved
*/
int do_rollup(struct Rollup * rollup) {
    /* assume that this directory can be rolled up */
    /* can_rollup should have been called earlier  */

    int rc = 0;

    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, rollup->name);
    sqlite3 * dst = opendb(dbname, RDWR, 0, 0
                           , NULL, NULL
                           #ifdef DEBUG
                           , NULL, NULL
                           , NULL, NULL
                           , NULL, NULL
                           , NULL, NULL
                           #endif
        );

    if (dst) {
        /* keep track of failed roll ups */
        int failed_rollup = 0;

        /* process each subdirectory */
        sll_loop(&rollup->subdirs, node) {
            struct Rollup * child = (struct Rollup *) sll_node_data(node);

            /* save this separately for remove */
            char child_db_name[MAXPATH];
            SNPRINTF(child_db_name, MAXPATH, "%s/" DBNAME, child->name);

            /* open this database in readonly mode */
            char child_db_uri[MAXPATH];
            SNPRINTF(child_db_uri, MAXPATH, "file:%s?mode=ro", child_db_name);

            char * err = NULL;
            size_t child_failed = 0;

            /* attach subdir */
            {
                char attach[MAXSQL];
                sqlite3_snprintf(MAXSQL, attach, "ATTACH %Q as %Q", child_db_uri, SUBDIR_ATTACH_NAME);

                if (sqlite3_exec(dst, attach, NULL, NULL, &err) != SQLITE_OK) {
                    fprintf(stderr, "Error: Failed to attach \"%s\": %s\n", child->name, err);
                    child_failed = 1;
                }
                sqlite3_free(err);
                err = NULL;
            }

            /* copy all entries rows */
            if (!child_failed) {
                char copy[MAXSQL];
                sqlite3_snprintf(MAXSQL, copy, "INSERT INTO entries SELECT NULL, s.name || '/' || e.name, e.type, e.inode, e.mode, e.nlink, e.uid, e.gid, e.size, e.blksize, e.blocks, e.atime, e.mtime, e.ctime, e.linkname, e.xattrs, e.crtime, e.ossint1, e.ossint2, e.ossint3, e.ossint4, e.osstext1, e.osstext2 FROM %s.summary as s, %s.entries as e", SUBDIR_ATTACH_NAME, SUBDIR_ATTACH_NAME);

                if (sqlite3_exec(dst, copy, NULL, NULL, &err) != SQLITE_OK) {
                    fprintf(stderr, "Error: Failed to copy rows from \"%s\" to \"%s\": %s\n", child->name, rollup->name, err);
                    child_failed = 1;
                }
                sqlite3_free(err);
                err = NULL;
            }

            /* probably want to update summary table */

            /* detach subdir */
            {
                char attach[MAXSQL];
                sqlite3_snprintf(MAXSQL, attach, "DETACH %Q", SUBDIR_ATTACH_NAME);

                if (sqlite3_exec(dst, attach, NULL, NULL, &err) != SQLITE_OK) {
                    fprintf(stderr, "Error: Failed to dettach \"%s\": %s\n", child->name, err);
                    child_failed = 1;
                }
                sqlite3_free(err);
                err = NULL;
            }

            /* if the roll up succeeded, remove the child database and directory */
            if (!child_failed) {
                if (unlink(child_db_name) != 0) {
                    fprintf(stderr, "Error: Failed to delete \"%s\": %s\n", child_db_name, strerror(errno));
                }

                if (rmdir(child->name) != 0) {
                    fprintf(stderr, "Error: Failed to remove \"%s\": %s\n", child->name, strerror(errno));
                }
            }

            failed_rollup += child_failed;
        }

        if (failed_rollup) {
            rc = (int) failed_rollup;
        }
        else {
            rollup->rolledup = 1;
        }
    }
    else {
        fprintf(stderr, "Error: Failed to open parent database at \"%s\": %s\n", rollup->name, sqlite3_errmsg(dst));
        rc = -1;
    }

    closedb(dst);

    return rc;
}

int ascend_to_top(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    /* reached root */
    if (!data) {
        return 0;
    }

    struct Rollup * rollup = (struct Rollup *) data;

    pthread_mutex_lock(&rollup->refs.mutex);
    size_t remaining = 0;
    if (rollup->refs.count) {
        remaining = --rollup->refs.count;
    }
    pthread_mutex_unlock(&rollup->refs.mutex);

    if (remaining) {
        return 0;
    }

    /* no subdirectories still need processing, so can attempt to roll up */

    if (can_rollup(rollup)) {
        if (do_rollup(rollup) != 0) {
            fprintf(stderr, "Error: Rollup failed: %s\n", rollup->name);
        }
    }

    /* clean up first, just in case parent runs before  */
    /* the current `struct Rollup` finishes cleaning up */

    /* clean up 'struct Rollup's here, when they are */
    /* children instead of when they are the parent  */
    sll_destroy(&rollup->subdirs, 1);

    /* mutex is not needed any more */
    pthread_mutex_destroy(&rollup->refs.mutex);

    /* always push parent to decrement their reference counters */
    QPTPool_enqueue(ctx, id, ascend_to_top, rollup->parent);

    return 0;
}

int descend_to_bottom(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    struct Rollup * rollup = (struct Rollup *) data;
    DIR * dir = opendir(rollup->name);
    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\": %s\n", rollup->name, strerror(errno));
        free(data);
        return 0;
    }

    pthread_mutex_init(&rollup->refs.mutex, NULL);
    rollup->refs.count = 0;
    rollup->rolledup = 0;
    sll_init(&rollup->subdirs);

    struct dirent * entry = NULL;
    while ((entry = readdir(dir))) {
        if ((strncmp(entry->d_name, ".",  2) == 0) ||
            (strncmp(entry->d_name, "..", 3) == 0)) {
            continue;
        }

        struct Rollup new_work;
        SNPRINTF(new_work.name, MAXPATH, "%s/%s", rollup->name, entry->d_name);

        if (lstat(new_work.name, &new_work.stat) != 0) {
            fprintf(stderr, "Error: Could not stat \"%s\": %s", new_work.name, strerror(errno));
            continue;
        }

        if (!S_ISDIR(new_work.stat.st_mode)) {
            continue;
        }

        struct Rollup * copy = malloc(sizeof(struct Rollup));
        memcpy(copy, &new_work, sizeof(struct Rollup));

        /* store the entries without enqueuing them */
        sll_push(&rollup->subdirs, copy);

        /* count how many children this directory has */
        rollup->refs.count++;
    }

    closedir(dir);

    /* if there are subdirectories, this directory cannot go back up just yet */
    if (rollup->refs.count) {
        sll_loop(&rollup->subdirs, node)  {
            struct Rollup *child = (struct Rollup *) sll_node_data(node);
            child->parent = rollup;

            /* keep going down */
            QPTPool_enqueue(ctx, id, descend_to_bottom, child);
        }
    }
    else {
        /* start working upwards */
        QPTPool_enqueue(ctx, id, ascend_to_top, rollup);
    }

    return 0;
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

    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Error: Failed to initialize thread pool\n");
        return -1;
    }

    if (QPTPool_start(pool, NULL) != (size_t) in.maxthreads) {
        fprintf(stderr, "Error: Failed to start threads\n");
        return -1;
    }

    /* enqueue all directories */
    const size_t count = argc - idx;
    struct Rollup * roots = malloc(count * sizeof(struct Rollup));
    for(size_t i = 0; i < count; i++) {
        struct Rollup * root = &roots[i];
        SNPRINTF(root->name, MAXPATH, "%s", argv[idx + i]);
        if (lstat(root->name, &root->stat) != 0) {
            fprintf(stderr, "Could not stat %s\n", root->name);
            free(root);
            continue;
        }

        if (!S_ISDIR(root->stat.st_mode)) {
            fprintf(stderr, "%s is not a directory\n", root->name);
            free(root);
            continue;
        }

        root->parent = NULL;

        QPTPool_enqueue(pool, i % in.maxthreads, descend_to_bottom, root);
    }

    QPTPool_wait(pool);

    /* clean up root directories since they don't get freed during processing */
    free(roots);

    const size_t thread_count = QPTPool_threads_completed(pool);
    fprintf(stderr, "Ran %zu threads\n", thread_count);

    QPTPool_destroy(pool);

    return 0;
}

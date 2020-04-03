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
#include "BottomUp.h"
#include "dbutils.h"
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "utils.h"

extern int errno;

struct UserArgs {
    size_t user_struct_size;
    AscendFunc_t func;
    int track_non_dirs;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    /* BottomUp Debug OutputBuffers*/
    struct OutputBuffers * debug_buffers;
    #else
    void * debug_buffers;
    #endif
};

int ascend_to_top(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    debug_create_buffer(4096);
    debug_define_start(ascend);

    struct UserArgs * ua = (struct UserArgs *) args;

    /* reached root */
    if (!data) {
        debug_end_print(ua->debug_buffers, id, debug_buffer, "ascend_to_top", ascend);
        return 0;
    }

    struct BottomUp * bu = (struct BottomUp *) data;

    debug_define_start(lock_refs);
    pthread_mutex_lock(&bu->refs.mutex);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "lock_refs", lock_refs);

    debug_define_start(get_remaining_refs);
    size_t remaining = 0;
    if (bu->refs.count) {
        remaining = --bu->refs.count;
    }
    debug_end_print(ua->debug_buffers, id, debug_buffer, "get_remaining_refs", get_remaining_refs);

    debug_define_start(unlock_refs);
    pthread_mutex_unlock(&bu->refs.mutex);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "unlock_refs", unlock_refs);

    if (remaining) {
        debug_end_print(ua->debug_buffers, id, debug_buffer, "ascend_to_top", ascend);
        return 0;
    }

    /* no subdirectories still need processing, so can attempt to roll up */

    /* call user function */
    debug_define_start(run_user_func);
    ua->func(bu);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "run_user_function", run_user_func);

    /* clean up first, just in case parent runs before  */
    /* the current `struct BottomUp` finishes cleaning up */

    /* clean up 'struct BottomUp's here, when they are */
    /* children instead of when they are the parent  */
    debug_define_start(cleanup);
    sll_destroy(&bu->subdirs, 1);
    sll_destroy(&bu->subnondirs, 1);

    /* mutex is not needed any more */
    pthread_mutex_destroy(&bu->refs.mutex);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "cleanup", cleanup);

    /* always push parent to decrement their reference counters */
    debug_define_start(enqueue_ascend);
    QPTPool_enqueue(ctx, id, ascend_to_top, bu->parent);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "enqueue_ascend", enqueue_ascend);

    debug_end_print(ua->debug_buffers, id, debug_buffer, "ascend_to_top", ascend);
    return 0;
}

static struct BottomUp * track(const char * name, const size_t name_len,
                               const size_t user_struct_size, struct sll * sll) {
    struct BottomUp * copy = malloc(user_struct_size);

    /* don't need anything else */
    memcpy(copy->name, name, name_len + 1);

    /* store the subdirectories without enqueuing them */
    sll_push(sll, copy);

    return copy;
}

int descend_to_bottom(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    debug_create_buffer(4096);
    debug_define_start(descend);

    struct UserArgs * ua = (struct UserArgs *) args;
    struct BottomUp * bu = (struct BottomUp *) data;

    debug_define_start(open_dir);
    DIR * dir = opendir(bu->name);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "opendir", open_dir);

    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\": %s\n", bu->name, strerror(errno));
        free(data);
        debug_end_print(ua->debug_buffers, id, debug_buffer, "descend_to_bottom", descend);
        return 0;
    }

    debug_define_start(init);
    pthread_mutex_init(&bu->refs.mutex, NULL);
    bu->refs.count = 0;
    sll_init(&bu->subdirs);
    sll_init(&bu->subnondirs);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "init", init);

    debug_define_start(read_dir_loop);
    while (1) {
        debug_define_start(read_dir);
        struct dirent * entry = readdir(dir);;
        debug_end_print(ua->debug_buffers, id, debug_buffer, "readdir", read_dir);

        if (!entry) {
            break;
        }

        if ((strncmp(entry->d_name, ".",  2) == 0) ||
            (strncmp(entry->d_name, "..", 3) == 0)) {
            continue;
        }

        struct BottomUp new_work;
        const size_t name_len = SNPRINTF(new_work.name, MAXPATH, "%s/%s", bu->name, entry->d_name);

        debug_define_start(lstat_entry);
        struct stat st;
        const int rc = lstat(new_work.name, &st);
        debug_end_print(ua->debug_buffers, id, debug_buffer, "lstat", lstat_entry);

        if (rc != 0) {
            fprintf(stderr, "Error: Could not stat \"%s\": %s", new_work.name, strerror(errno));
            continue;
        }

        debug_define_start(track_entry);
        if (S_ISDIR(st.st_mode)) {
            track(new_work.name, name_len, ua->user_struct_size, &bu->subdirs);

            /* count how many subdirectories this directory has */
            bu->refs.count++;
        }
        else {
            if (ua->track_non_dirs) {
                track(new_work.name, name_len, ua->user_struct_size, &bu->subnondirs);
            }
        }
        debug_end_print(ua->debug_buffers, id, debug_buffer, "track", track_entry);
    }
    debug_end_print(ua->debug_buffers, id, debug_buffer, "readdir_loop", read_dir_loop);

    debug_define_start(close_dir);
    closedir(dir);
    debug_end_print(ua->debug_buffers, id, debug_buffer, "closedir", close_dir);

    /* if there are subdirectories, this directory cannot go back up just yet */
    if (bu->refs.count) {
        debug_define_start(enqueue_subdirs);
        sll_loop(&bu->subdirs, node)  {
            struct BottomUp *child = (struct BottomUp *) sll_node_data(node);
            child->parent = bu;

            /* keep going down */
            debug_define_start(enqueue_subdir);
            QPTPool_enqueue(ctx, id, descend_to_bottom, child);
            debug_end_print(ua->debug_buffers, id, debug_buffer, "enqueue_subdir", enqueue_subdir);
        }
            debug_end_print(ua->debug_buffers, id, debug_buffer, "enqueue_subdirs", enqueue_subdirs);
    }
    else {
        /* start working upwards */
        debug_define_start(enqueue_bottom);
        QPTPool_enqueue(ctx, id, ascend_to_top, bu);
        debug_end_print(ua->debug_buffers, id, debug_buffer, "enqueue_bottom", enqueue_bottom);
    }

    debug_end_print(ua->debug_buffers, id, debug_buffer, "descend_to_bottom", descend);
    return 0;
}

int parallel_bottomup(char ** root_names, size_t root_count,
                      const size_t thread_count,
                      const size_t user_struct_size, AscendFunc_t func,
                      const int track_non_dirs
                      #if defined(DEBUG) && defined(PER_THREAD_STATS)
                      , struct OutputBuffers * debug_buffers
                      #endif
    ) {
    struct UserArgs ua;
    ua.debug_buffers = debug_buffers;

    debug_create_buffer(4096);

    if (user_struct_size < sizeof(struct BottomUp)) {
        fprintf(stderr, "Error: Provided user struct size is smaller than a struct BottomUp\n");
        return -1;
    }

    if (!func) {
        fprintf(stderr, "Error: No function provided\n");
        return -1;
    }

    ua.user_struct_size = user_struct_size;
    ua.func = func;
    ua.track_non_dirs = track_non_dirs;

    struct QPTPool * pool = QPTPool_init(thread_count);
    if (!pool) {
        fprintf(stderr, "Error: Failed to initialize thread pool\n");
        return -1;
    }

    if (QPTPool_start(pool, &ua) != (size_t) thread_count) {
        fprintf(stderr, "Error: Failed to start threads\n");
        QPTPool_destroy(pool);
        return -1;
    }

    /* enqueue all root directories */
    debug_define_start(enqueue_roots);
    struct BottomUp * roots = malloc(root_count * user_struct_size);
    for(size_t i = 0; i < root_count; i++) {
        struct BottomUp * root = &roots[i];
        SNPRINTF(root->name, MAXPATH, "%s", root_names[i]);

        struct stat st;
        if (lstat(root->name, &st) != 0) {
            fprintf(stderr, "Could not stat %s\n", root->name);
            free(root);
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "%s is not a directory\n", root->name);
            free(root);
            continue;
        }

        root->parent = NULL;

        debug_define_start(enqueue_root);
        QPTPool_enqueue(pool, i % in.maxthreads, descend_to_bottom, root);
        debug_end_print(ua.debug_buffers, thread_count, debug_buffer, "enqueue_root", enqueue_root);
    }
    debug_end_print(ua.debug_buffers, thread_count, debug_buffer, "enqueue_roots", enqueue_roots);

    debug_define_start(qptpool_wait);
    QPTPool_wait(pool);
    debug_end_print(ua.debug_buffers, thread_count, debug_buffer, "wait_for_threads", qptpool_wait);

    /* clean up root directories since they don't get freed during processing */
    free(roots);

    const size_t threads_ran = QPTPool_threads_completed(pool);
    fprintf(stdout, "Ran %zu threads\n", threads_ran);

    QPTPool_destroy(pool);

    return 0;
}

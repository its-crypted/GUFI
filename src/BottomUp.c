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
    size_t size;
    AscendFunc_t func;
};

int ascend_to_top(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    /* reached root */
    if (!data) {
        return 0;
    }

    struct BottomUp * bu = (struct BottomUp *) data;
    struct UserArgs * ua = (struct UserArgs *) args;

    pthread_mutex_lock(&bu->refs.mutex);
    size_t remaining = 0;
    if (bu->refs.count) {
        remaining = --bu->refs.count;
    }
    pthread_mutex_unlock(&bu->refs.mutex);

    if (remaining) {
        return 0;
    }

    /* no subdirectories still need processing, so can attempt to roll up */

    /* call user function */
    ua->func(bu);

    /* clean up first, just in case parent runs before  */
    /* the current `struct BottomUp` finishes cleaning up */

    /* clean up 'struct BottomUp's here, when they are */
    /* children instead of when they are the parent  */
    sll_destroy(&bu->subdirs, 1);

    /* mutex is not needed any more */
    pthread_mutex_destroy(&bu->refs.mutex);

    /* always push parent to decrement their reference counters */
    QPTPool_enqueue(ctx, id, ascend_to_top, bu->parent);

    return 0;
}

int descend_to_bottom(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    struct BottomUp * bu = (struct BottomUp *) data;
    DIR * dir = opendir(bu->name);
    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\": %s\n", bu->name, strerror(errno));
        free(data);
        return 0;
    }

    const size_t size = ((struct UserArgs *) args)->size;

    pthread_mutex_init(&bu->refs.mutex, NULL);
    bu->refs.count = 0;
    sll_init(&bu->subdirs);

    struct dirent * entry = NULL;
    while ((entry = readdir(dir))) {
        if ((strncmp(entry->d_name, ".",  2) == 0) ||
            (strncmp(entry->d_name, "..", 3) == 0)) {
            continue;
        }

        struct BottomUp new_work;
        const size_t name_len = SNPRINTF(new_work.name, MAXPATH, "%s/%s", bu->name, entry->d_name);

        struct stat st;
        if (lstat(new_work.name, &st) != 0) {
            fprintf(stderr, "Error: Could not stat \"%s\": %s", new_work.name, strerror(errno));
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            continue;
        }

        struct BottomUp * copy = malloc(size);

        /* don't need anything else */
        memcpy(copy->name, new_work.name, name_len + 1);

        /* store the subdirectories without enqueuing them */
        sll_push(&bu->subdirs, copy);

        /* count how many children this directory has */
        bu->refs.count++;
    }

    closedir(dir);

    /* if there are subdirectories, this directory cannot go back up just yet */
    if (bu->refs.count) {
        sll_loop(&bu->subdirs, node)  {
            struct BottomUp *child = (struct BottomUp *) sll_node_data(node);
            child->parent = bu;

            /* keep going down */
            QPTPool_enqueue(ctx, id, descend_to_bottom, child);
        }
    }
    else {
        /* start working upwards */
        QPTPool_enqueue(ctx, id, ascend_to_top, bu);
    }

    return 0;
}

int parallel_bottomup(char ** root_names, size_t root_count,
                      const size_t thread_count,
                      const size_t size, AscendFunc_t func) {
    if (size < sizeof(struct BottomUp)) {
        fprintf(stderr, "Error: Provided size is smaller than a struct BottomUp\n");
        return -1;
    }

    if (!func) {
        fprintf(stderr, "Error: No function provided\n");
        return -1;
    }

    struct UserArgs ua;
    ua.size = size;
    ua.func = func;

    struct QPTPool * pool = QPTPool_init(thread_count);
    if (!pool) {
        fprintf(stderr, "Error: Failed to initialize thread pool\n");
        return -1;
    }

    if (QPTPool_start(pool, &ua) != (size_t) thread_count) {
        fprintf(stderr, "Error: Failed to start threads\n");
        return -1;
    }

    /* enqueue all root directories */
    struct BottomUp * roots = malloc(root_count * size);
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

        QPTPool_enqueue(pool, i % in.maxthreads, descend_to_bottom, root);
    }

    QPTPool_wait(pool);

    /* clean up root directories since they don't get freed during processing */
    free(roots);

    const size_t threads_ran = QPTPool_threads_completed(pool);
    fprintf(stderr, "Ran %zu threads\n", threads_ran);

    QPTPool_destroy(pool);

    return 0;
}

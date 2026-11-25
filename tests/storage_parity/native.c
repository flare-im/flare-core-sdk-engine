/* Drive the public C binding one callback at a time. EOF releases the handle. */
#include "flare_im_core_sdk_ffi.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ready = PTHREAD_COND_INITIALIZER;
static bool done;
static bool failed;

static void on_result(void *ctx, const FlareError *error, FlareString result) {
    (void)ctx;
    pthread_mutex_lock(&lock);
    if (error) {
        fprintf(stderr, "SDK error %d: %.*s\n", error->code,
                (int)error->message.len, error->message.ptr);
        flare_error_heap_free((FlareError *)error);
        failed = true;
    } else {
        if (result.len) fwrite(result.ptr, 1, result.len, stdout);
        else fputs("null", stdout);
        fputc('\n', stdout);
        fflush(stdout);
    }
    flare_string_free(result);
    done = true;
    pthread_cond_signal(&ready);
    pthread_mutex_unlock(&lock);
}

/* Keep asynchronous ACK and sync observations separate from request responses. */
static FILE *events;
static void on_event(void *ctx, int32_t kind, FlareString payload) {
    (void)ctx;
    if (kind == FLARE_EVENT_MESSAGE_SEND_ACK || kind == FLARE_EVENT_SYNC_READINESS) {
        fprintf(events, "%.*s\n", (int)payload.len, payload.ptr);
        fflush(events);
    }
    flare_string_free(payload);
}

int main(void) {
    FlareSubscriptionHandle subscription = 0;
    FlareHandle handle = flare_sdk_create();
    if (!handle) return 1;
    char *line = NULL;
    size_t capacity = 0;
    while (getline(&line, &capacity, stdin) >= 0) {
        char *body = strchr(line, '\t');
        if (!body) return 1;
        *body++ = 0;
        body[strcspn(body, "\n")] = 0;
        done = false;
        int rc;
        if (!strcmp(line, "sdk.init"))
            rc = flare_sdk_init(handle, body, NULL, on_result);
        else if (!strcmp(line, "sdk.prepare"))
            rc = flare_sdk_prepare(handle, body, NULL, NULL, on_result);
        else if (!strcmp(line, "sdk.connect")) {
            char *token = strchr(body, '\t');
            if (!token) return 1;
            *token++ = 0;
            if (!subscription) {
                events = fdopen(3, "w");
                if (!events) return 1;
                subscription = flare_event_subscribe(handle, NULL, on_event);
                if (!subscription) return 1;
            }
            rc = flare_sdk_connect(handle, body, token, NULL, on_result);
        } else
            rc = flare_sdk_invoke_json(handle, line, body, NULL, on_result);
        if (rc) return 1;
        pthread_mutex_lock(&lock);
        while (!done) pthread_cond_wait(&ready, &lock);
        pthread_mutex_unlock(&lock);
        if (failed) return 1;
    }
    if (subscription) flare_event_unsubscribe(subscription);
    free(line);
    flare_sdk_release(handle);
    return 0;
}

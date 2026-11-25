/* Drive the public C binding one callback at a time. EOF releases the handle.
 * A result prints as its JSON; an SDK error prints as {"__error": details} with the
 * human message added, and the driver keeps reading so bindings can be compared. */
#include "flare_im_core_sdk_ffi.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ready = PTHREAD_COND_INITIALIZER;
static bool done;

static void put_json_string(const char *text, size_t len) {
    fputc('"', stdout);
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)text[i];
        if (c == '"' || c == '\\') fprintf(stdout, "\\%c", c);
        else if (c < 0x20) fprintf(stdout, "\\u%04x", c);
        else fputc(c, stdout);
    }
    fputc('"', stdout);
}

static void on_result(void *ctx, const FlareError *error, FlareString result) {
    (void)ctx;
    pthread_mutex_lock(&lock);
    if (error) {
        /* details_json is an object: splice the message in before its closing brace. */
        const char *details = error->details_json.ptr;
        size_t len = error->details_json.len;
        while (len && details[len - 1] != '}') len--;
        fputs("{\"__error\":", stdout);
        if (len > 1) {
            fwrite(details, 1, len - 1, stdout);
            fputs(len > 2 ? ",\"message\":" : "\"message\":", stdout);
        } else {
            fprintf(stdout, "{\"cCode\":%d,\"message\":", error->code);
        }
        put_json_string(error->message.ptr, error->message.len);
        fputs("}}\n", stdout);
        fflush(stdout);
        flare_error_heap_free((FlareError *)error);
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
        if (rc) {
            /* Rejected before submission: no callback follows. */
            printf("{\"__error\":{\"cCode\":%d,\"code\":\"SUBMIT_REJECTED\",\"message\":\"submit rejected\"}}\n", rc);
            fflush(stdout);
            continue;
        }
        pthread_mutex_lock(&lock);
        while (!done) pthread_cond_wait(&ready, &lock);
        pthread_mutex_unlock(&lock);
    }
    if (subscription) flare_event_unsubscribe(subscription);
    free(line);
    flare_sdk_release(handle);
    return 0;
}

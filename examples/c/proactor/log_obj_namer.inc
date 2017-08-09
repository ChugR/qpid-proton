/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef PRINTF
#define PRINTF printf
#endif

/* log object namer - shorter names than addresses */
typedef struct log_obj_namer_s {
    struct log_obj_namer_s *next;
    char                   *name;
    void                   *obj_ptr;
    char                   *longname;
} log_obj_namer_t;

typedef struct log_obj_namers_s {
  pthread_mutex_t  lock;
  const char      *prefix;
  log_obj_namer_t *obj_namers;
  int              next_id;
} log_obj_namers_t;

log_obj_namers_t *log_obj_namers(const char *prefix) {
    log_obj_namers_t *lon = (log_obj_namers_t *)malloc(sizeof(log_obj_namers_t));
    if (lon == 0)
        fatal("failed to allocate namer for %s" , prefix);
    pthread_mutex_init(&lon->lock, NULL);
    lon->prefix = prefix;
    lon->obj_namers = 0;
    lon->next_id = 1;
    return lon;
}

void log_obj_namers_destroy(log_obj_namers_t *namer) {
    pthread_mutex_destroy(&namer->lock);
    // TODO:
}

/* Given a pointer, return the short name from List
 * or add a new name
 */
const char * log_obj_name_of(log_obj_namers_t *name_pool, void *ptr) {
    pthread_mutex_lock(&name_pool->lock);
    log_obj_namer_t *last = 0;
    log_obj_namer_t *curr =  name_pool->obj_namers;
    while (curr) {
        if (curr->obj_ptr == ptr) {
            break;
        }
        last = curr;
        curr = curr->next;
    }
    if (curr == 0) {
        curr = (log_obj_namer_t*)malloc(sizeof(log_obj_namer_t));
        curr->next = 0;
        curr->name = (char *)malloc(strlen(name_pool->prefix) + 20);  // "prefix-" + int-as-string
        if (ptr == (void*)0) {
            curr->name[0] = '\0';
        } else {
            strncpy(curr->name, name_pool->prefix, strlen(name_pool->prefix));
            sprintf(&curr->name[strlen(name_pool->prefix)], "%d", name_pool->next_id++);
        }
        curr->obj_ptr = ptr;
        if (last == 0)
            name_pool->obj_namers = curr;
        else
            last->next = curr;
    }
    pthread_mutex_unlock(&name_pool->lock);
    return curr->name;
}


/* Given a string, return the short name from List
 * or add a new name
 */
const char * log_obj_short_name_of(log_obj_namers_t *name_pool, const char* lname) {
    pthread_mutex_lock(&name_pool->lock);
    log_obj_namer_t *last = 0;
    log_obj_namer_t *curr =  name_pool->obj_namers;
    while (curr) {
        if (strcmp(curr->longname, lname) == 0) {
            break;
        }
        last = curr;
        curr = curr->next;
    }
    if (curr == 0) {
        curr = (log_obj_namer_t*)malloc(sizeof(log_obj_namer_t));
        curr->next = 0;
        curr->name = (char *)malloc(strlen(name_pool->prefix) + 20);  // "prefix-" + int-as-string
        if ((void*)lname == (void*)0) {
            curr->name[0] = '\0';
        } else {
            strncpy(curr->name, name_pool->prefix, strlen(name_pool->prefix));
            sprintf(&curr->name[strlen(name_pool->prefix)], "%d", name_pool->next_id++);
        }
        size_t len = strlen(lname) + 1;
        curr->longname = (char *)malloc(len);
        memmove(curr->longname, lname, len);
        if (last == 0)
            name_pool->obj_namers = curr;
        else
            last->next = curr;
        PRINTF(",MAPPED LINK NAME %s to long name %s\n", curr->name, curr->longname);
    }
    pthread_mutex_unlock(&name_pool->lock);
    return curr->name;
}


/* Event csv logger */
static pn_timestamp_t tboot = 0;
static log_obj_namers_t *log_transports = 0;
static log_obj_namers_t *log_sessions   = 0;
static log_obj_namers_t *log_links      = 0;
static log_obj_namers_t *log_linknames  = 0;
static log_obj_namers_t *log_deliveries = 0;


void log_this_init(void) {
    tboot = time_now();
    PRINTF("Time(S), note, event, "
        "xport, maxFrame, "
        "session, incoming_capacty, outgoing_window, incoming_bytes, outgoing_bytes, "
        "link, linkName, link_is_receiver, max_message_size, remote_max_message_size, current, credit, available, queued, unsettled\n");
    log_transports = log_obj_namers("xport-");
    log_sessions   = log_obj_namers("sessn-");
    log_links      = log_obj_namers("link-");
    log_linknames  = log_obj_namers("linkname-");
    log_deliveries = log_obj_namers("dlvry-");
}

void log_this(pn_event_t *event, const char * note) {
    pn_timestamp_t rtnow = time_now() - tboot;
    double drtnow = rtnow;
    drtnow /= 1000.0;

    const char * ename = pn_event_type_name(pn_event_type(event));

    pn_transport_t * xport = pn_event_transport(event);
    uint32_t max_frame = 0;
    if (!!xport) {
        max_frame = pn_transport_get_max_frame(xport);
    }
    
    pn_session_t * sessn = pn_event_session(event);
    size_t incoming_capacity = 0;
    size_t outgoing_window   = 0;
    size_t incoming_bytes    = 0;
    size_t outgoing_bytes    = 0;
    if (!!sessn) {
        incoming_capacity = pn_session_get_incoming_capacity(sessn);
        outgoing_window   = pn_session_get_outgoing_window(sessn);
        incoming_bytes    = pn_session_incoming_bytes(sessn);
        outgoing_bytes    = pn_session_outgoing_bytes(sessn);
    }
    
    pn_link_t * link = pn_event_link(event);
    const char * linkname      = "";
    bool is_rcvr                = true;
    const char * rcvr_status    = "";
    uint64_t max_message        = 0;
    uint64_t remote_max_message = 0;
    pn_delivery_t * current     = 0;
    int credit                  = 0;
    int available               = 0;
    int queued                  = 0;
    int unsettled               = 0;
    if (!!link) {
        linkname           = pn_link_name(link);
        is_rcvr            = pn_link_is_receiver(link);
        rcvr_status        = is_rcvr ? "recvr" : "sendr";
        max_message        = pn_link_max_message_size(link);
        remote_max_message = pn_link_remote_max_message_size(link);
        current            = pn_link_current(link);
        credit             = pn_link_credit(link);
        available          = pn_link_available(link);
        queued             = pn_link_queued(link);
        unsettled          = pn_link_unsettled(link);
    }
    
    PRINTF("%.3f, %s, %s, "
        "%s, %d, "
        "%s, %ld, %ld, %ld, %ld, "
        "%s, %s, %s, %lu, %lu, %s, %d, %d, %d, %d\n",
        drtnow, note, ename,
        log_obj_name_of(log_transports, (void*)xport), max_frame,
        log_obj_name_of(log_sessions,   (void*)sessn), incoming_capacity, outgoing_window, incoming_bytes, outgoing_bytes,
        log_obj_name_of(log_links,      (void*)link), 
        log_obj_short_name_of(log_linknames,  linkname), rcvr_status, max_message, remote_max_message, 
        log_obj_name_of(log_deliveries, (void*)current), credit, available, queued, unsettled
        );
}

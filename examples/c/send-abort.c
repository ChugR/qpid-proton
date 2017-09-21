/*
 *
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
 *
 */

#include <proton/connection.h>
#include <proton/condition.h>
#include <proton/delivery.h>
#include <proton/link.h>
#include <proton/message.h>
#include <proton/proactor.h>
#include <proton/session.h>
#include <proton/transport.h>

#include <stdio.h>
#include <stdlib.h>

/*
 * This hack sends aborted messages.
 * Each messages process:
 *  Generate a tag and a message data buffer
 *  Send half the data
 *  Wait for a Flow signalling that outbound_bytes has changed
 *  Abort the delivery
 * Command line arg 5 may be '0' to do original send
 *  to see if *that* still works.
 * Data is a biggish string that will exceed a frame size and
 *  hopefully get proton to push it so an abort is not on the first
 *  frame of a message.
 * Data gets sent in two chunks:
 *  From beginning, len=total-HOLDBACK
 *  From beginning+HOLDBACK, len=HOLDBACK
 */

#define STRING_MESSAGE_SIZE 80000

#define HOLDBACK 1000

typedef struct app_data_t {
  const char *host, *port;
  const char *amqp_address;
  const char *container_id;
  int message_count;

  pn_proactor_t *proactor;
  pn_rwbytes_t message_buffer;
  int sent;
  int acknowledged;
  int strategy;     // 0->do original thing, else->abort all messages
  bool in_progress;
  pn_bytes_t msgbuf;
} app_data_t;

static int exit_code = 0;

static void check_condition(pn_event_t *e, pn_condition_t *cond) {
  if (pn_condition_is_set(cond)) {
    fprintf(stderr, "%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),
            pn_condition_get_name(cond), pn_condition_get_description(cond));
    pn_connection_close(pn_event_connection(e));
    exit_code = 1;
  }
}

/* Create a message with a map { "sequence" : number } encode it and return the encoded buffer. */
static pn_bytes_t encode_message(app_data_t* app) {
  /* Construct a message with the map { "sequence": app.sent } */
  pn_message_t* message = pn_message();
  char ddd[STRING_MESSAGE_SIZE];
  for (size_t i=0; i<sizeof(ddd); i++) ddd[i] = '?';
  pn_data_put_int(pn_message_id(message), app->sent); /* Set the message_id also */
  pn_data_t* body = pn_message_body(message);
  pn_data_enter(body);
  pn_data_put_string(body, pn_bytes(sizeof(ddd), ddd));
  pn_data_exit(body);

  /* encode the message, expanding the encode buffer as needed */
  if (app->message_buffer.start == NULL) {
    static const size_t initial_size = STRING_MESSAGE_SIZE + 1000;
    app->message_buffer = pn_rwbytes(initial_size, (char*)malloc(initial_size));
  }
  /* app->message_buffer is the total buffer space available. */
  /* mbuf wil point at just the portion used by the encoded message */
  pn_rwbytes_t mbuf = pn_rwbytes(app->message_buffer.size, app->message_buffer.start);
  int status = 0;
  while ((status = pn_message_encode(message, mbuf.start, &mbuf.size)) == PN_OVERFLOW) {
    app->message_buffer.size *= 2;
    app->message_buffer.start = (char*)realloc(app->message_buffer.start, app->message_buffer.size);
    mbuf.size = app->message_buffer.size;
  }
  if (status != 0) {
    fprintf(stderr, "error encoding message: %s\n", pn_error_text(pn_message_error(message)));
    exit(1);
  }
  pn_message_free(message);
  return pn_bytes(mbuf.size, mbuf.start);
}

/* Returns true to continue, false if finished */
static bool handle(app_data_t* app, pn_event_t* event) {
  switch (pn_event_type(event)) {

   case PN_CONNECTION_INIT: {
     pn_connection_t* c = pn_event_connection(event);
     pn_connection_set_container(c, app->container_id);
     pn_connection_open(c);
     pn_session_t* s = pn_session(pn_event_connection(event));
     pn_session_open(s);
     pn_link_t* l = pn_sender(s, "my_sender");
     pn_terminus_set_address(pn_link_target(l), app->amqp_address);
     pn_link_open(l);
     break;
   }

   case PN_LINK_FLOW: {
     /* The peer has given us some credit, now we can send messages */
     pn_link_t *sender = pn_event_link(event);
     printf("Link Flow event: pn_link_credit(sender)=%d, sent=%d, msg_count=%d\n", pn_link_credit(sender), app->sent, app->message_count);
    while (app->in_progress || (pn_link_credit(sender) > 0 && app->sent < app->message_count)) {
        if (!app->in_progress) {
            // Use sent counter as unique delivery tag.
            pn_delivery(sender, pn_dtag((const char *)&app->sent, sizeof(app->sent)));
            pn_delivery_t * pnd = pn_link_current(sender);
            printf("Link flow first part. delivery at %p\n", (void*)pnd);
            app->msgbuf = encode_message(app);
            pn_link_send(sender, app->msgbuf.start, app->msgbuf.size - HOLDBACK); // Don't send it all
            app->in_progress = true;
            break;
        } else {
            pn_delivery_t * pnd = pn_link_current(sender);
            printf("Link flow second part. delivery at %p\n", (void*)pnd);
            if (app->strategy == 0) {
                // This sends the message like normal
                pn_link_send(sender, app->msgbuf.start + HOLDBACK, HOLDBACK);
                pn_link_advance(sender);
            } else {
                // Here we get devious
                pn_delivery_abort(pnd);
                // aborted delivery are presettled and never ack'd. So pretend that happened here.
                if (++app->acknowledged == app->message_count) {
                    printf("%d messages sent and aborted\n", app->acknowledged);
                    // Testing: don't close connection. Let everything dribble out onto the wire
                    //pn_connection_close(pn_event_connection(event));

                    /* Continue handling events till we receive TRANSPORT_CLOSED */
                }
            }
            ++app->sent;
            app->in_progress = false;
        }
    }
     break;
   }

   case PN_DELIVERY: {
     /* We received acknowledgedment from the peer that a message was delivered. */
     pn_delivery_t* d = pn_event_delivery(event);
     if (pn_delivery_remote_state(d) == PN_ACCEPTED) {
       if (++app->acknowledged == app->message_count) {
         printf("%d messages sent and acknowledged\n", app->acknowledged);
         pn_connection_close(pn_event_connection(event));
         /* Continue handling events till we receive TRANSPORT_CLOSED */
       }
     } else {
       fprintf(stderr, "unexpected delivery state %d\n", (int)pn_delivery_remote_state(d));
       pn_connection_close(pn_event_connection(event));
       exit_code=1;
     }
     break;
   }

   case PN_TRANSPORT_CLOSED:
    check_condition(event, pn_transport_condition(pn_event_transport(event)));
    break;

   case PN_CONNECTION_REMOTE_CLOSE:
    check_condition(event, pn_connection_remote_condition(pn_event_connection(event)));
    pn_connection_close(pn_event_connection(event));
    break;

   case PN_SESSION_REMOTE_CLOSE:
    check_condition(event, pn_session_remote_condition(pn_event_session(event)));
    pn_connection_close(pn_event_connection(event));
    break;

   case PN_LINK_REMOTE_CLOSE:
   case PN_LINK_REMOTE_DETACH:
    check_condition(event, pn_link_remote_condition(pn_event_link(event)));
    pn_connection_close(pn_event_connection(event));
    break;

   case PN_PROACTOR_INACTIVE:
    return false;

   default: break;
  }
  return true;
}

void run(app_data_t *app) {
  /* Loop and handle events */
  do {
    pn_event_batch_t *events = pn_proactor_wait(app->proactor);
    for (pn_event_t *e = pn_event_batch_next(events); e; e = pn_event_batch_next(events)) {
      if (!handle(app, e)) {
        return;
      }
    }
    pn_proactor_done(app->proactor, events);
  } while(true);
}

int main(int argc, char **argv) {
  struct app_data_t app = {0};
  app.container_id = argv[0];   /* Should be unique */
  app.host = (argc > 1) ? argv[1] : "";
  app.port = (argc > 2) ? argv[2] : "amqp";
  app.amqp_address = (argc > 3) ? argv[3] : "jms.queue.qpid-interop.abort-test";
  app.message_count = (argc > 4) ? atoi(argv[4]) : 10;
  app.strategy = (argc > 5) ? atoi(argv[5]) : 1; // default to abort, specify 0 to do original

  app.proactor = pn_proactor();
  char addr[PN_MAX_ADDR];
  pn_proactor_addr(addr, sizeof(addr), app.host, app.port);
  pn_proactor_connect(app.proactor, pn_connection(), addr);
  run(&app);
  pn_proactor_free(app.proactor);
  free(app.message_buffer.start);
  return exit_code;
}

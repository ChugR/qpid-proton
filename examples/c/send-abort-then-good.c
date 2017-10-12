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
#include <proton/disposition.h>

#include <stdio.h>
#include <stdlib.h>

/*
 * Send aborted messages.
 * 
 * An N-byte (MSG_SIZE) message is generated.
 * If size is chosen to be large enough then some of the message
 * will go out on the wire before the abort is sent.
 * 
 * (MSG_SIZE - HOLDBACK) bytes are sent on one link flow event
 * then the message is aborted on the next link flow event.
 * 
 * Good messages are sent in two chunks, too.
 */

#define DEFAULT_MSG_SIZE  20000
#define HOLDBACK          10

typedef struct app_data_t {
  const char *host, *port;
  const char *amqp_address;
  const char *container_id;
  int message_size;
  int message_count;
  int then_send_n_good;

  pn_proactor_t *proactor;
  pn_rwbytes_t message_buffer;
  int sent;
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
  int i;
  pn_data_t* body;
  char *data = (char*)malloc(app->message_size + 11);;
  for (i=0; i<app->message_size; i+=10) 
      sprintf(&data[i], "<%09d", i);
  pn_data_put_int(pn_message_id(message), app->sent); /* message_id */
  body = pn_message_body(message);
  pn_data_enter(body);
  pn_data_put_string(body, pn_bytes(app->message_size, data)); /* message body */
  pn_data_exit(body);

  /* encode the message, expanding the encode buffer as needed */
  if (app->message_buffer.start == NULL) {
    size_t initial_size = app->message_size + 1000;
    app->message_buffer = pn_rwbytes(initial_size, (char*)malloc(initial_size));
  }
  /* app->message_buffer is the total buffer space available. */
  /* mbuf wil point at just the portion used by the encoded message */
  {
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
}

/* Returns true to continue, false if finished */
static bool handle(app_data_t* app, pn_event_t* event) {
  switch (pn_event_type(event)) {

   case PN_CONNECTION_INIT: {
     pn_connection_t* c = pn_event_connection(event);
     pn_session_t* s = pn_session(pn_event_connection(event));
     pn_connection_set_container(c, app->container_id);
     pn_connection_open(c);
     pn_session_open(s);
     {
     pn_link_t* l = pn_sender(s, "my_sender");
     pn_terminus_set_address(pn_link_target(l), app->amqp_address);
     pn_link_open(l);
     break;
     }
   }

   case PN_LINK_FLOW: {
     /* The peer has given us some credit, now we can send messages */
     pn_link_t *sender = pn_event_link(event);
     int total_to_send = app->message_count + app->then_send_n_good;
     while (app->in_progress || (pn_link_credit(sender) > 0 && app->sent < total_to_send)) {
        if (!app->in_progress) {
          app->msgbuf = encode_message(app);
          /* Use sent counter as unique delivery tag. */
          pn_delivery(sender, pn_dtag((const char *)&app->sent, sizeof(app->sent)));
          pn_link_send(sender, app->msgbuf.start, app->msgbuf.size - HOLDBACK); /* Send some part of message */
          app->in_progress = true;
          /* Return from this link flow event and abort or complete the message on next, */
          break;
        } else {
          if (app->sent < app->message_count) {
            /* sending a batch of aborted messages */
            pn_delivery_t * pnd = pn_link_current(sender);
            pn_delivery_abort(pnd);
            /* aborted delivery is presettled and never ack'd. */
            if (app->sent + 1 == app->message_count) {
                printf("%d messages started and aborted\n", app->message_count);
            }
          } else {
            /* sending some good messages after the aborted batch */
            pn_link_send(sender, app->msgbuf.start + app->msgbuf.size - HOLDBACK, HOLDBACK);
            pn_link_advance(sender);
            if (app->sent + 1 == total_to_send) {
                printf("%d messages started and completed\n", app->then_send_n_good);
            }
          }
          ++app->sent;
          app->in_progress = false;
          if (app->sent == total_to_send) {
            pn_connection_close(pn_event_connection(event));
          }
        }
     }
     break;
   }

   case PN_DELIVERY: {
     /* We received acknowledgedment from the peer that a message was delivered. */
     pn_delivery_t* d = pn_event_delivery(event);
     if (pn_delivery_aborted(d)) {
        fprintf(stderr, "Aborted deliveries should not receive delivery events. Delivery state %d : %s\n", (int)pn_delivery_remote_state(d), pn_disposition_type_name(pn_delivery_remote_state(d)));
        pn_connection_close(pn_event_connection(event));
        exit_code=1;
        break;
     } else {
        if (pn_delivery_remote_state(d) != PN_ACCEPTED) {
            fprintf(stderr, "delivery not accepted. state %d : %s.\n", (int)pn_delivery_remote_state(d), pn_disposition_type_name(pn_delivery_remote_state(d)));
            pn_connection_close(pn_event_connection(event));
            exit_code=1;
            break;
        }
     }
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
    pn_event_t* e;
    for (e = pn_event_batch_next(events); e; e = pn_event_batch_next(events)) {
      if (!handle(app, e)) {
        return;
      }
    }
    pn_proactor_done(app->proactor, events);
  } while(true);
}

int main(int argc, char **argv) {
  struct app_data_t app = {0};
  char addr[PN_MAX_ADDR];

  app.container_id     =                   argv[0];   /* Should be unique */
  app.host             = (argc > 1) ?      argv[1] : "";
  app.port             = (argc > 2) ?      argv[2] : "amqp";
  app.amqp_address     = (argc > 3) ?      argv[3] : "examples";
  app.message_size     = (argc > 4) ? atoi(argv[4]) : DEFAULT_MSG_SIZE;
  app.message_count    = (argc > 4) ? atoi(argv[5]) : 10;
  app.then_send_n_good = (argc > 5) ? atoi(argv[6]) : 0;

  if (app.message_count < 0) app.message_count = 10;
  if (app.then_send_n_good < 0) app.then_send_n_good = 0;
  if (app.message_count + app.then_send_n_good == 0) return exit_code;
  if (app.message_size < HOLDBACK) {
      printf("Messge size must be > %d.\n", HOLDBACK);
      return 1;
  }

  app.proactor = pn_proactor();
  pn_proactor_addr(addr, sizeof(addr), app.host, app.port);
  pn_proactor_connect(app.proactor, pn_connection(), addr);
  run(&app);
  pn_proactor_free(app.proactor);
  free(app.message_buffer.start);
  return exit_code;
}

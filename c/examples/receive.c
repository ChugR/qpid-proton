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

/*
 * hacked to trigger stream of policy link denials on one connection/
 * session.
 *              host      port addr n-tries print-every-n
 * $bin/receive 127.0.0.1 5672 xyz  1000000 1000
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

typedef struct app_data_t {
  const char *host, *port;
  const char *amqp_address;
  const char *container_id;
  int count;
  int n_opened;
  int n_detached;
  int n_detached_w_condition;
  int print_upcount;
  int print_every_n;

  pn_proactor_t *proactor;
} app_data_t;

static int exit_code = 0;

/* Return if-condition-is-set */
static bool check_condition(pn_event_t *e, pn_condition_t *cond) {
  bool isset = false;
  if (pn_condition_is_set(cond)) {
    fprintf(stderr, "%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),
            pn_condition_get_name(cond), pn_condition_get_description(cond));
    isset = true;
  }
  return isset;
}

/* Return true to continue, false to exit */
static bool handle(app_data_t* app, pn_event_t* event) {
  /*
   * fprintf(stderr, "EVENT: %s\n", pn_event_type_name(pn_event_type(event)));
   */
  switch (pn_event_type(event)) {

   case PN_CONNECTION_INIT: {
     pn_connection_t* c = pn_event_connection(event);
     pn_session_t* s = pn_session(c);
     pn_connection_set_container(c, app->container_id);
     pn_connection_open(c);
     pn_session_open(s);
     {
     pn_link_t* l = pn_receiver(s, "my_receiver");
     pn_terminus_set_address(pn_link_source(l), app->amqp_address);
     pn_link_open(l);
     app->n_opened++;
     }
   } break;

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
    app->n_detached++;
    pn_link_detach(pn_event_link(event));
    pn_link_close(pn_event_link(event));
    pn_link_free(pn_event_link(event));
    {
    pn_condition_t *cond = pn_link_remote_condition(pn_event_link(event));
    if (pn_condition_is_set(cond)) {
        /* Remote link detached. Expected. Close existing link.*/
        app->n_detached_w_condition++;
        app->print_upcount++;
        if (app->print_upcount == app->print_every_n) {
            app->print_upcount = 0;
            fprintf(stderr, "Detached link %d: %s: %s\n", app->n_detached, 
                    pn_condition_get_name(cond), pn_condition_get_description(cond));
        }
    } else {
        fprintf(stderr, "Detached with no error??? link number: %d", app->n_detached);
    }
    }
    break;

   /* case PN_LINK_LOCAL_CLOSE: */
   case PN_LINK_FINAL:
    if (app->n_opened < app->count) {
        pn_link_t* l = pn_receiver(pn_event_session(event), "my_receiver");
        pn_terminus_set_address(pn_link_source(l), app->amqp_address);
        pn_link_open(l);
        app->n_opened++;
    } else {
        pn_connection_close(pn_event_connection(event));
    }
    break;

   case PN_LINK_REMOTE_DETACH:
    check_condition(event, pn_link_remote_condition(pn_event_link(event)));
    pn_connection_close(pn_event_connection(event));
    break;


   case PN_PROACTOR_INACTIVE:
    return false;
    break;

   default:
    break;
  }
    return true;
}

void run(app_data_t *app) {
  /* Loop and handle events */
  do {
    pn_event_batch_t *events = pn_proactor_wait(app->proactor);
    pn_event_t *e;
    for (e = pn_event_batch_next(events); e; e = pn_event_batch_next(events)) {
      if (!handle(app, e) || exit_code != 0) {
        return;
      }
    }
    pn_proactor_done(app->proactor, events);
  } while(true);
}

int main(int argc, char **argv) {
  struct app_data_t app = {0};
  char addr[PN_MAX_ADDR];

  app.container_id = argv[0];   /* Should be unique */
  app.host = (argc > 1) ? argv[1] : "";
  app.port = (argc > 2) ? argv[2] : "amqp";
  app.amqp_address = (argc > 3) ? argv[3] : "examples";
  app.count = (argc > 4) ? atoi(argv[4]) : 10;
  app.print_every_n = (argc > 5) ? atoi(argv[5]) : 1;

  /* Create the proactor and connect */
  app.proactor = pn_proactor();
  pn_proactor_addr(addr, sizeof(addr), app.host, app.port);
  pn_proactor_connect2(app.proactor, NULL, NULL, addr);
  run(&app);
  pn_proactor_free(app.proactor);
  return exit_code;
}

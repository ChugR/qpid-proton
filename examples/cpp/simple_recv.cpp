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

#include "options.hpp"

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/default_container.hpp>
#include <proton/delivery.hpp>
#include <proton/link.hpp>
#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/thread_safe.hpp>
#include <proton/value.hpp>

#include <iostream>
#include <map>
#include <ctime>

#include "fake_cpp11.hpp"

// If no progress in INTERVAL_SEC * N_INTERVALS then print number rcvd and exit
// At startup, when received == 0, wait longer
#define INTERVAL_SEC         2
#define N_INTERVALS          5
#define N_INTERVALS_STARTUP 30

class simple_recv : public proton::messaging_handler {
  private:
    std::string url;
    std::string user;
    std::string password;
    proton::receiver receiver;
    proton::duration interval;
    int expected;
    int received;
    int last_rcvd;
    int stuck_intervals;

  public:
    simple_recv(const std::string &s, const std::string &u, const std::string &p, int c) :
        url(s), user(u), password(p), 
        interval(int(INTERVAL_SEC*proton::duration::SECOND.milliseconds())), // Check interval
        expected(c), received(0), last_rcvd(0), stuck_intervals(0) {}

    void on_container_start(proton::container &c) OVERRIDE {
        proton::connection_options co;
        if (!user.empty()) co.user(user);
        if (!password.empty()) co.password(password);
        receiver = c.open_receiver(url, co);
        std::cout << "simple_recv listening on " << url << std::endl;
         // Start regular ticks every interval.
        c.schedule(interval, [this]() { this->tick(); });
    }

    void on_message(proton::delivery &d, proton::message &msg) OVERRIDE {
        if (proton::coerce<int>(msg.id()) < received) {
            return; // Ignore duplicate
        }

        if (expected == 0 || received < expected) {
            //std::cout << msg.body() << std::endl;
            received++;

            if (received == expected) {
                d.receiver().close();
                d.connection().close();
            }
        }
    }
    
    std::string getTime() {
        time_t rawtime;
        struct tm *timeinfo;
        char buffer[80];
        time(&rawtime);
        timeinfo = localtime(&rawtime);
        strftime(buffer, sizeof(buffer), "%Y-%m-%d %I:%M:%S", timeinfo);
        std::string str(buffer);
        return str;
    }
        
    void tick() {
        bool done = received == expected;
        bool progress = received != last_rcvd;
        bool stuck = false;
        if (progress) {
            stuck_intervals = 0;
        } else {
            stuck_intervals += 1;
            stuck = stuck_intervals == (received == 0 ? N_INTERVALS_STARTUP : N_INTERVALS);
        }
        // Schedule the next tick only if we are not done and not stuck
        if (!done and !stuck)
            receiver.container().schedule(interval, [this]() { this->tick(); });
        // Report where we're at
        std::cout << getTime() << " Messages received: " << received << std::endl;
        // Exit if stuck
        if (stuck) {
            std::cout << getTime() << " Progress is stuck at message: " << received << std::endl;
            receiver.connection().close();
        }
        // Remember where we were
        last_rcvd = received;
    }
};

int main(int argc, char **argv) {
    std::string address("127.0.0.1:5672/examples");
    std::string user;
    std::string password;
    int message_count = 100;
    example::options opts(argc, argv);

    opts.add_value(address, 'a', "address", "connect to and receive from URL", "URL");
    opts.add_value(message_count, 'm', "messages", "receive COUNT messages", "COUNT");
    opts.add_value(user, 'u', "user", "authenticate as USER", "USER");
    opts.add_value(password, 'p', "password", "authenticate with PASSWORD", "PASSWORD");


    try {
        opts.parse();

        simple_recv recv(address, user, password, message_count);
        proton::default_container(recv).run();

        return 0;
    } catch (const example::bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}

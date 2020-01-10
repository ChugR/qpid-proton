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
#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/tracker.hpp>
#include <proton/types.hpp>

#include <iostream>
#include <map>

#include "fake_cpp11.hpp"
#include <chrono>

#define TICK_INTERVAL 1000

std::string getCurrentTimestamp() {
    using std::chrono::system_clock;
    auto currentTime = std::chrono::system_clock::now();
    char buf1[80];
    char buf2[80];
    auto transformed = currentTime.time_since_epoch().count() / 1000;
    auto usec = transformed % 1000000;
    std::time_t tt;
    tt = system_clock::to_time_t ( currentTime );
    auto timeinfo = localtime ( &tt );
    strftime (buf1, 80, "%F %H:%M:%S", timeinfo);
    sprintf(buf2, "%s.%06d", buf1, (int)usec);
    return std::string(buf2);
}

class simple_send : public proton::messaging_handler {
  private:
    std::string url;
    std::string user;
    std::string password;
    proton::sender sender;
    int sent;
    int confirmed;
    int total;
    bool tick;
    std::string id;
    int accepted;
    int rejected;
    int released;

  public:
    simple_send(const std::string &s, const std::string &u, const std::string &p, int c, bool t, const std::string &i) :
        url(s), user(u), password(p), sent(0), confirmed(0), total(c), tick(t), id(i), accepted(0), rejected(0), released(0) {}

    void ticktock() {
        std::cout << getCurrentTimestamp() << " Sent: " << sent << ", Confirmed: " << confirmed << 
            ", Accepted: " << accepted << ", Rejected: " <<
            rejected << ", Released: " << released << std::endl;
    }

    void on_container_start(proton::container &c) OVERRIDE {
        proton::connection_options co;
        if (!user.empty()) co.user(user);
        if (!password.empty()) co.password(password);
        sender = c.open_sender(url, co);
    }

    void on_connection_open(proton::connection& c) OVERRIDE {
        if (c.reconnected()) {
            sent = confirmed;   // Re-send unconfirmed messages after a reconnect
        }
    }

    void on_sendable(proton::sender &s) OVERRIDE {
        while (s.credit() && sent < total) {
            proton::message msg;
            int nsent = sent + 1;
            std::string m = id + std::to_string(nsent);

            msg.id(nsent);
            msg.body(m);

            s.send(msg);
            sent++;
            if (tick && (sent % TICK_INTERVAL) == 0) {
                ticktock();
            }
        }
    }

    void on_tracker_accept(proton::tracker &t) OVERRIDE {
        confirmed++;
        accepted++;

        if (confirmed == total) {
            std::cout << "all messages confirmed" << std::endl;
            t.connection().close();
            ticktock();
        } else {
            if (tick && (confirmed % TICK_INTERVAL) == 0) {
                ticktock();
            }
        }
    }

    void on_tracker_reject(proton::tracker &t) OVERRIDE {
        confirmed++;
        rejected++;

        if (confirmed == total) {
            std::cout << "all messages confirmed" << std::endl;
            t.connection().close();
            ticktock();
        } else {
            if (tick && (confirmed % TICK_INTERVAL) == 0) {
                ticktock();
            }
        }
    }

    void on_tracker_release(proton::tracker &t) OVERRIDE {
        confirmed++;
        released++;

        if (confirmed == total) {
            std::cout << "all messages confirmed" << std::endl;
            t.connection().close();
            ticktock();
        } else {
            if (tick && (confirmed % TICK_INTERVAL) == 0) {
                ticktock();
            }
        }
    }

    void on_transport_close(proton::transport &) OVERRIDE {
        sent = confirmed;
    }
};

int main(int argc, char **argv) {
    std::string address("127.0.0.1:5672/examples");
    std::string user;
    std::string password;
    int message_count = 100;
    bool ticks;
    std::string idprefix;
    example::options opts(argc, argv);

    opts.add_value(      address, 'a', "address",       "connect and send to URL",                   "URL");
    opts.add_value(message_count, 'm', "messages",      "send COUNT messages",                       "COUNT");
    opts.add_value(         user, 'u', "user",          "authenticate as USER",                      "USER");
    opts.add_value(     password, 'p', "password",      "authenticate with PASSWORD",                "PASSWORD");
    opts.add_flag(         ticks, 't', "ticks-inhibit", "do not print progress every 1000th message");
    opts.add_value(     idprefix, 'i', "id-prefix",     "content identifying prefix",                "IDPREFIX");

    try {
        opts.parse();
        simple_send send(address, user, password, message_count, !ticks, idprefix);
        proton::container(send).run();

        return 0;
    } catch (const example::bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}

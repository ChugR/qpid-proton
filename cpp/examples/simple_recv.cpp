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
#include <proton/delivery.hpp>
#include <proton/link.hpp>
#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/value.hpp>

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

class simple_recv : public proton::messaging_handler {
  private:
    std::string url;
    std::string user;
    std::string password;
    proton::receiver receiver;
    int expected;
    int received;
    bool tick;
    int credit;

  public:
    simple_recv(const std::string &s, const std::string &u, const std::string &p, int c, bool t, int cr) :
        url(s), user(u), password(p), expected(c), received(0), tick(t), credit(cr) {}

    void ticktock() {
        std::cout << getCurrentTimestamp() << " Received: " << received << std::endl;
    }

    void on_container_start(proton::container &c) OVERRIDE {
        proton::connection_options co;
        if (!user.empty()) co.user(user);
        if (!password.empty()) co.password(password);
        if (credit <= 0) credit = 10;
        receiver = c.open_receiver(url, proton::receiver_options().credit_window(credit), co);
    }

    void on_message(proton::delivery &d, proton::message &msg) OVERRIDE {
        //if (!msg.id().empty() && proton::coerce<int>(msg.id()) < received) {
        //    return; // Ignore if no id or duplicate
        //}
        received++;

        if (tick && (received % TICK_INTERVAL) == 0) {
            ticktock();
        }

        if (expected == 0 || received < expected + 1) {
            // another flag to turn this on: std::cout << msg.body() << std::endl;

            if (received == expected) {
                d.receiver().close();
                d.connection().close();
                ticktock();
            }
        }
    }
};

int main(int argc, char **argv) {
    std::string address("127.0.0.1:5672/examples");
    std::string user;
    std::string password;
    int message_count = 100;
    bool ticks;
    int credit = 10;
    example::options opts(argc, argv);

    opts.add_value(address,       'a', "address",       "connect to and receive from URL",           "URL");
    opts.add_value(message_count, 'm', "messages",      "receive COUNT messages",                    "COUNT");
    opts.add_value(user,          'u', "user",          "authenticate as USER",                      "USER");
    opts.add_value(password,      'p', "password",      "authenticate with PASSWORD",                "PASSWORD");
    opts.add_flag(ticks,          't', "ticks-inhibit", "do not print progress every 1000th message");
    opts.add_value(credit,        'c', "credit",        "initial credit and auto refresh",           "CREDIT");

    try {
        opts.parse();

        simple_recv recv(address, user, password, message_count, !ticks, credit);
        proton::container(recv).run();

        return 0;
    } catch (const example::bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}

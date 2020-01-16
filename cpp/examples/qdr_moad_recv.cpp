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
 * This program creates a zillion receivers with different addresses.
 * Connection
 *   host:port
 * Addresses are constructed:
 *   prefix      - a name for the run and to identify multiple runs
 *   NNNNNNNN    - 0..10,000,000 uniquifier for this run
 *   xxxx...     - padding to make the address the right length
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
#include <proton/value.hpp>

#include <iostream>
#include <map>
#include <string>
#include <algorithm>

#include "fake_cpp11.hpp"

class moad_recv : public proton::messaging_handler {
  private:
    std::string url;
    std::string user;
    std::string password;
    std::string a_prefix;
    int a_length;
    int a_count;
    proton::connection connection;
    std::vector<proton::receiver> receivers;
    int expected;
    int received;

  public:
    moad_recv(const std::string &s, const std::string &u, const std::string &p, int c, const std::string &ap, int al, int ac) :
        url(s), user(u), password(p), a_prefix(ap), a_length(al), a_count(ac), expected(c), received(0) {}

    void on_container_start(proton::container &c) OVERRIDE {
        proton::connection_options co;
        if (!user.empty()) co.user(user);
        if (!password.empty()) co.password(password);
        connection = c.connect(url, co);
    }

    void on_connection_open(proton::connection & conn) OVERRIDE {
        // a_length must be long enough to hold prefix and 8 char uniquifier
        a_length = std::max(a_length, ((int)a_prefix.length() + 8));
        int padlen = a_length - (int)a_prefix.length() - 8;
        std::string pad(padlen, 'x');
        if (a_count <= 0) a_count = 1;
        std::cout << "Creating " << a_count << " receivers with prefix '" << a_prefix << "' and length " << a_length << std::endl;
        
        // Loop to create the receivers.
        // This loop loads proton's work queue with things to do.
        // Nothing happens until the loop exits and proton regains control.
        for (int i=0; i<a_count; i++) {
            // unique address based on receiver number
            char buf[20];
            sprintf(buf, "%08d", i);
            // generate address
            std::string address(a_prefix);
            address.append(std::string(buf));
            address.append(pad);
            receivers.push_back(conn.open_receiver(address));
            if (i % 100 == 0) {
                std::cout << "Created receiver " << i << std::endl;
            }
        }
        std::cout << "Created " << a_count << " receivers" << std::endl;
    }
    
    void on_message(proton::delivery &d, proton::message &msg) OVERRIDE {
        if (!msg.id().empty() && proton::coerce<int>(msg.id()) < received) {
            return; // Ignore if no id or duplicate
        }

        if (expected == 0 || received < expected) {
            std::cout << msg.body() << std::endl;
            received++;

            if (received == expected) {
                d.receiver().close();
                d.connection().close();
            }
        }
    }
};

int main(int argc, char **argv) {
    std::string hostport("127.0.0.1:5672");
    std::string user;
    std::string password;
    int message_count = 100;
    std::string a_prefix;
    int a_length = 1000;
    int a_count = 1000;
    example::options opts(argc, argv);

    opts.add_value(hostport, 'h', "hostport", "connect to hostport", "HOSTPORT");
    opts.add_value(message_count, 'm', "messages", "receive COUNT messages", "COUNT");
    opts.add_value(user, 'u', "user", "authenticate as USER", "USER");
    opts.add_value(password, 'p', "password", "authenticate with PASSWORD", "PASSWORD");
    opts.add_value(a_prefix, 'x', "prefix", "leading address text to avoid multi-run name collisions", "APREFIX");
    opts.add_value(a_length, 'l', "length", "address name length", "ALENGTH");
    opts.add_value(a_count, 'c', "count", "number of addresses to create", "ACOUNT");


    try {
        opts.parse();

        moad_recv recv(hostport, user, password, message_count, a_prefix, a_length, a_count);
        proton::container(recv).run();

        return 0;
    } catch (const example::bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}


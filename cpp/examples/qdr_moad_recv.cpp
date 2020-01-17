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
#include <chrono>

#include "fake_cpp11.hpp"

std::string tod() {
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

class moad_recv : public proton::messaging_handler {
  private:
    std::string bus;
    std::string user;
    std::string password;
    std::string a_prefix;
    int         a_length;
    int         a_count;
    int         a_in_flight;
    int         n_open;
    int         n_in_flight;
    std::string pad;
    std::vector<proton::receiver> receivers;

  public:
    moad_recv(const std::string &b, const std::string &u, const std::string &p, const std::string &ap,       int al,      int ac,          int ai) :
                             bus(b),              user(u),          password(p),          a_prefix(ap), a_length(al), a_count(ac), a_in_flight(ai), n_open(0), n_in_flight(0) {}

    void on_container_start(proton::container &c) OVERRIDE {
        std::cout << tod() << " on_container_start: opening connection" << std::endl;
        proton::connection_options co;
        if (!user.empty()) co.user(user);
        if (!password.empty()) co.password(password);
        (void) c.connect(bus, co);

        a_length = std::max(a_length, ((int)a_prefix.length() + 8));
        int padlen = a_length - (int)a_prefix.length() - 8;
        pad = std::string(padlen, 'x');
        if (a_count <= 0) a_count = 1;
        std::cout << tod() << " Creating " << a_count << " receivers with prefix '" << a_prefix << "' and length " << a_length << std::endl;
    }
    
    void more_receivers(proton::connection & conn) {
        // keep proton loaded with in-flight receiver creation requests
        while (( a_count > (n_in_flight + n_open)) && (a_in_flight > n_in_flight)) {
            n_in_flight++;
            int serialno = n_in_flight + n_open;
            char buf[20];
            sprintf(buf, "%08d", serialno);
            // generate address
            std::string address(a_prefix);
            address.append(std::string(buf));
            address.append(pad);
            receivers.push_back(conn.open_receiver(address));
            if (serialno % 100 == 0) {
                std::cout << tod() << " N receivers queued : " << serialno << std::endl;
            }
            if (serialno == a_count) {
                std::cout << tod() << " Requested all " << a_count << " receivers" << std::endl;
            }
        }
    }
        

    void on_connection_open(proton::connection & conn) OVERRIDE {
        more_receivers(conn);
    }

    void on_receiver_open(proton::receiver & rcvr) OVERRIDE {
        if (n_open == 0) {
            std::cout << tod() << " on_receiver_open: First receiver opened." << std::endl;
        }
        n_open++;
        n_in_flight--;
        proton::connection c = rcvr.connection();
        more_receivers(c);
        if (n_open % 100 == 0) {
            std::cout << tod() << " N receivers open   : " << n_open << std::endl;
        }
        if (n_open == a_count) {
            std::cout << tod() << " All receivers are open." << std::endl;
        }
    }
};

int main(int argc, char **argv) {
    std::string bus("127.0.0.1:5672");
    std::string user;
    std::string password;
    std::string a_prefix("moad_");
    int a_length = 1000;
    int a_count = 1000;
    int a_in_flight = 100;
    example::options opts(argc, argv);

    opts.add_value(          bus, 'b', "bus",      "connect to bus host:port",                                "BUS");
    opts.add_value(         user, 'u', "user",     "authenticate as USER",                                    "USER");
    opts.add_value(     password, 'p', "password", "authenticate with PASSWORD",                              "PASSWORD");
    opts.add_value(     a_prefix, 'x', "prefix",   "leading address text to avoid multi-run name collisions", "APREFIX");
    opts.add_value(     a_length, 'l', "length",   "address name length",                                     "ALENGTH");
    opts.add_value(      a_count, 'c', "count",    "number of addresses to create",                           "ACOUNT");
    opts.add_value(  a_in_flight, 'i', "inflight", "number of in-flight receiver creations",                  "AINFLIGHT");

    try {
        opts.parse();

        moad_recv recv(bus, user, password, a_prefix, a_length, a_count, a_in_flight);
        proton::container(recv).run();

        return 0;
    } catch (const example::bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}


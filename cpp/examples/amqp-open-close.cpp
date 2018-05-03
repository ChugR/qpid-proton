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

// HACK ALERT: not hello world. is open/close demon
//
// helloworld //127.0.0.1:5672 1000000
//            url              connection open count

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/tracker.hpp>

#include <iostream>

#include "fake_cpp11.hpp"

class hello_world : public proton::messaging_handler {
    std::string conn_url_;
    std::string addr_;

  public:
    hello_world(const std::string& u, const std::string& a) :
        conn_url_(u), addr_(a) {}

    void on_container_start(proton::container& c) OVERRIDE {
        c.connect(conn_url_);
    }

    void on_connection_open(proton::connection& c) OVERRIDE {
        c.close();
    }
};

int main(int argc, char **argv) {
    try {
        std::string conn_url = argc > 1 ? argv[1]       : "//127.0.0.1:5672";
        int count            = argc > 2 ? atoi(argv[2]) : 1000000;

        for (int i=0; i<count; i++) {
            hello_world hw(conn_url, "");
            proton::container(hw).run();
            if (i % 1000 == 0) {
                std::cout << "processed: " << i << std::endl;
            }
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}

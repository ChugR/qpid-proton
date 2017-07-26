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
#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/thread_safe.hpp>
#include <proton/tracker.hpp>
#include <proton/types.hpp>

#include <iostream>
#include <map>
#include <sstream>
#include <iomanip>
#include <vector>

#include "fake_cpp11.hpp"

// If no progress in INTERVAL_SEC * N_INTERVALS then print number processed and exit
// At startup, when processed == 0, wait longer
#define INTERVAL_SEC        10
#define N_INTERVALS          5
#define N_INTERVALS_STARTUP 30
#define N_GENERATED_MSGS    256

class simple_send : public proton::messaging_handler {
  private:
    std::string url;
    std::string user;
    std::string password;
    proton::sender sender;
    proton::duration interval;
    int sent;
    int confirmed;
    int released;
    int total;
    int last_sent;
    int stuck_intervals;
    int n_annotations;
    proton::message::annotation_map annotation_map;
    std::vector< proton::message > msgs;

  public:
    simple_send(const std::string &s, const std::string &u, const std::string &p, int c, int na) :
        url(s), user(u), password(p), 
        interval(int(INTERVAL_SEC*proton::duration::SECOND.milliseconds())), // Check interval
        sent(0), confirmed(0), released(0), total(c), last_sent(0), stuck_intervals(0), n_annotations(na) 
        {
            for (int i=0; i<n_annotations; i++) {
                std::ostringstream ossk;
                ossk << "K_" << i;
                annotation_map.put(ossk.str(), "0123456789");
            }
            // generated a bunch of messages
            for (int i=0; i<N_GENERATED_MSGS; i++) {
                proton::message msg;
                msg.message_annotations() = annotation_map;
                std::ostringstream oss;
                oss << "Sequence: " << i;
                msg.body(oss.str());
                msgs.push_back(msg);
            }
        }

    void on_container_start(proton::container &c) OVERRIDE {
        std::cout << "simple_send sending to " << url << std::endl;
        proton::connection_options co;
        if (!user.empty()) co.user(user);
        if (!password.empty()) co.password(password);
        sender = c.open_sender(url, co);
         // Start regular ticks every interval.
        c.schedule(interval, [this]() { this->tick(); });
    }

    void on_sendable(proton::sender &s) OVERRIDE {
        while (s.credit() && sent < total) {
            //proton::message msg;
            // Sending a map in the message body is fine and all but
            // it croaks wireshark and the whole packet is declared
            // corrupt and then nothing shows via tshark pdml. And
            // that slows my day.
            //
            //std::map<std::string, int> m;
            //m["sequence"] = sent + 1;
            //std::ostringstream oss;
            //oss << "Sequence: " << sent + 1;
            proton::message msg = msgs[ sent & (N_GENERATED_MSGS - 1) ];
            msg.id(sent + 1);
            //msg.body("abc"); //oss.str());
            //msg.message_annotations() = annotation_map;
            s.send(msg);
            sent++;
        }
    }

    void on_tracker_accept(proton::tracker &t) OVERRIDE {
        confirmed++;

        if (confirmed == total) {
            std::cout << "all messages confirmed" << std::endl;
            t.connection().close();
        }
    }
    
    void on_tracker_release(proton::tracker &t) OVERRIDE {
        released++;

        sent -= 1;
    }

    void on_transport_close(proton::transport &) OVERRIDE {
        sent = confirmed;
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
        //std::cout << "Tick. sent:" << sent << ", total:" << total << 
        //    ", last_sent" << last_sent << ", confirmed: " << confirmed << std::endl;
        bool done = sent == total;
        bool progress = sent != last_sent;
        stuck_intervals = (progress ? 0 : stuck_intervals + 1);
        bool stuck = stuck_intervals >= (sent == 0 ? N_INTERVALS_STARTUP : N_INTERVALS);
        
        // Schedule the next tick if work left to do and making progress
        if (!done && !stuck)
            sender.container().schedule(interval, [this]() { this->tick(); });
        
        // compute this interval's msg/S rate
        double fSec = double(INTERVAL_SEC);
        double fMsg = double(sent - last_sent);
        double rate = fMsg / fSec; // Caution: higher math in progress here
        
        // Report where we're at
        std::cout << getTime() << 
            " Messages sent: " << sent << 
            ", confirmed: " << confirmed << 
            ", rate msg/S: " << std::fixed << std::setprecision(0) << rate << std::endl;
        
            // Exit if stuck
        if (stuck) {
            std::cout << getTime() << " Progress is stuck. Messages sent: " << sent <<
                ", confirmed: " << confirmed << std::endl;
            sender.connection().close();
        }
        
        // Remember where we were
        last_sent = sent;
    }
};

int main(int argc, char **argv) {
    std::string address("127.0.0.1:5672/examples");
    std::string user;
    std::string password;
    int message_count = 100;
    example::options opts(argc, argv);
    int n_annotations = 0;

    opts.add_value(address, 'a', "address", "connect and send to URL", "URL");
    opts.add_value(message_count, 'm', "messages", "send COUNT messages", "COUNT");
    opts.add_value(user, 'u', "user", "authenticate as USER", "USER");
    opts.add_value(password, 'p', "password", "authenticate with PASSWORD", "PASSWORD");
    opts.add_value(n_annotations, 'n', "n_annotations", "number of ANNOTATIONS", "ANNOTATIONS");

    try {
        opts.parse();

        simple_send send(address, user, password, message_count, n_annotations);
        proton::default_container(send).run();

        return 0;
    } catch (const example::bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}

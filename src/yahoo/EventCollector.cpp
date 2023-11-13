/**
 * Copyright (c) 2020 University of Luxembourg. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials
 * provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors may be
 * used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF LUXEMBOURG AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE UNIVERSITY OF LUXEMBOURG OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

/*
 * EventCollector.cpp
 *
 *  Created on: Dec 26, 2018
 *      Author: vinu.venugopal
 */

#include <unistd.h>
#include "../serialization/Serialization.hpp"
#include "EventCollector.hpp"
#include <bits/stdc++.h>
#include <chrono>
using namespace std;

EventCollector::EventCollector(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;

	S_CHECK(if (rank == 0) {
		datafile.open("Data/results" + to_string(rank) + ".tsv");
	})

	D(cout << "EVENTCOLLECTOR [" << tag << "] CREATED @ " << rank << endl;)
}

EventCollector::~EventCollector()
{
	D(cout << "EVENTCOLLECTOR [" << tag << "] DELETED @ " << rank << endl;)
}

void EventCollector::batchProcess()
{
	D(cout << "EVENTCOLLECTOR->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}
long int EventCollector::timeSinceEpochMillisec()
{
	using namespace std::chrono;
	return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

long int EventCollector::getCurrentTimestampMillisec() {
    using namespace std::chrono;

    auto currentTimePoint = system_clock::now();
    auto duration = duration_cast<milliseconds>(currentTimePoint.time_since_epoch());

    return duration.count();
}
void EventCollector::streamProcess(int channel)
{
	int w_id = 0;

	D(cout << "EVENTCOLLECTOR->STREAMPROCESS [" << tag << "] @ " << rank
		   << " IN-CHANNEL " << channel << endl;)

	if (rank == 0)
	{
		long int res = 0;
		time_t start_time = getCurrentTimestampMillisec();
		// time_t end_time = start_time + 1;
		long int max_event_time = LONG_MAX;

		Message *inMessage;
		list<Message *> *tmpMessages = new list<Message *>();
		Serialization sede;

		EventPC eventPC;
		EventFT eventFT;
		int c = 0;
		while (ALIVE)
		{

			pthread_mutex_lock(&listenerMutexes[channel]);

			while (inMessages[channel].empty())
				pthread_cond_wait(&listenerCondVars[channel],
								  &listenerMutexes[channel]);

			while (!inMessages[channel].empty())
			{
				inMessage = inMessages[channel].front();
				inMessages[channel].pop_front();
				tmpMessages->push_back(inMessage);
			}

			pthread_mutex_unlock(&listenerMutexes[channel]);

			time_t alt_maxi = getCurrentTimestampMillisec();
			while (!tmpMessages->empty())
			{

				inMessage = tmpMessages->front();
				tmpMessages->pop_front();

				// cout << "Aggregating message: TAG [" << tag << "] @ " << rank
				// 	 << " CHANNEL " << channel << " BUFFER " << inMessage->size
				// 	 << endl;
				sede.unwrap(inMessage);
				// if (inMessage->wrapper_length > 0) {
				//	sede.unwrapFirstWU(inMessage, &wrapper_unit);
				//	sede.printWrapper(&wrapper_unit);
				// }

				int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));

				int event_count = (inMessage->size - offset) / sizeof(EventFT);

				// cout << "TOTAL EVENT_COUNT: " << res << endl;

				int i = 0, j = 0;
				time_t now = getCurrentTimestampMillisec();
				while (i < event_count)
				{
					res++;
					sede.YSBdeserializeFT(inMessage, &eventFT,
										  offset + (i * sizeof(EventFT)));
					// sede.YSBprintFT(&eventFT);
					// cout << eventFT.event_time << " " << eventFT.ad_id << endl;
					if (max_event_time > eventFT.event_time)
					{
						max_event_time = eventFT.event_time;
						// cout<<"HI "<<eventFT.event_time<<endl;

					}
					// eventPC.latency = difftime(now, eventFT.max_event_time);
					i++;
				}


				time_t curr_time = getCurrentTimestampMillisec();
				if(max_event_time > curr_time) max_event_time = alt_maxi;

				if (curr_time - start_time >= 10000)
				{
					time_t now = getCurrentTimestampMillisec();
					cout << " RELEASING EVENT:"<<endl;
					double elapsed = difftime(now, max_event_time);

					cout << "W_ID: " << w_id<< " Latency: "<< eventPC.latency
						  <<" " <<"Result: "<< res << " " <<"Elapsed time in seconds: "<< elapsed/1000 << endl;
					cout<<endl;
					res = 0;
					start_time = curr_time;
					w_id++;
					max_event_time = LONG_MAX;
				}

				delete inMessage; // delete message from incoming queue
				c++;
			}

			tmpMessages->clear();
		}

		delete tmpMessages;
	}
}

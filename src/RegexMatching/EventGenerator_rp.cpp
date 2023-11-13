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
 * EventGenerator.cpp
 *
 *  Created on: Dec 06, 2018
 *      Author: vinu.venugopal
 */

#include <mpi.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <iterator>
#include <list>
#include <string>
#include <vector>
#include <chrono>

#include "../communication/Window.hpp"
#include "EventGenerator_rp.hpp"

using namespace std;

EventGeneratorRP::EventGeneratorRP(int tag, int rank, int worldSize,
								   unsigned long tp) : Vertex(tag, rank, worldSize)
{
	this->throughput = tp;
	std::ifstream ifile("../data/YSB_data/ad_ids.txt");
	for (std::string line; getline(ifile, line);)
	{
		ad_ids.push_back(line);
	}

	cout << "AIR INSTANCE AT RANK " << (rank + 1) << "/" << worldSize << " | TP: " << throughput << " | MSG/SEC/RANK: " << PER_SEC_MSG_COUNT << " | AGGR_WINDOW: " << AGG_WIND_SPAN << "ms" << endl;

	S_CHECK(
		datafile.open("Data/data" + to_string(rank) + ".tsv");)

	D(cout << "EventGeneratorRP [" << tag << "] CREATED @ " << rank << endl;)
}

EventGeneratorRP::~EventGeneratorRP()
{
	D(cout << "EventGeneratorRP [" << tag << "] DELTETED @ " << rank << endl;)
}

void EventGeneratorRP::batchProcess()
{
	D(
		cout << "EventGeneratorRP->BATCHPROCESS: TAG [" << tag << "] @ "
			 << rank << endl;)
}

void EventGeneratorRP::streamProcess(int channel)
{

	D(
		cout << "EventGeneratorRP->STREAMPROCESS: TAG [" << tag << "] @ "
			 << rank << " CHANNEL " << channel << endl;)

	Message *message;
	Message **outMessagesPerSec = new Message *[PER_SEC_MSG_COUNT];

	WrapperUnit wrapper_unit;
	EventRG eventRG;

	int wrappers_per_msg = 1; // currently only one wrapper per message!
	int events_per_msg = this->throughput / PER_SEC_MSG_COUNT / worldSize;

	long int start_time = (long int)MPI_Wtime();
	long int t1, t2;

	int iteration_count = 0, c = 0;

	while (ALIVE)
	{

		t1 = MPI_Wtime();

		int msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT)
		{

			outMessagesPerSec[msg_count] = new Message(
				events_per_msg * sizeof(EventDG), wrappers_per_msg);

			// Message header
			long int time_now = (start_time + iteration_count) * 1000;
			wrapper_unit.window_start_time = time_now + 999; // this is the max-event-end-time
			wrapper_unit.completeness_tag_numerator = 1;
			wrapper_unit.completeness_tag_denominator = PER_SEC_MSG_COUNT * worldSize * AGG_WIND_SPAN / 1000;

			memcpy(outMessagesPerSec[msg_count]->buffer, &wrappers_per_msg,
				   sizeof(int));
			memcpy(outMessagesPerSec[msg_count]->buffer + sizeof(int),
				   &wrapper_unit, sizeof(WrapperUnit));
			outMessagesPerSec[msg_count]->size += sizeof(int) + outMessagesPerSec[msg_count]->wrapper_length * sizeof(WrapperUnit);

			// Message body
			getNextMessage(&eventRG, &wrapper_unit,
						   outMessagesPerSec[msg_count], events_per_msg, time_now);

			// Debug output ---
			Serialization sede;

			// for (int e = 0; e < events_per_msg; e++)
			// {
			// sede.YSBdeserializeDG(outMessagesPerSec[msg_count], &eventRG,
			// 						  sizeof(int) + (outMessagesPerSec[msg_count]->wrapper_length * sizeof(WrapperUnit)) + (e * sizeof(EventDG)));
			// }

			msg_count++;
			c++;
		}

		t2 = MPI_Wtime();
		while ((t2 - t1) < 1)
		{
			usleep(100);
			t2 = MPI_Wtime();
		}

		msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT)
		{
			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex *>::iterator v = next.begin(); v != next.end();
				 ++v)
			{

				int idx = n * worldSize + rank; // always keep workload on same rank

				if (PIPELINE)
				{

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(
						outMessagesPerSec[msg_count]);

					D(
						cout << "EventGeneratorRP->PIPELINE MESSAGE [" << tag
							 << "] #" << c << " @ " << rank
							 << " IN-CHANNEL " << channel
							 << " OUT-CHANNEL " << idx << " SIZE "
							 << outMessagesPerSec[msg_count]->size << " CAP "
							 << outMessagesPerSec[msg_count]->capacity << endl;)

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);
				}
				else
				{

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessagesPerSec[msg_count]);

					D(
						cout << "EventGeneratorRP->PUSHBACK MESSAGE [" << tag
							 << "] #" << c << " @ " << rank
							 << " IN-CHANNEL " << channel
							 << " OUT-CHANNEL " << idx << " SIZE "
							 << outMessagesPerSec[msg_count]->size << " CAP "
							 << outMessagesPerSec[msg_count]->capacity << endl;)

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;
				break; // only one successor node allowed!
			}

			msg_count++;
		}

		iteration_count++;
	}
}

string EventGeneratorRP::generate_seq()
{
	const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	const int alphabetSize = alphabet.size();
	string sequence = "";
	// sequence +=alphabet;
	// sequence +="ABCEDEFGHIJKLMNOPRSTUVWX";

	std::srand(static_cast<unsigned int>(std::time(0)));

	for (int i = 0; i < 50; ++i)
	{
		sequence += alphabet[rand() % alphabetSize];
	}

	return sequence;
}

long int EventGeneratorRP::timeSinceEpochMillisec()
{
	using namespace std::chrono;
	return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

long int EventGeneratorRP::getCurrentTimestampMillisec()
{
	using namespace std::chrono;

	auto currentTimePoint = system_clock::now();
	auto duration = duration_cast<milliseconds>(currentTimePoint.time_since_epoch());

	return duration.count();
}

void EventGeneratorRP::getNextMessage(EventRG *event, WrapperUnit *wrapper_unit,
									  Message *message, int events_per_msg, long int time_now)
{

	Serialization sede;
	long int max_time = 0;
	// Serializing the events
	int i = 0;
	while (i < events_per_msg)
	{
		string seq = generate_seq();
		memcpy(event->ad_id, seq.c_str(), 50);
		event->event_time = (time_now);

		S_CHECK(
			datafile << event->event_time << "\t"
					 // divide this by the agg wid size
					 << event->event_time / AGG_WIND_SPAN << "\t"
					 // divide this by the agg wid size
					 << rank << "\t" << i << "\t" << event->event_type << "\t"
					 << event->ad_id << endl;);

		sede.YSBserializeRG(event, message);
		if (max_time < event->event_time)
			max_time = event->event_time;

		i++;
	}
	wrapper_unit->window_start_time = max_time;
}

int EventGeneratorRP::myrandom(int min, int max)
{ // range : [min, max)
	static bool first = true;
	if (first)
	{
		srand(time(NULL));
		first = false;
	}
	return min + rand() % ((max + 1) - min);
}

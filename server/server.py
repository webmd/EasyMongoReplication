
# Mongodb cross data-center active-passive replication
# Author:: Matthew Lee (<mattlee446@gmail.com>)

# Copyright:: Copyright (c) 2015 WebMD, LLC
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import Queue
import re
import pymongo
import time
import datetime
import bson
import config as CONFIG
from pymongo import MongoClient
from bson.objectid import ObjectId
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.errors import AutoReconnect
from pymongo.errors import DuplicateKeyError
from pymongo.errors import InvalidName
from pymongo.errors import ConnectionFailure
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.cursor import CursorType
from pymongo.errors import BulkWriteError
from pymongo.errors import OperationFailure
from bson.timestamp import Timestamp
from threading import Thread
from Queue import PriorityQueue
from time import sleep

__author__ = "Matthew Lee"
__email__ = "mlee@webmd.net"


class ReplServer:  # Server class which handles replication

    def __init__(self, master_host, master_port, target_host, target_port, dbnames):
        try:
            self.master_mongos = MongoClient(master_host, master_port)
            self.target_mongos = MongoClient(target_host, target_port)
            self.replsets = {}
            self.replnames = []

            self.master_shards = self.get_shard_connections()  # Gets connections to all shard instances of the master slave cluster
            self.last_sync = self.target_mongos['sync_log']['init_sync'].find().sort('ts', pymongo.DESCENDING).limit(1)  # Gets the last initial sync time, if it exists
            self.cursor_ids = []
            self.dbnames = dbnames
            sleep(5)

            print("Shards you are going to replicate:")

            for con in self.master_shards:
                print con

            sleep(2)

            if(self.last_sync.count() == 0):
                print("Doing Initial Sync")
                self.initial_sync()
            else:
                self.last_sync = self.last_sync[0]["ts"]
                print("Doing Continuous Sync")
                self.continuous_sync()
        except(AutoReconnect, ConnectionFailure, ServerSelectionTimeoutError) as e:
            print ("Could not establish connection to mongo")

    def start_listening(self):  # This method spawns seperate threads to tail each shard's oplog.
        shards = self.master_shards
        for shard in shards:
            listener = Thread(target=self.listen_oplog, args=(shard,))
            listener.start()

    def listen_oplog(self, shard):  # Listens to oplog on single shard and replays them to the target database.
        db = shard.local
        name = shard.address
        name = str(name[0]) + ":" + str(name[1])
        name = self.replsets[name]

        while True:
            try:
                last_sync = self.target_mongos['sync_log'][name].find().sort('ts', pymongo.DESCENDING).limit(1)[0]['ts']
                break
            except AutoReconnect:
                sleep(10)
                continue
        sync_list = []
        for x in self.replnames:
            x = "sync_log." + x
            sync_list.append(x)
        print(sync_list)
        while True:
            query = {'ts': {'$gt': last_sync}, 'fromMigrate': {'$exists': False}, 'ns': {'$nin': sync_list}}  # find all oplog operations greater than the last_sync, and don't include sync_log entries
            try:
                cursor = db.oplog.rs.find(query, cursor_type=CursorType.TAILABLE_AWAIT, no_cursor_timeout=True)
            except AutoReconnect:
                sleep(10)
                continue
            self.cursor_ids.append(cursor.cursor_id)
            try:
                while cursor.alive:
                    try:
                        entry = cursor.next()
                        self.replay_command(entry)
                        timestamp = entry["ts"]
                        self.target_mongos['sync_log'][name].insert({'ts': timestamp})  # updates last sync_log entry in sync_log database
                        self.master_mongos['sync_log'][name].insert({'ts': timestamp})
                    except (AutoReconnect, StopIteration) as e:
                        sleep(10)
            finally:
                cursor.close()
                sleep(5)

    def get_shard_connections(self):  # returns a list of mongoclients of all the different shards in the master cluster
        configdb = self.master_mongos.config
        shardinfo = configdb.shards  # get the locations of all the database shards
        shardinfo = shardinfo.find()

        shardconnections = []
        for shard in shardinfo:  # data is formatted like this: {u'host': u'tic/Matthews-MacBook-Pro-3.local:27018,Matthews-MacBook-Pro-3.local:27019,Matthews-MacBook-Pro-3.local:27020', u'_id': u'tic
            replname = shard["_id"]
            self.replnames.append(replname)
            hostinfo = shard['host']
            hostinfo = hostinfo.split('/')
            machines = hostinfo[1]
            machines = machines.split(',')
            for machine in machines:
                self.replsets[machine] = replname
            connectinfo = hostinfo[1]
            connectinfo = connectinfo.split(',')
            connection = MongoClient(connectinfo, replicaset=replname)  # mongoclient automatically reads from primary and will reconnect to new primaries
            shardconnections.append(connection)

        return shardconnections

    def initial_sync(self):  # Method that starts the initial collection dump and then spawns the writer
        print self.dbnames

        time_t = time.time()
        time_log = Timestamp(int(time_t) - 1, 0)
        times = Timestamp(int(time_t), 0)
        curr_time = times.as_datetime()

        self.target_mongos['sync_log']['init_sync'].insert({'ts': time_log})
        self.master_mongos['sync_log']['init_sync'].insert({'ts': time_log})
        for name in self.replnames:
            print name
            self.target_mongos['sync_log'][name].insert({'ts': time_log})
            self.master_mongos['sync_log'][name].insert({'ts': time_log})
        self.last_sync = time_log  # set last time sync time to be current time, push to database

        threads = []

        for shard in self.master_shards:
            for dbname in self.dbnames:  # loop through all databases that you want to replicate
                if dbname in shard.database_names():  # if the database is on the shard
                    identity = shard.address
                    print("Replicating database: %s , on Shard: %s: %s" % (dbname, identity[0], identity[1]))
                    db = shard[dbname]
                    colls = db.collection_names(include_system_collections=False)
                    for coll in colls:  # spawn collection dumper threads for all collections within the database
                        coll_dumper = Thread(target=self.dump_collection, args=(db, dbname, coll,))
                        threads.append(coll_dumper)
                        coll_dumper.start()

        for thread in threads:  # wait on all dumper threads before moving on to write oplog operations
            thread.join()

        print("Finished inital sync, took")
        print(time.time() - time_t)
        self.start_listening()  # start tailing on all shards

    def continuous_sync(self):  # method which handles continuous sync between servers, using the sync_log db as a reference for last sync time
        self.start_listening()  # start tailing on all shards and begin filling up the queue

    def dump_collection(self, db, dbname, collname):
        cursor = db[collname].find()
        numdocs = cursor.count()
        cursor.close()
        print("Initial Sync, Dumping Collection %s" % collname)

        last_grabbed = db[collname].find().sort([("_id", 1)]).limit(1)
        batch_size = 100000
        cursor_l = []
        self.target_mongos[dbname][collname].insert_one(last_grabbed[0])

        last_grabbed = last_grabbed[0]["_id"]

        while True:
            try:
                cursor = db[collname].find({"_id": {"$gt": last_grabbed}}, no_cursor_timeout=True).sort([("_id", 1)]).limit(batch_size)
            except OperationFailure as e:
                sleep(5)
                cursor = db[collname].find({"_id": {"$gt": last_grabbed}}, no_cursor_timeout=True).sort([("_id", 1)]).limit(batch_size)
            except AutoReconnect:  # if a replica set election is happening, AutoReconnect will be thrown. In this case, wait until a new primary is elected then pick up where you left off
                sleep(5)
                continue

            if(cursor.count() == 0):
                break
            cursor_l = list(cursor)
            cursor.close()
            try:
                self.target_mongos[dbname][collname].insert_many(cursor_l, ordered=False)
            except BulkWriteError as bwe:
                pass
            except OperationFailure as ow:
                sleep(5)
                self.target_mongos[dbname][collname].insert_many(cursor_l, ordered=False)
            except AutoReconnect:
                sleep(5)
                continue
            last_grabbed = cursor_l[len(cursor_l) - 1]["_id"]
            del cursor_l[:]

        print("Finished dumping collection %s" % collname)

    def replay_command(self, oplog_entry):  # replays an oplog entry to write to the slave database
        legal_ops = ['i', 'u', 'd']
        operation = oplog_entry['op']

        if operation not in legal_ops:
            return

        databaseinfo = oplog_entry['ns']
        databaseinfo = databaseinfo.split('.')  # formatted like "ns" : "test.foo"
        collection = self.target_mongos[databaseinfo[0]][databaseinfo[1]]
        if(operation == "u"):
            try:
                criteria = oplog_entry["o2"]
                action = oplog_entry["o"]
                collection.update_one(criteria, action)
            except ValueError as ve:
                criteria = oplog_entry["o2"]
                action = oplog_entry["o"]
                collection.replace_one(criteria, action)
        elif(operation == "i"):
            action = oplog_entry["o"]
            try:
                collection.insert_one(action)
            except DuplicateKeyError:
                return
        else:
            criteria = oplog_entry["o"]
            collection.delete_one(criteria)

    def stop_syncing(self):  # kill all cursors and remove connections to database
        self.master_mongos.kill_cursors(self.cursor_ids)
        self.master_mongos.close()
        self.target_mongos.close()
        for shard in self.master_shards:
            shard.close()
        print("Connections to servers are closing. Goodbye")


def main():
    config_dict = CONFIG.config
    server = ReplServer(config_dict["master_name"], config_dict["master_port"], config_dict["target_name"],
                        config_dict["target_port"], config_dict["dbnames"])


if __name__ == '__main__':
    main()

#Easy Mongo Replication

##License
Author:: Matthew Lee <mlee@webmd.net>
Copyright:: Copyright (c) WebMD, LLC
License:: Apache License, Version 2.0
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
```
http://www.apache.org/licenses/LICENSE-2.0
```
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

##About
This purpose of this program is to provide a better way to replicate MongoDB sharded clusters across different data centers. 

The program allows for seperate clusters to maintain identical datasets with completely different internal configurations.

This program provides an alternative to annoyingly complex replicaset/config server configuration across datacenters. 

In a DR scenario, this program allows for easy Master/slave switching and turns DR into a "1 click process"

This program is written in Python. 

##Usage
1. Edit the config file. Fill in the hostnames and ports for the master and target mongos instances for the clusters you want to replicate. In the dbnames field, fill in a list of all the dbs you want to replicate. 
   For example, if you wanted to replicate only the db carrots, the field would look like ["carrots"]. If you wanted carrots and celery the field would look like ["carrots", "celery"]
2. Ensure that the master cluster's shard balancing is turned off. To do this fire up a mongo shell instance connected to the master mongos and run the command sh.stopBalancer().
   Some chunk migrations may be in process, so wait until the command sh.isBalancerRunning() returns false before you start the replication process. 
3. (optional) create a database named sync_log. For each of your replicasets, create a capped collection with the collection name as the replicaset name. If your replica set name is "mongomongo" you should have a collection
   in the database sync_log with the name "mongomongo". If you don't do this step the sync will still work, but your sync_log database will grow forever as last synced times get updated.
4. If any of your replicated databases are sharded, you must pre shard/enable sharding on the target cluster before you kick off the replication process.
5. Launch the server.py program. Your initial sync should start. Currently, you cannot shut down the initial sync progress and start it up again. If you interrupt the initial sync you will have to drop everything and redo the entire process
6. Once the initial sync is completed, the continuous sync will start. At this point you can freely shut down and start up the program. It takes a bit of time after the initial sync completes for the target database
   to fully catch up with the changes that happened during the initial sync.

##How it works

For the initial sync, the server will essentially launch a seperate thread for each collection and do a find() and sort() by _id to get an image of the data. It will then batch insert these documents, while noting the last _id inserted.

For the continuous sync, seperate threads are launched to tail each shards oplog. As new oplog entries come in they are replayed on the target cluster by each thread. 


##Dependencies
```
pymongo
```

##How to Contribute

There are a number of optimizations and problems to work on for the program. 

####Integrity:
1. Aggregate oplog entries accross all shards in a priority queue to ensure that the oplog replay happens in order (may not be necessary, and would decrease performance)
2. Fix the error logger
3. Create a good checker program to ensure data integrity between the two databases

####Performance:
1. Allow for the initial sync to restart if interrupted
2. Multiprocess the inital dump (could potentially start a dumper script on each of the shards)
3. Bulk write oplog entries (would increase performance but potentially introduce integrity errors)
4. Find a way to do the initial dump without a sort() 

####Features:
1. Add feature to automatically turn off the  shard balancer before the sync process
2. Add feature to automatically copy sharding configuration of master cluster



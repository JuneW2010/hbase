#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script to add a peer to a cluster
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main add_peer.rb
#

include Java
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.EmptyWatcher
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper

# Name of this script
NAME = "add_peer"

# Print usage for this script
def usage
  puts 'Usage: %s.rb master_zookeeper.quorum.peers:clientport:znode_parent slave_zookeeper.quorum.peers:clientport:znode_parent' % NAME
  exit!
end

if ARGV.size != 2
  usage
end

LOG = LogFactory.getLog(NAME)

parts1 = ARGV[0].split(":")
LOG.info("Master cluster located at " + parts1[0] + " port " + parts1[1] + " in folder " + parts1[2])

c2 = HBaseConfiguration.create()
parts2 = ARGV[1].split(":")
LOG.info("Slave cluster located at " + parts2[0] + " port " + parts2[1] + " in folder " + parts2[2])

LOG.info("The addresses must be exactly the same as those in hbase-site.xml of each cluster.")
print "Are those info correct? [Y/n] "
answer = $stdin.gets.chomp

if answer.length != 0 || answer == "n" || answer == "no"
  exit!
end

c1 = HBaseConfiguration.create()
c1.set(HConstants.ZOOKEEPER_QUORUM, parts1[0])
c1.set("hbase.zookeeper.property.clientPort", parts1[1]) 
c1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parts1[2])

zkw1 = ZooKeeperWrapper.new(c1, EmptyWatcher.instance)
zkw1.writeZNode(parts1[2], "replication", "a")
zkw1.writeZNode(parts1[2] + "/replication", "master", ARGV[0]);
zkw1.writeZNode(parts1[2] + "/replication", "state", "true");
zkw1.writeZNode(parts1[2] + "/replication/peers", "test", ARGV[1]);


c2.set(HConstants.ZOOKEEPER_QUORUM, parts2[0])
c2.set("hbase.zookeeper.property.clientPort", parts2[1]) 
c2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parts2[2])

zkw2 = ZooKeeperWrapper.new(c2, EmptyWatcher.instance)
zkw2.writeZNode(parts2[2], "replication", "a")
zkw2.writeZNode(parts2[2] + "/replication", "master", ARGV[0]);

#!/usr/bin/env python
import time
from raft import RaftNode

# Create the intercommunication json 
ip_addr = "127.0.0.1"
comm_dict = {}
num_nodes = 10
port = 5429
for i in range(num_nodes):
    comm_dict["node" + str(i)] = {"ip": ip_addr, "port": str(port)}
    port = port - 1
# comm_dict = {"node0": {"ip": ip_addr, "port": "5518"}, 
#              "node1": {"ip": ip_addr, "port": "5517"},
#              "node2": {"ip": ip_addr, "port": "5516"}}

# Start a few nodes
nodes = []
print("Starting nodes:")
count = 0

for name, address in comm_dict.items():
    nodes.append(RaftNode(comm_dict, name))
    start = time.time()
    print("start: ", start, " : ", name)
    # if(count == 0):
    #     nodes.append(RaftNode(comm_dict, name, 'leader'))
    #     # time.sleep(10)
    # else:
    #     # print("In else")
    #     nodes.append(RaftNode(comm_dict, name))
    #     # time.sleep(10)
    # count = count + 1
    nodes[-1].start()

# Let a leader emerge
time.sleep(5)

# start = time.time()
# Make some requests
print("Making req:")
for val in range(1):
    nodes[0].client_request({'val': val}, start)
time.sleep(20)

# end = time.time()
# Check and see what the most recent entry is
print("commit:")
for n in nodes:
    print(n.check_committed_entry())

# Stop all the nodes
print("Stopping:")
for n in nodes:
    n.stop()

exit()
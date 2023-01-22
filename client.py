import rpyc
import time
import math
import pickle
import random
from bisect import bisect
from datetime import timedelta, datetime

class VectorClock:
    def __init__(self, hostname, port, vid, load, start_range, version_no):
        self.hostname = hostname
        self.port = port
        self.vid = vid
        self.load = load
        self.start_range = start_range
        self.version_no = version_no

    def to_dict(self):
        return {
            "hostname": self.hostname,
            "port": self.port,
            "vid": self.vid,
            "load": self.load,
            "start_range": self.start_range,
            "version_no": self.version_no
        }


class Client:
    def __init__(self, init_nodes) -> None:
        # Initial random worker nodes
        self.init_nodes = init_nodes
        # Dict: {key -> (controller's hash)}
        self.keys_cache = dict()
        # Dict: {node's hash -> (VectorClock, last_updated_time)}
        self.worker_nodes_cache = dict()
        # Sorted list of nodes
        self.worker_nodes = []
        self.CACHE_TIMEOUT = 500
        self.NUMBER_NODES = 4

    def deserialize(self, routing_table):
        res_dict = dict()
        for hash, node in routing_table.items():
            res_dict[hash] = self.from_dict(node)
        return res_dict

    def from_dict(self, vector_obj):
        return VectorClock(vector_obj['hostname'], vector_obj['port'], vector_obj['vid'], vector_obj['load'], vector_obj['start_range'], vector_obj['version_no'])

    def get_routing_table_info(self, key):
        while(True):
            try:
                random_node_index = random.randint(0, len(self.worker_nodes))
                random_node = self.worker_nodes[random_node_index]
                res_routing_table, controller_key = rpyc.connect(*random_node).root.get_routing_table_info(key)
                res_routing_table = self.deserialize(pickle.dumps(res_routing_table))
                # check if result contains the routing table
                curr_time = datetime.now()
                for hash, node in res_routing_table.items():
                    if node not in self.worker_nodes_cache or self.worker_nodes_cache[hash].version_no < node.version_no:
                        self.worker_nodes_cache[hash] = (node, curr_time)
                        # find the position of the node in the sorted list and insert there
                        if node not in self.worker_nodes_cache:
                            pos = bisect(self.worker_nodes, hash)
                            self.worker_nodes.insert(pos, hash)
                self.keys_cache[key] = controller_key
                break
            except Exception as e:
                continue


    def get(self, key):
        if key not in self.keys_cache:
            self.get_routing_table_info(key)
        pos = bisect(self.worker_nodes, self.keys_cache[key])
        curr_time = datetime.now()
        # try on the nodes and if succeeds for any one break over there only
        for i in range(0, math.min(len(self.worker_nodes), self.NUMBER_NODES)):
            try:
                node = self.worker_nodes_cache[self.worker_nodes[(pos + i) % len(self.worker_nodes)]]
                if timedelta.total_seconds(curr_time - node[1]) < self.CACHE_TIMEOUT:
                    res = rpyc.connect(node.hostname, node.port).root.get(key)
            except Exception as e:
                pass




        pass
    
    def put(self, key):
        pass

if __name__ == "__main__":
    nodes = [('10.237.27.245', 3000), ('10.237.27.245', 3001),
            ('10.237.27.245', 3000), ('10.237.27.245', 3001)]
    client = Client(nodes)

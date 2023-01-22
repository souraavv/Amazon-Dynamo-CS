import os 
import rpyc
import pickle
import random
import math 

from typing  import Dict
from bisect import bisect
from datetime import timedelta, datetime
from rpyc.util.server import ThreadedServer

class VectorClock:
    def __init__(self, ip:str, port:int, version_number:int, load:float, start_of_range:int) -> None:
        self.ip:str = ip
        self.version_number:int = version_number
        self.load:float = load 
        self.port:int = port
        self.start_of_range:str = start_of_range
        
    def to_dict(self) -> Dict:
        return {
            "ip": self.ip,
            "version_number": self.version_number,
            "load": self.load,
            "port": self.port,
            "start_of_range": self.start_of_range 
        }

class Client(rpyc.Service):
    def __init__(self, nodes) -> None:
        self.nodes = nodes #* In future when some service reply this then we can cache this too
        self.cache = dict()
        self.locate_key = dict()
        self.all_nodes = set()
        self.CACHE_TIMEOUT = 20
        self.READ = 3
        self.WRITE = 2
        self.HINTED_REPLICA_COUNT = 2

    def deserialize(self, response):
        deserialize_response = {}
        for hash, vc in response.items():
            deserialize_response[hash] = VectorClock(ip=vc['ip'],port=vc['port'], 
                version_number=vc["version_number"], 
                load=vc["load"], start_of_range=vc["start_of_range"])
        return deserialize_response

    def serialize(self, response):
        serialized_response = {}
        for hash, vc in response.items():
            serialized_response[hash] = vc.to_dict()
        return serialized_response

    '''
    This function will fetch the routing table from some random node from self.nodes list
    '''
    def get_routing_info(self, key):
        while True:
            try:
                node = random.randint(0, len(self.nodes) - 1)
                who = random.randint(0, self.nodes[node]["vnodes"] - 1)
                url = (node["ip"], int(node["port"]) + who)
                conn = rpyc.connect(*url)
                conn._config['sync_request_timeout'] = None 
                response, controller_node = conn.root.fetch_routing_info(key)
                self.deserialize(pickle.loads(response))
                self.locate_key[key] = controller_node 
                for node_hash, vc in response.items():
                    if node_hash in self.cache.keys():
                        if self.cache[node_hash].version_number < vc.version_number: # update only version number is newest
                            self.cache[node_hash] = (vc, datetime.now())
                    else:     # if this is the fresh entry then simply update
                        self.all_nodes.append(node_hash)
                        self.all_nodes.sort()
                        self.cache[node_hash] = (vc, datetime.now())
                break 
            except Exception as e:
                print (f"Some thing bad happened while fetching routing info...", e)
                continue
            return response


    '''
    This function will return the nodes which are potential candidate
    for the given key
    '''
    def get_key_containing_nodes(self, key):
        if key not in self.locate_key: # Fetch in case it is not there.
            self.get_routing_info(key)
        controller_node = self.locate_key[key]
        controller_node_idx = bisect(self.all_nodes, controller_node)
        controller_node_idx = controller_node_idx - 1 if controller_node_idx else 0 
        n = len(self.all_nodes)
        key_contained_by = []
        #* Since it is a ring, not a linear chain, we need to do %
        for pos in range(0, math.min(len(self.all_nodes) ,self.READ)):
            key_contained_by.append(self.all_nodes[(controller_node_idx + pos) % n])
        return controller_node, key_contained_by

    def get(self, key):
        controller_node, key_contained_by = self.get_key_containing_nodes(key)
        #TODO: Make this function really abstracted
        #TODO: Talk to relevant nodes and then get the final output
        #TODO: Write the logic to read now, based on concillation algo.
        pass

    def put(self, key, value):
        controller_node, key_contained_by = self.get_key_containing_nodes(key)
        
        #TODO: Write the logic to write now
        #TODO: We need this service to be mostly say ok to client
        
        pass

if __name__ == '__main__':
    port = 6001
    #TODO: Later move these to some service provided by Hashring or some 
    #TODO: complete independent service also ok.
    nodes = [
            {
                'username': 'sourav',
                'hostname': '10.237.27.95',
                'port': 3000,
                'vnodes': 4
            },
            {
                'username': 'baadalvm',
                'hostname': '10.17.50.254',
                'port': 3000,
                'vnodes': 4
            }
    ]
    ThreadedServer(Client(nodes), hostname='0.0.0.0', port=port).start()
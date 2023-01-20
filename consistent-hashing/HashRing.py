import rpyc 
import subprocess
import redis
import copy 
import os 
import time 
from os.path import join, dirname
import subprocess as sp

from HashRing import *
from hashlib import md5
from pexpect import pxssh
from bisect import bisect
from rpyc.utils.server import ThreadedServer
from typing import List, Set, Dict, Tuple, Callable, Iterator, Union, Optional, Any, Counter
from dotenv import load_dotenv

'''
nodes_conf: {hostname -> configuration}
ring: [hashi(hostname) -> (hostname, port, who)]
vnode: Virtual node count
hosts: Active host which are used in rings
keys: List of all keys of vnodes present on the ring: maintain sorted so can binarysearch
hash_function: str -> int 
resources : Handle by admin, to add a new resource in teh list
'''

class HashRing(rpyc.Service):
    def __init__(self, nodes_conf: List[Dict[str, Any]] = {}, **kwargs) -> None:
        self.hash_function: Callable[[str], int] = (lambda key: int(md5(str(key).encode("utf-8")).hexdigest(), 16))
        self.ring: Dict[int, Set(Any, Any, Any)] = {} 
        self.default_vnodes: int = 2
        self.vnodes: int = kwargs.get("vnodes", self.default_vnodes)
        self.hosts: Dict[str, Dict[str, Any]] = {}
        self.keys: List[str] = []
        self.resources: List[Dict[str, Any]] = nodes_conf 
        # self.make_setup_ready()
        self.SPAWN_WORKER_PORT = 4001

    def give_hash(self, key: str) -> str:
        return str(self.hash_function(key))

    def initialize_worker(self, conf):        
        mydir = os.path.dirname(os.path.realpath(__file__))
        s = pxssh.pxssh()    
        dotevn_path = join(dirname(__file__), '.env')
        load_dotenv(dotevn_path)
        username, hostname = conf['username'], conf['hostname']
        env_key = "_".join([username.upper(), "_".join(hostname.split('.'))])
        print (env_key)
        password = os.environ.get(env_key)
        print (hostname, username, password)
        uri = f"{username}@{hostname}"
        s.login(hostname, username, password, sync_multiplier=5, auto_prompt_reset=False)
        s.prompt()
        s.sendline(f'mkdir -p Dynamo')
        s.prompt()
        sp.run(['scp', 'spawn_worker.py', 'worker.py', f'{uri}:~/Dynamo/']).check_returncode()
        s.sendline(f'redis-cli SHUTDOWN')
        s.prompt()
        s.sendline('nohup redis-server &')
        s.prompt()
        s.sendline(f'redis-cli flushall')
        s.prompt()
        s.sendline('cd Dynamo && python3 spawn_worker.py')
        print (s.before)
        s.prompt()
        
    def make_setup_ready(self):
        for conf in self.resources:
            self.initialize_worker(conf)

    '''
    Check the configuration change or existence in the present ring, if even
    a single change we reconfigure the complete ring again
    '''
    def configure_nodes(self, nodes_conf: List[Dict[str, Any]]) -> bool:
        if not isinstance(nodes_conf, List):
            raise ValueError(
                f'nodes_conf configuation must be Dict, got {type(nodes_conf)}'
            )

        conf_changed: bool = False
        for conf in nodes_conf:
            hostname = conf['hostname']
            if hostname not in self.hosts.keys():
                conf_changed = True
            self.hosts[hostname] = conf 
        return conf_changed
   
    '''
    Create ring: This function will add new nodes_conf, if configuration changes
                 or a new node is added
    '''

    def get_neighbours(self, vnode_hash:str) -> Any:
        print ("keys: ", self.keys)
        idx = bisect(self.keys, vnode_hash)
        idx = 0 if idx == len(self.keys) else idx
        return (idx - 1, idx)

    def create_ring(self, nodes_conf: List[Dict[str, Any]]) -> None:
        # TODO: first start all the nodes present in thfe conf
        # TODO store their hash keys and send the update to the get_host() 
        # node_Hash -> {hostname, virual name}
        go_to_ring:Dict[str: set(str, str, str)] = {}
        for node_conf in nodes_conf:
            hostname = node_conf['hostname']
            port = node_conf['port']
            for who in range(0, node_conf["vnodes"]):
                go_to_ring[self.give_hash(f'{hostname}_{who}')] = (hostname, port, who)
            conn = rpyc.connect(hostname, self.SPAWN_WORKER_PORT)
            conn._config['sync_request_timeout'] = None 
            conn.root.spawn_worker(node_conf["port"], node_conf["vnodes"])
        
        time.sleep(20)

        for vnode_hash, vnode_info in go_to_ring.items():
            print ("hash", vnode_hash, len(self.ring))
            # right and left are considered assuming clockwise movement
            # and back of head is always facing center while moving
            hostname, port, who = vnode_info
            left_idx, right_idx = self.get_neighbours(vnode_hash)
            print (left_idx, right_idx)
            only_single_node:bool = True
            
            left_node_hash, right_node_hash =  vnode_hash, -1
            if len(self.ring):
                left_node_hash, right_node_hash = self.keys[left_idx], self.keys[right_idx]
                only_single_node = False 

            print (left_node_hash, right_node_hash, only_single_node)
            new_added = {
                "start_of_range": str(int(left_node_hash) + 1),
                "ip": hostname,
                "port": port + who,
                "version_number": 0,
                "load": 0,
                "end_of_range": str(vnode_hash) 
            }

            print(new_added)
            # TODO: rpc call 1 to the newly added node
            response_to_new_node = {
                            "new_start": str(int(left_node_hash) + 1),
                            "new_end": str(vnode_hash),
                            "new_added": new_added
                        }

            self_url = (hostname, port + who)
            print (*self_url)
            conn = rpyc.connect(*self_url) 
            conn._config['sync_request_timeout'] = None 
            conn.root.init_table(response_to_new_node)
            # TODO: rpc call 2 to the right node
            if only_single_node == False:
                response_to_right_node = {
                                "new_start": str(int(vnode_hash) + 1),
                                "new_end": str(right_node_hash),
                                "new_added": new_added
                            }
                right_ip, right_port, _ = self.ring[self.keys[right_idx]]
                right_url = (right_ip, right_port)
                conn = rpyc.connect(*right_url) 
                conn._config['sync_request_timeout'] = None 
                conn.root.update_table(response_to_right_node)
            # add to ring
            self.ring[vnode_hash] = vnode_info
            #sort the keys
            self.keys = sorted(self.ring.keys())
        
        self.keys = sorted(self.ring.keys())

    '''
    To remove a node from the ring, first remove it from node list, then 
    from the distribution and at last from the ring too
    '''
    def remove_node(self, hostname: str) -> None:
        try:
            node_conf: dict = self.hosts.pop(hostname)
            
        except Exception:
            raise KeyError (
                f'Node: {hostname} not found, available nodes_conf are {self.hosts.keys()}'
            )
        else:
            for who in range(0, node_conf.get("vnodes")):
                del self.ring[(self.give_hash(f'{hostname}_{who}'))]
            self.keys: List[str] = sorted(self.ring.keys())
            self.resources.append(node_conf)

    '''
     Add a new node in the ring
    '''
    def exposed_add_node(self, node_conf:List[Dict[str, Any]]) -> None:
        if self.configure_nodes(node_conf):
            self.create_ring(node_conf)
        return {"status": 0, "msg": "success"}

    '''
    A generic function to fetch the several property of node configuration
    '''
    def _get(self, key:str, what) -> Any:
        p = bisect(self.keys, self.give_hash(key)) 
        p = 0 if p == len(self.keys) else p
        hostname, port, who = self.ring[self.keys[p]]
        if what == 'hostname': 
            return hostname
        return self.hosts[hostname][what]
        
    # def locate_key(self, key:str) -> Any:
    #     return self._get(key, 'instance')

    def get_host(self, key:str) -> str:
        return self._get(key, 'hostname')

    def get_port(self, node_hash:str) -> str: 
        p = bisect(self.keys, node_hash) 
        p = 0 if p == len(self.keys) else p 


    def exposed_get_all_node_location(self, ip:str, virtual_id:str) -> Any:
        vnode = f'{ip}-{virtual_id}'
        return {"status": 0, "msg": "success", "res": self.ring}

    # API for resource grant and revoke
    def exposed_allocate_nodes(self, required_nodes) -> Any:
        if required_nodes > len(self.resources):
            return {"status": -1, "msg": "Not sufficient resources available", "output": len(self.resources)}
        
        node_conf:List[Dict[str, Any]] = []
        for idx in range(0, required_nodes):
            node_conf.append(self.resources[idx])
            
        print (f'asked: {required_nodes}, allocated: {node_conf}')
        # add the nodes to the list
        self.exposed_add_node(node_conf)
        print (node_conf)
        for node in node_conf:
            self.resources.remove(node)
        return {"status": 0, "msg": "success"}

    def exposed_remove_nodes(self, remove_count) -> Any:
        # can ask each node there load, and may be the nodes with less load can be reomved
        pass

    # API for get/put
    # def exposed_put(self, key, value):
    #     self.locate_key(key).set(key, value)
    #     return {"status": 0, "msg": "success"}

    # def exposed_get(self, key):
    #     ret:Any = self.locate_key(key).get(key)
    #     return {"status": 0, "msg": "success", "output": ret}

    # api : add_resource
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

if __name__ == '__main__':
    print ("starting listening on port 3000...")
    port = 3000
    ThreadedServer(HashRing(nodes), hostname='0.0.0.0', port=port).start()

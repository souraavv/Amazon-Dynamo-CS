import os 
import copy 
import rpyc 
import time 
import redis
import subprocess
import subprocess as sp
 
from hashlib import md5
from bisect import bisect
from pexpect import pxssh
from pprint import pprint
from dotenv import load_dotenv
from os.path import join, dirname
from rpyc.utils.server import ThreadedServer
from typing import List, Set, Dict, Tuple, Callable, Iterator, Union, Optional, Any, Counter

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
    def __init__(self, nodes_conf: List[Dict[str, Any]], spawn_whom:str, **kwargs) -> None:
        self.hash_function: Callable[[str], str] = (lambda key: int(md5(str(key).encode("utf-8")).hexdigest(), 16))
        self.ring: Dict[int, Set(Any, Any, Any)] = {} 
        self.default_vnodes: int = 2
        self.vnodes: int = kwargs.get("vnodes", self.default_vnodes)
        self.hosts: Dict[str, Dict[str, Any]] = {}
        self.keys: List[str] = []
        self.resources: List[Dict[str, Any]] = nodes_conf 
        self.spawn_whom = spawn_whom
        self.N = 4
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
        idx = bisect(self.keys, vnode_hash)
        idx = 0 if (idx == len(self.keys)) else idx
        return (idx - 1, idx)

    def create_ring(self, nodes_conf: List[Dict[str, Any]]) -> None:
        # TODO: first start all the nodes present in thfe conf
        # TODO store their hash keys and send the update to the get_host() 
        # node_Hash -> {hostname, virual name}
        go_to_ring = {}
        for node_conf in nodes_conf:
            hostname = node_conf['hostname']
            port = node_conf['port']
            for who in range(0, int(node_conf["vnodes"])):
                go_to_ring[self.give_hash(f'{hostname}_{who}')] = (hostname, port + who, who)
            
            conn = rpyc.connect(hostname, self.SPAWN_WORKER_PORT)
            conn._config['sync_request_timeout'] = None 
            conn.root.spawn_worker(port=node_conf["port"], vnodes=node_conf["vnodes"], spawn_whom=self.spawn_whom)
        
        time.sleep(10) #TODO: put it to some constant

        for vnode_hash, vnode_info in go_to_ring.items():
            # right and left are considered assuming clockwise movement
            # and back of head is always facing center while moving
            hostname, port, who = vnode_info
            left_idx, right_idx = self.get_neighbours(vnode_hash)
            only_single_node:bool = True
            
            left_node_hash, right_node_hash =  vnode_hash, -1
            if len(self.ring) > 0:
                left_node_hash, right_node_hash = self.keys[left_idx], self.keys[right_idx]
                only_single_node = False 

            new_added = {
                "start_of_range": str(int(left_node_hash) + 1),
                "ip": hostname,
                "port": port,
                "version_number": 0,
                "load": 0,
                "end_of_range": str(vnode_hash) 
            }

            response_to_new_node = {
                            "new_start": str(int(left_node_hash) + 1),
                            "new_end": str(vnode_hash),
                            "new_added": new_added
                        }

            print ("----"*5)
            print (f" New: [{int(new_added['start_of_range']) % 10000}, {int(new_added['end_of_range']) % 10000 }, ip:port({new_added['ip']}, {new_added['port']})]")
            self_url = (hostname, port)
            try:
                conn = rpyc.connect(*self_url) 
                conn._config['sync_request_timeout'] = None 
                primary, replica_nodes = -1, []
                if only_single_node == False:
                    primary = self.ring[right_node_hash]
                    for i in range(min(len(self.ring), self.N)):
                        node_idx = (i + right_idx + 1) % (len(self.ring))
                        secondary = self.ring[self.keys[node_idx]]
                        if primary != secondary:
                            replica_nodes.append(secondary)

                conn.root.init_table(routing_info=response_to_new_node, primary=primary, replica_nodes=replica_nodes)
                if only_single_node == False:
                    response_to_right_node = {
                                    "new_start": str(int(vnode_hash) + 1),
                                    "new_end": str(right_node_hash),
                                    "new_added": new_added
                                }
                
                    if response_to_right_node["new_start"] == response_to_new_node["new_start"]:
                        print ("--------------  They are same ------------------")
                    right_ip, right_port, _ = self.ring[self.keys[right_idx]]
                    print (f" Already: [{int(response_to_right_node['new_start']) % 1000 }, {int(response_to_right_node['new_end']) % 1000}, ip:port({right_ip}, {right_port})]")
                    print (self.keys[right_idx])
                    right_url = (right_ip, right_port)
                    conn = rpyc.connect(*right_url) 
                    conn._config['sync_request_timeout'] = None 
                    conn.root.update_table(response_to_right_node)
            except Exception as e:
                print ("Some thing bad happend in ring ", e)
            self.ring[vnode_hash] = vnode_info #add to ring
            self.keys = sorted(self.ring.keys()) #sort the keys
            print ("----"*5)
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
            
        pprint (f'Asked: {required_nodes}\n Allocated: {node_conf}')
        # add the nodes to the list
        self.exposed_add_node(node_conf)
        for node in node_conf:
            self.resources.remove(node)
        return {"status": 0, "msg": "success"}

    def exposed_remove_nodes(self, remove_count) -> Any:
        # can ask each node there load, and may be the nodes with less load can be reomved
        pass


if __name__ == '__main__':
    types_of_workers:list = ['semantic', 'syntactic']
    worker_type: int = int(input('Which worker you want to initialize ? \n1. Semantic:: Suitable for in-general key-value store\n2. Syntactic::Suitable for username & password\nEnter response(1/2): '))
    print (worker_type)
    if (worker_type != 1) and (worker_type != 2):
        raise ValueError('Invalid argument provided, please provide 1 or 2')
    spawn_whom:str = types_of_workers[worker_type - 1]
    workers_port:int = 3100 if spawn_whom == 'semantic' else 3000
    print (f"Workers will be spawn for {spawn_whom}\nWorker port: {workers_port}")
    nodes = [
        {
            'username': 'sourav',
            'hostname': '10.237.27.95',
            'port': workers_port,
            'vnodes': 6
        },
        {
            'username': 'baadalvm',
            'hostname': '10.17.50.254',
            'port': workers_port,
            'vnodes': 6
        }
    ]
    HashRing_port:int = 3000
    print (f"Hashring started listening on port {HashRing_port}...")
    ThreadedServer(HashRing(nodes, spawn_whom), hostname='0.0.0.0', port=HashRing_port).start()

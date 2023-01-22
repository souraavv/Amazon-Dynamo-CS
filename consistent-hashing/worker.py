import os 
import sys 
import time 
import json
import math
import rpyc 
import socket 
import pickle
import random
import threading
import redis 

from hashlib import md5
from pprint import pprint
from bisect import bisect 
from json import JSONEncoder
from datetime import datetime 
from rpyc.utils.server import ThreadedServer
from typing import List, Set, Dict, Tuple, Callable, Iterator, Union, Optional, Any, Counter
 
'''
gossip_data:
    “end_of_range”: {ip_x, v_id, version_number, load, start_of_range}
    
'''

class VectorClock:
    def __init__(self, ip:str, port:int, version_number:int, load:float, start_of_range:int):
        self.ip:str = ip
        self.version_number:int = version_number
        self.load:float = load 
        self.port:int = port
        self.start_of_range:str = start_of_range
    
    #* From vector clock to dict.
    def to_dict(self) -> Dict:
        return {
            "ip": self.ip,
            "version_number": self.version_number,
            "load": self.load,
            "port": self.port,
            "start_of_range": self.start_of_range 
        }
    #* From dict to vector clock


class Worker(rpyc.Service):
    def __init__(self, port:int, redis_port:int) -> None:
        '''
        Constants (or configurable)
        '''
        self.SUCCESS = 0
        self.FAILURE = -1
        self.GOSSIP_INTERVAL = 5
        self.PING_DOWN_NODE_INTERVAL = 30
        self.REPLICATED_TIMEOUT = 20
        self.READ = 3 # Take it as config from client
        self.WRITE = 2 # Take it as config from client
        self.REDIS_PORT = redis_port 
        self.N = self.READ + self.WRITE + 1 # set it properly
        self.hashmap = f'hash-map-{port}'
        '''
        Locks
        '''
        self.lock_routing_table = threading.Lock()
        self.lock_down_routing_table = threading.Lock()
        '''
        Meta data a worker need to have
        '''
        self.ip:str = 'localhost'
        self.port:int = port
        self.start_of_range:str = "-1" 
        self.end_of_range: str = "-1"
        self.controller_url: Set(str, int) = ('10.194.58.46', 3000) 
        self.routing_table: Dict[str, VectorClock] = dict() #* Will store the routing table of active nodes
        self.down_routing_table: Dict[str, VectorClock] = dict() #* Will store all those entry which are down now
        self.hash_function: Callable[[str], str] = (lambda key: int(md5(str(key).encode("utf-8")).hexdigest(), 16))
        self.requests_log = dict()
        '''
        Redis instance
        '''
        self.rds = redis.Redis(host='localhost', port=self.REDIS_PORT, db=0)

        '''
        Setting up daemon threads
        '''
        gossip_thread = threading.Thread(target=self.start_gossip, args = (), daemon=True)
        ping_down_node_thread = threading.Thread(target=self.thread_ping_down_node, args = (), daemon=True)
        
        gossip_thread.start()
        ping_down_node_thread.start()
 


    def deserialize(self, routing_table):
        deserialize_routing_table = {}
        for hash, vc in routing_table.items():
            deserialize_routing_table[hash] = VectorClock(ip=vc['ip'],port=vc['port'], 
                                                          version_number=vc["version_number"], 
                                                          load=vc["load"], start_of_range=vc["start_of_range"])
        return deserialize_routing_table

    def serialize(self, routing_table):
        serialized_routing_table = {}
        for hash, vc in routing_table.items():
            serialized_routing_table[hash] = vc.to_dict()
        return serialized_routing_table

    # This function will fetch the keys from next 'x' nodes
    def fetch_keys(self):
        #TODO: Reconsillation
        pass 

    def exposed_init_table(self, routing_info):
        print ("New beginning")
        new_added = routing_info['new_added']
        self.routing_table[str(new_added['end_of_range'])] = VectorClock(ip=new_added['ip'],
        port=new_added['port'], version_number=new_added["version_number"],
        load=new_added["load"], start_of_range=new_added["start_of_range"]) 
        self.end_of_range = new_added['end_of_range']
        self.start_of_range = new_added['start_of_range']
        self.fetch_keys() 
        self.print_routing_table()

    def exposed_update_table(self, routing_info:Dict[Any, Any]):
        #*Update your starting point
        print ("before: ", self.routing_table[str(self.end_of_range)].start_of_range)
        self.routing_table[str(self.end_of_range)].start_of_range = routing_info['new_start']
        print ("after: ", self.routing_table[str(self.end_of_range)].start_of_range)
        #* Add the fresh entry to the table
        new_added = routing_info['new_added']
        self.routing_table[str(new_added['end_of_range'])] = VectorClock(ip=new_added['ip'],
        port=new_added['port'], version_number=new_added["version_number"],
        load=new_added["load"], start_of_range=new_added["start_of_range"]) 
        self.print_routing_table()
    
    '''
    This will be background thread which will keep running at some interval
    '''
    def ping(self, ip, port, timeout = 2):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #presumably 
        sock.settimeout(timeout)
        try:
            sock.connect((ip, port))
        except:
            return False
        else:
            sock.close()
            return True

    def ping_thread(self, to_ping):
        print (f"need to ping: by:{self.port}: nodes = ",  to_ping)
        nodes = set(self.routing_table.keys())
        down_nodes = set(self.down_routing_table.keys())    
        for node in to_ping:
            if node in down_nodes:
                vc = self.down_routing_table[str(node)]
                ip, port = vc.ip, int(vc.port)
                response = self.ping(ip, port)
                if response == True:
                    print ("removing from down: ", node)
                    self.lock_down_routing_table.acquire()
                    del self.down_routing_table[str(node)]
                    self.lock_down_routing_table.release()
                    print ("Adding to active: ", node)
                    self.lock_routing_table.acquire()
                    vc.version_number += 1
                    self.routing_table[node] = vc 
                    self.lock_routing_table.release()

            elif node in nodes: 
                vc = self.routing_table[str(node)]
                ip, port = vc.ip, int(vc.port)
                response = self.ping(ip, port)  
                if response == False:
                    print ("removing from active: ", node)
                    self.lock_routing_table().acquire()
                    del self.routing_table[str(node)]
                    self.lock_routing_table.release()
                    print ("Adding to active: ", node)
                    self.lock_down_routing_table.acquire()
                    vc.version_number += 1
                    self.down_routing_table[str(node)] = vc
                    self.lock_down_routing_table.release()

    '''
    This is a thread which will try to ping self down nodes
    '''
    def thread_ping_down_node(self):
        while True:
            if (len(self.down_routing_table)):
                print ("Pinging down nodes...")
                self.ping_thread(list(self.down_routing_table.keys()))
            time.sleep(self.PING_DOWN_NODE_INTERVAL)
            
    '''
    Start the gossip
    '''
    def start_gossip(self):
        while True: 
            time.sleep(self.GOSSIP_INTERVAL)
            self.print_routing_table()
            if (len(self.routing_table) or len(self.down_routing_table)):
                nodes = list(self.routing_table.keys())
                nodes.sort()
                down_nodes = set(self.down_routing_table.keys())
                n:int = len(self.routing_table)
                idx:int = random.randint(0, n - 1)
                talk_to:int = nodes[idx]
                node:VectorClock = self.routing_table[talk_to]
                url = (node.ip, node.port)
                if (node.ip == self.ip) and (node.port == self.port):
                    continue
                
                ask_guest_to_ping = []
                try:
                    conn = rpyc.connect(*url) 
                    conn._config['sync_request_timeout'] = None 
                    '''
                    We are using 2-way communication to, in first go we send
                    both our active and down routing table and then in response
                    we are getting what entry we need to udpate
                    '''        
                    routing_table, down_routing_table = pickle.dumps(self.serialize(self.routing_table)), pickle.dumps(self.serialize(self.down_routing_table))
                    gift_routing_table, gift_down_routing_table, ask_guest_to_ping = conn.root.do_chit_chat(routing_table, down_routing_table)
                    gift_routing_table = self.deserialize(pickle.loads(gift_routing_table))
                    gift_down_routing_table = self.deserialize(pickle.loads(gift_down_routing_table))
                    #* node here represent the hash(or end_of_key)
                    #* update the outdated entry in both routing and down routing table 
                    for node, vc in gift_routing_table.items():
                        self.routing_table[node] = vc 
                    for node, vc in gift_down_routing_table.items():
                        self.down_routing_table[node] = vc 
                    #* ping the nodes.
                except Exception as e: 
                    ask_guest_to_ping.append(nodes[idx])

                if len(ask_guest_to_ping): 
                    ping_thread = threading.Thread(target=self.ping_thread, args = (ask_guest_to_ping, ))
                    ping_thread.start()

    '''
    '''
    def exposed_do_chit_chat(self, routing_table, down_routing_table) -> Any:
        
        print (f"Ok starting chit chat with {self.ip}, {self.port}....")
        routing_table = self.deserialize(pickle.loads(routing_table))
        down_routing_table = self.deserialize(pickle.loads(down_routing_table))

        gift_routing_table: Dict[str, VectorClock] = {}
        gift_down_routing_table: Dict[str, VectorClock] = {}

        guest_active_nodes = set(list(routing_table.keys()))
        guest_down_nodes = set(list(down_routing_table.keys()))

        self_active_nodes = set(list(self.routing_table.keys()))
        self_down_nodes = set(list(self.down_routing_table.keys()))

        '''
        If guest have some down keys which are active in mine, I need to tell guest
        to try to ping over those
        '''
        ask_guest_to_ping = self_active_nodes & guest_down_nodes
        no_need_to_ask_guest = set()
        need_to_ping = guest_active_nodes & self_down_nodes
        no_need_to_ping = set()
        '''
        If guest have some active keys which are in my down_keys, 
        I need to ping those
        '''
        for node in ask_guest_to_ping:
            vc = self.down_routing_table[node]
            ip, port, version_number = vc.ip, vc.port, vc.version_number
            if (version_number < down_routing_table[node].version_number):
                need_to_ping.add(node)
                no_need_to_ask_guest.add(node) 
                 
        ask_guest_to_ping = ask_guest_to_ping - no_need_to_ask_guest
        
        for node in need_to_ping:
            vc = self.down_routing_table[node]
            ip, port, version_number = vc.ip, vc.port, vc.version_number
            #* If I have down record later point in time, then the node which 
            #* asked me to ping, then I will ask that node to ping
            if (version_number > routing_table[node].version_number):
                no_need_to_ping.add(node) # If my version number at down is more updated then the one who is asking me to udpate
                ask_guest_to_ping.add(node)

        need_to_ping = need_to_ping - no_need_to_ping
        
        ping_thread = threading.Thread(target=self.ping_thread, args = (need_to_ping, ))
        ping_thread.start()
        '''
        If entry is active in both, and I'm the most updated I will send this
        as gift to the guest in routing table
        '''
        active_in_both = self_active_nodes & guest_active_nodes
        for node in active_in_both:
            if routing_table[node].version_number < self.routing_table[node].version_number:
                gift_routing_table[node] = self.routing_table[node]
            elif routing_table[node].version_number > self.routing_table[node].version_number:
                self.lock_routing_table.acquire()
                self.routing_table[node] = routing_table[node]
                self.lock_routing_table.release()
        '''
        If entry is down in both then, update your if you have stale, if you
        have much updated then ask guest to udpate
        '''
        down_in_both = self_down_nodes & guest_down_nodes
        for node in down_in_both:
            if down_routing_table[node].version_number < self.down_routing_table[node].version_number:
                gift_down_routing_table[node] = self.down_routing_table[node]
            elif down_routing_table[node].version_number > self.down_routing_table[node].version_number:
                self.lock_down_routing_table.acquire()
                self.down_routing_table[node] = down_routing_table[node]    
                self.lock_down_routing_table.release()    
        '''
        A fresh entry which I haven't seen before
        '''
        surprise_gift_from_guest = guest_active_nodes - self_active_nodes
        for node in surprise_gift_from_guest:
            if node not in self_down_nodes:
                self.lock_routing_table.acquire()
                self.routing_table[node] = routing_table[node]
                self.lock_routing_table.release()

        surprise_gift_from_guest = guest_down_nodes - self_down_nodes
        for node in surprise_gift_from_guest:
            if node not in self_active_nodes:
                self.lock_down_routing_table.acquire()
                self.down_routing_table[node] = down_routing_table[node]
                self.lock_down_routing_table.release()
        '''
        A fresh entry which guest haven't seen before 
        '''
        surprise_gift_to_guest = self_active_nodes - guest_active_nodes
        for node in surprise_gift_to_guest:
            if node not in guest_down_nodes: 
                gift_routing_table[node] = self.routing_table[node]

        surprise_gift_to_guest = self_down_nodes - guest_down_nodes
        for node in surprise_gift_to_guest:
            if node not in guest_active_nodes:
                gift_down_routing_table[node] = self.down_routing_table[node]
        
            
        gift_routing_table = pickle.dumps(self.serialize(gift_routing_table))
        gift_down_routing_table = pickle.dumps(self.serialize(gift_down_routing_table))
        
        print ("done chit-chat")
        return (gift_routing_table, gift_down_routing_table, ask_guest_to_ping)

    '''
    This function will be called by client for getting the appropriate routing
    tables.
    '''
    def exposed_fetch_routing_info(self, key:str, need_serialized=True):
        self_active_nodes = list(self.routing_table.keys())
        self_active_nodes.sort()
        key_hash = str(self.hash_function(key))
        idx = bisect(self_active_nodes, key_hash)
        controller_node = self_active_nodes[idx]
        response = {}
        n = len(self_active_nodes)
        # since it is a ring, we need to do %
        for pos in range(0, math.min(len(self_active_nodes), self.N)):
            node_hash = self_active_nodes[(idx + pos) % n]
            response[node_hash] = self.routing_table[node_hash] 

        if need_serialized:
            response = pickle.dump(self.serialize(response))
        return response, controller_node



    def print_routing_table(self):
        print ("--" *10, "Routing table of ", self.port, "--"* 10)
        print("--"*10, "Active Routing table", "--" * 10)
        for node, vc in self.routing_table.items():
            print (f"[{vc.start_of_range}, {node}]| url = ({vc.ip}, {vc.port}), version = {vc.version_number} | Load = {vc.load}")
        print("--"*10, "Down Routing Table", "--" * 10)
        for node, vc in self.down_routing_table.items():
            print (f"[{vc.start_of_range}, {node}]| url = ({vc.ip}, {vc.port}), version = {vc.version_number} | Load = {vc.load}")   
        print("--"*25)

    
    def exposed_replicas_put(self, key, value):
        retry_count:int = 3
        with self.rds.pipeline() as pipe:
            pipe.watch(self.hashmap)
            while True:
                if retry_count == 0:
                    return {"status": -1, "msg": "Cannot update the redis of replica"}
                try:
                    pipe.multi()
                    pipe.hset(self.hashmap, key, value)
                    pipe.incr(key)
                    pipe.execute()
                    break 
                except redis.WatchError:
                    retry_count -= 1
                    continue
        return {"status": self.SUCCESS, "msg": "Success"}
    
    def exposed_get(self):
        pass 

    

    def exposed_put(self, key, value):
        #* Generate the request id
        now = datetime.datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")
        request_id = now + str(key) 
        request_id = str(self.hash_function(now))  
        #* 
        key_hash = str(self.hash_function(key))
        start, end = self.start_of_range, self.end_of_range
        replica_nodes, controller_node = self.exposed_fetch_routing_info(key=key, need_serialized=False)
        
        if ((start > end and (key_hash >= start or key_hash < start)) or (start <= key_hash and key_hash < end)):
            with self.rds.pipeline() as pipe:
                pipe.watch(self.hashmap)
                while True:
                    try:
                        pipe.multi()
                        pipe.hset(self.hashmap, key, value)
                        pipe.incr(key)
                        pipe.execute()
                        break 
                    except redis.WatchError:
                        continue
            #* controller node is not always the first node.(May be I'm a programmer)
            
            if len(self.routing_table) < self.WRITE:
                return {"status": -1, "msg": "Not enough replicas to write, Please try later!"}


            replicated_on = 0
            #* This is a callback function used by async thread.
            def callback():
                if res.status == "SUCCESS":
                    request_id = res.request_id
                    node = res.node
                    del self.requests_log[request_id][node]
            
            #* Add to the requests_logs, so that background thread can run 
            SEND_RPC = 1
            for node, vc in replica_nodes.items():
                if node != self.end_of_range:
                    self.requests_log[request_id][node] = SEND_RPC
            #* Try to send the async rpyc request to the replica node 
            #* So that they can have key, value stored.
            for node, vc in replica_nodes.items():
                if node != self.end_of_range: #* End of range represent node hash
                    try:
                        conn = rpyc.connect(vc.ip, vc.port)
                        # conn._config['sync_request_timeout'] = None 
                        async_func = rpyc.async_(conn.root.replicated_put)
                        res = async_func(key, value, request_id)
                        res = conn.root.replicated_put(key, value, request_id)
                        res.add_callback(callback)
                    except Exception as e:
                        pass 
            '''
                Now wait for the W to finish the writes and once they are done
                we are free to repsonse to the client for their write
                and our background thread will try to make it to write to N replicas
            '''
            time.sleep(self.REPLICATED_TIMEOUT)
            
            if len(self.requests_log[request_id]) > (self.N - self.WRITE):
                return {"status": self.FAILURE, "msg": "Service unavailable! Retry again"}
            else:
                return {"status": self.SUCCESS, "msg": f"Successfully wrote {key} = {value}", "version_number": -1}
                    

        else:
            #* Return the node which should contain this key, if I'm not the controller
            #* of that key any more/ or was never.
            return replica_nodes, controller_node



if __name__ == '__main__':
    port = int(sys.argv[1])
    redis_port = int(sys.argv[2])
    print (f"Listenting worker at {port}...")
    ThreadedServer(Worker(port, redis_port), hostname='0.0.0.0', port=port, protocol_config={'allow_public_attrs': True}).start()
    
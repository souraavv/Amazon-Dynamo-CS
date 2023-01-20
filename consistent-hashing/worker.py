import rpyc 
import sys 
import os 
import time 
import json
import socket 
import pickle
import random
import threading
from json import JSONEncoder
from pprint import pprint
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



class Encoder(JSONEncoder):
    def default(self, o):
        return o.__dict__

class Worker(rpyc.Service):
    def __init__(self, port:str) -> None:
        self.ip:str = 'localhost'
        self.port:int = port
        self.start_of_range:str = "-1" 
        self.end_of_range: str = "-1"
        self.GOSSIP_INTERVAL = 10
        self.controller_url: Set(str, int) = ('10.194.58.46', 3000) 
        self.routing_table: Dict[str, VectorClock] = {} #* Will store the routing table of active nodes
        self.down_routing_table: Dict[str, VectorClock] = {} #* Will store all those entry which are down now
        gossip_thread = threading.Thread(target=self.start_gossip, args = (), daemon=True)
        gossip_thread.start()

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
        nodes = set(self.routing_table.keys())
        down_nodes = set(self.down_routing_table.keys())    
        for node in to_ping:
            if node in down_nodes:
                vc = self.down_routing_table[str(node)]
                ip, port = vc.ip, int(vc.port)
                response = self.ping(ip, port)
                if response == True:
                    del self.down_routing_table[str(node)]
                    self.routing_table[node] = vc 
            elif node in nodes: 
                vc = self.routing_table[str(node)]
                ip, port = vc.ip, int(vc.port)
                response = self.ping(ip, port)  
                if response == False:
                    del self.routing_table[str(node)]
                    self.down_routing_table[str(node)] = vc


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
        '''
        If guest have some active keys which are in my down_keys, 
        I need to ping those
        '''
        need_to_ping = guest_active_nodes & self_down_nodes
        no_need_to_ping = set()
        for node in ask_guest_to_ping:
            vc = self.down_routing_table[node]
            ip, port, version_number = vc.ip, vc.port, vc.version_number
            if (version_number < down_routing_table[node].version_number):
                need_to_ping.add(node)
                no_need_to_ask_guest.add(node)  
                
        ask_guest_to_ping = ask_guest_to_ping - no_need_to_ping
        
        for node in need_to_ping:
            vc = self.down_routing_table[node]
            ip, port, version_number = vc.ip, vc.port, vc.version_number
            #* If I have down record later point in time, then the node which 
            #* asked me to ping, then I will ask that node to ping
            if (version_number > routing_table[node].version_number):
                no_need_to_ping.add(node) # If my version number at down is more updated then the one who is asking me to udpate
        
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
                self.routing_table[node] = routing_table[node]
        '''
        If entry is down in both then, update your if you have stale, if you
        have much updated then ask guest to udpate
        '''
        down_in_both = self_down_nodes & guest_down_nodes
        for node in down_in_both:
            if down_routing_table[node].version_number < self.down_routing_table[node].version_number:
                gift_down_routing_table[node] = self.down_routing_table[node]
            elif down_routing_table[node].version_number > self.down_routing_table[node].version_number:
                self.down_routing_table[node] = down_routing_table[node]        
        '''
        A fresh entry which I haven't seen before
        '''
        surprise_gift_from_guest = guest_active_nodes - self_active_nodes
        for node in surprise_gift_from_guest:
            self.routing_table[node] = routing_table[node]
        '''
        A fresh entry which guest haven't seen before 
        '''
        surprise_gift_to_guest = self_active_nodes - guest_active_nodes
        for node in surprise_gift_to_guest:
            gift_routing_table[node] = self.routing_table[node]
            
        gift_routing_table = pickle.dumps(self.serialize(gift_routing_table))
        gift_down_routing_table = pickle.dumps(self.serialize(gift_down_routing_table))
        
        return (gift_routing_table, gift_down_routing_table, ask_guest_to_ping)

    
    def print_routing_table(self):
        print ("--" *10, "Routing table of ", self.port, "--"* 10)
        print("--"*10, "Active Routing table", "--" * 10)
        for node, vc in self.routing_table.items():
            print (f"[{vc.start_of_range}, {node}]| url = ({vc.ip}, {vc.port}), version = {vc.version_number} | Load = {vc.load}")
        print("--"*10, "Down Routing Table", "--" * 10)
        for node, vc in self.down_routing_table.items():
            print (f"[{vc.start_of_range}, {node}]| url = ({vc.ip}, {vc.port}), version = {vc.version_number} | Load = {vc.load}")   
        print("--"*25)

    def exposed_get(self):
        pass 

    def exposed_put(self):
        pass 

if __name__ == '__main__':
    port = int(sys.argv[1])
    print (f"Listenting worker at {port}...")
    ThreadedServer(Worker(port), hostname='0.0.0.0', port=port, protocol_config={'allow_public_attrs': True}).start()
    
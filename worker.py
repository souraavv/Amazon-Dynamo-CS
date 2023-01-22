import sys
import rpyc
import time
import copy
import math
import redis
import pickle
import random
import socket
import datetime
import threading
from hashlib import md5
from bisect import bisect
from rpyc.utils.server import ThreadedServer

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


class Worker(rpyc.Service):
    def __init__(self, port, redis_instance_port) -> None:
        self.hostname = 'localhost'
        self.port = port
        self.controller_host = ('10.194.58.175', 3000)
        self.rds = redis.Redis(host = 'localhost', port = redis_instance_port, db=0)
        self.routing_table_lock = threading.Lock()
        self.failed_nodes_lock = threading.Lock()
        self.range = (-1, -1)
        # Routing table: end of range(hash of the node) -> {hostname, port, virtual node id, load, start of range}
        # Same for failed noeds: end of range -> {hostname, port, virtual node id, load, start of range}
        self.routing_table = dict()
        self.failed_nodes = dict()
        self.READ = 3
        self.WRITE = 4
        self.N = self.READ + self.WRITE + 1
        self.hashmap = f'hashmap-{self.port}'
        self.requests_log = dict()
        self.RES_NODES_TO_CLIENT = 4
        self.REPLICATE_TIMEOUT = 20

        # Daemon threads: one for gossip with random node.
        gossip_thread = threading.Thread(target=self.start_gossip, args = (), daemon=True)
        # Other for pinging crashed nodes.
        ping_failed_nodes_thread = threading.Thread(target=self.ping_failed_nodes, args = (), daemon = True)
        self.GOSSIP_TIMEOUT = 20
        self.FAILED_NODES_PING_TIMEOUT = 30
        gossip_thread.start()
        ping_failed_nodes_thread.start()
        # Data to be kept in redis storage
        #* key -> value, version number
        print("Worker initialized")

    def serialize(self, routing_table):
        res_dict = dict()
        for hash, node in routing_table.items():
            res_dict[hash] = node.to_dict()
        return res_dict
    
    def deserialize(self, routing_table):
        res_dict = dict()
        for hash, node in routing_table.items():
            res_dict[hash] = self.from_dict(node)
        return res_dict

    def from_dict(self, vector_obj):
        return VectorClock(vector_obj['hostname'], vector_obj['port'], vector_obj['vid'], vector_obj['load'], vector_obj['start_range'], vector_obj['version_no'])

    def ping_failed_nodes(self):
        while(True):
            time.sleep(self.FAILED_NODES_PING_TIMEOUT)
            self.ping_nodes(copy.deepcopy(self.failed_nodes))


    def hash(self, key):
        return str(int(md5(str(key).encode("utf-8")).hexdigest(), 16))

    def create_or_update_routing_table_entry(self, node_config):
        '''
        create an entry and add it to the routing table
        '''
        return VectorClock(node_config['hostname'], node_config['port'], node_config['vid'], node_config['load'], node_config['start_range'], node_config['version_no'])

    def exposed_update_routing_table(self, new_node_config):
        '''
        On adding node at the position previous to it
        '''
        print('update call received')
        # Add the new node to the routing table
        new_node_end_point = self.hash(new_node_config['hostname'] + '_' + str(new_node_config['port']))
        new_node_config['version_no'] = 1
        self.routing_table[str(new_node_end_point)] = self.create_or_update_routing_table_entry(new_node_config)
        # update your own start range
        self_end = self.range[1]
        self.range = (new_node_end_point + 1, self_end)
        self.routing_table_lock.acquire()
        self.routing_table[str(self_end)].start_range = new_node_end_point + 1
        self.routing_table_lock.release()
        self.print_routing_table()

    def exposed_init_self_key_range(self, node_config):
        print(f'call received, self end range: {node_config["end_range"]}')
        self.range = (node_config['start_range'], node_config['end_range'])
        node_config['version_no'] = 1
        self.routing_table_lock.acquire()
        self.routing_table[str(node_config['end_range'])] = self.create_or_update_routing_table_entry(node_config)
        self.routing_table_lock.release()
        # initialize the thread to fetch from W next workers

    def start_gossip(self):
        while(True):
            self.print_routing_table()
            if len(self.routing_table.keys()) > 0 or len(self.failed_nodes) > 0:
                #TODO get the random node out of the routing table keys
                nodes = list(self.routing_table.keys())
                nodes_len = len(nodes)
                random_index = random.randint(0, nodes_len - 1)
                random_node_end = nodes[random_index]
                random_node = self.routing_table[str(random_node_end)]
                if random_node_end == self.range[1]:
                    continue
                # TODO send the entire routing table to the random node along with the failed nodes(for this node).
                routing_table = pickle.dumps(self.serialize(self.routing_table))
                failed_nodes = pickle.dumps(self.serialize(self.failed_nodes))
                to_ping_nodes = dict()
                try:
                    res_active_nodes, res_failed_nodes = rpyc.connect(random_node.hostname, random_node.port).root.gossip(routing_table, failed_nodes)
                    res_active_nodes = self.deserialize(pickle.loads(res_active_nodes))
                    res_failed_nodes = self.deserialize(pickle.loads(res_failed_nodes))
                    for hash, node in res_active_nodes.items():
                        if hash in self.failed_nodes:
                            to_ping_nodes[hash] = node
                        # if node is in my active node list, update it
                        else:
                            self.routing_table_lock.acquire()
                            self.routing_table[str(hash)] = node
                            self.routing_table_lock.release()
                    for hash, node in res_failed_nodes.items():
                        if hash in self.routing_table:
                            to_ping_nodes[hash] = node
                        elif hash not in self.failed_nodes:
                            to_ping_nodes[hash] = node
                        else:
                            self.failed_nodes_lock.acquire()
                            self.failed_nodes[str(hash)] = node
                            self.failed_nodes_lock.release()
                except Exception as e:
                    to_ping_nodes[random_node_end] = random_node
                # initiate the thread to ping the nodes
                if len(to_ping_nodes) > 0:
                    ping_thread = threading.Thread(target=self.ping_nodes, args = (to_ping_nodes,))
                    ping_thread.start()
            time.sleep(self.GOSSIP_TIMEOUT)

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

    def ping_nodes(self, nodes):
        for hash, node in nodes.items():
            res = self.ping(node.hostname, node.port)
            if res == True:
                self.routing_table_lock.acquire()
                self.routing_table[str(hash)] = node
                self.routing_table_lock.release()
                if hash in self.failed_nodes:
                    self.failed_nodes_lock.acquire()
                    del self.failed_nodes[str(hash)]
                    self.failed_nodes_lock.release()
            else:
                self.failed_nodes_lock.acquire()
                self.failed_nodes[str(hash)] = node
                self.failed_nodes_lock.release()
                if hash in self.routing_table:
                    self.routing_table_lock.acquire()
                    del self.routing_table[str(hash)]
                    self.routing_table_lock.release()

    def print_routing_table(self):
        print("-"*10, "Active Routing table", "-" * 10)
        for node, vc in self.routing_table.items():
            print (f"[{vc.start_range}, {node}]| url = ({vc.hostname}, {vc.port}), version = {vc.version_no} | Load = {vc.load}")
        print("-"*10, "Failed Nodes", "-" * 10)
        for node, vc in self.failed_nodes.items():
            print (f"[{vc.start_range}, {node}]| url = ({vc.hostname}, {vc.port}), version = {vc.version_no} | Load = {vc.load}")   
        print("--"*25)
        
    def exposed_gossip(self, routing_table, failed_nodes):
        '''
        Check for the failed nodes, try to ping for each node in failed_nodes, if ping fails for any node, then put that
        node into your failed_nodes list.
        For each entry in the routing_table, check if data in your own routing table is deprecated or not. 
        If deprecated, then update your routing table with new one
        else if the data in the routing_table parameter is deprecated, 
        then append it to the dictionary which is to be returned to the gossip-initiating node. Also return your failed nodes
        '''
        print(f'Gossip received')
        routing_table = self.deserialize(pickle.loads(routing_table))
        failed_nodes = self.deserialize(pickle.loads(failed_nodes))
        return_nodes = dict()
        return_failed_nodes = dict()
        to_ping_nodes = dict()
        for hash, node in routing_table.items():
            if hash in self.routing_table:
                self_node = self.routing_table[str(hash)]
                # if received version number is less than the one held by me, then add it to return_nodes
                if node.version_no < self_node.version_no:
                    return_nodes[hash] = self_node
                elif node.version_no > self_node.version_no:
                    self.routing_table_lock.acquire()
                    self.routing_table[str(hash)] = node
                    self.routing_table_lock.release()
            # If node is not available in your routing table
            else:
                # If the node is in your failed nodes list
                if hash in self.failed_nodes:
                    # If you have updated info that the node is down as compared to caller, then send it to the caller
                    if node.version_no < self.failed_nodes[str(hash)].version_no:
                        return_failed_nodes[hash] = self.failed_nodes[str(hash)]
                        continue
                    # If the caller has the updated version, then add to ping nodes
                    to_ping_nodes[hash] = node
                # if not available in any list, add it to your routing table
                else:
                    self.routing_table_lock.acquire()
                    self.routing_table[str(hash)] = node
                    self.routing_table_lock.release()
        for hash, node in failed_nodes.items():
            if hash in failed_nodes:
                # if received version number is less than the one held by me, then add it to return_failed_nodes
                if node.version_no < self.failed_nodes[str(hash)].version_no:
                    return_failed_nodes[hash] = self.failed_nodes[str(hash)].version_no
                elif node.version_no > self.failed_nodes[str(hash)].version_no:
                    self.failed_nodes_lock.acquire()
                    self.failed_nodes[str(hash)] = node
                    self.failed_nodes_lock.release()
            else:
                # if it's in my routing table
                if hash in self.routing_table:
                    # if I have updated version, send it to the caller
                    if node.version_no < self.routing_table[str(hash)].version_no:
                        return_nodes[hash] = node
                        continue
                    # delete the node from the routing table and add it to the failed nodes
                    to_ping_nodes[hash] = node

                else:
                    self.failed_nodes_lock.acquire()
                    self.failed_nodes[str(hash)] = node
                    self.failed_nodes_lock.release()

        # send the nodes which are available in your routing table but not in the gossip initiator's table
        nodes_diff = list(set(self.routing_table.keys()) - set(routing_table.keys()) - set(failed_nodes.keys()))
        for hash in nodes_diff:
            return_nodes[hash] = self.routing_table[str(hash)]
        # send the nodes which are available in your failed_nodes list but not in the initiator's
        failed_nodes_diff = list(set(self.failed_nodes.keys()) - set(routing_table.keys()) - set(failed_nodes.keys()))
        for hash in failed_nodes_diff:
            return_failed_nodes[hash] = self.failed_nodes[str(hash)]
        # initiate the thread to ping the nodes
        ping_thread = threading.Thread(target=self.ping_nodes, args = (to_ping_nodes,))
        ping_thread.start()
        return_nodes = pickle.dumps(self.serialize(return_nodes))
        return_failed_nodes = pickle.dumps(self.serialize(return_failed_nodes))
        return return_nodes, return_failed_nodes
    
    def exposed_replicate_put(self, key, value):
        retries_count = 0
        with self.rds.pipeline() as pipe:
            pipe.watch(self.hashmap)
            while retries_count < 3:
                try:
                    pipe.multi()
                    pipe.hset(self.hashmap, key, value)
                    pipe.incr(key)
                    pipe.execute()
                    break
                except redis.WatchError as e:
                    continue
        return {"status": 1, "message": "Success"}

        
    def exposed_put(self, key, value):
        key_hash = self.hash(key)
        start, end = self.range

        replica_nodes, controller_key = self.exposed_get_routing_table_info(key, is_rpc = False)
        if (key_hash >= start and key_hash <= end) or (start > end and (key_hash >= start and key_hash <= end)):
            # TODO store the key into redis and write it to next W workers in list
            with self.rds.pipeline() as pipe:
                pipe.watch(self.hashmap)
                while True:
                    try:
                        pipe.multi()
                        pipe.hset(self.hashmap, key, value)
                        pipe.incr(key)
                        pipe.execute()
                        break
                    except redis.WatchError as e:
                        continue
            
            # Generating a unique request id
            now = datetime.datetime.now()
            datetime_str = now.strftime("%Y-%m-%d %H:%M:%S")
            request_id = datetime_str + key
            request_hash = self.hash(request_id)

            # RPC response callback
            def response_callback(res):
                if res['status'] == 1:
                    request_hash = res['request_hash']
                    node_hash = res['node_hash']
                    del self.requests_log[request_hash][node_hash]
                

            # Keeping the requests in the log to collect the status
            self.requests_log[request_hash] = dict()

            if len(self.routing_table.keys()) < self.WRITE:
                return {"status": 0, "message": "Please try again later, not enough nodes to replicate"}
            
            for node_hash, replica_node in replica_nodes.items():
                if node_hash == self.range[1]:
                    continue
                self.requests_log[request_hash][node_hash] = 1

            for node_hash, replica_node in replica_nodes.items():
                # Ignore sending to self
                if node_hash == self.range[1]:
                    continue
                try:
                    conn = rpyc.connect(replica_node.hostname, replica_node.port)
                    async_func = rpyc.async_(conn.root.replica_put)
                    res = async_func(key, value, request_hash)
                    res.add_callback(response_callback)
                except Exception as e:
                    pass

            # wait for self.WRITE nodes to send response and then send response to the client
            # background thread will handle the other responses and sending to hinted replica
            time.sleep(self.REPLICATE_TIMEOUT)
            if len(self.requests_log[request_hash]) > (self.N - self.WRITE):
                return {"status": 1, "message": "Success"}
            else:
                return {"status": 0, "message": "Failure"}


        else:
            # return the nodes which would be potentially helding the key
            return replica_nodes, controller_key

    def exposed_get(self, key):
        key_hash = self.hash(key)
        start, end = self.range
        if key_hash:
            pass
    
    def exposed_get_routing_table_info(self, key, is_rpc = True):
        key_hash = self.hash(key)
        node_keys = sorted(list(self.routing_table.keys()))
        pos = bisect(node_keys, key_hash)
        res_routing_table = dict()
        for i in range(0, math.min(len(self.routing_table), self.RES_NODES_TO_CLIENT)):
            node_hash = node_keys[(pos + i) % len(node_keys)]
            res_routing_table[node_hash] = self.routing_table[node_hash]
            if i == 0:
                controller_key = node_hash
        if is_rpc:
            res_routing_table = pickle.dumps(self.serialize(res_routing_table))
        return res_routing_table, controller_key

        

if __name__ == '__main__':
    port = int(sys.argv[1])
    redis_instance_port = int(sys.argv[2])
    print(f"Worker listening on port: {port}")
    t = ThreadedServer(Worker(port, redis_instance_port), hostname = '0.0.0.0', port = port, protocol_config={'allow_public_attrs': True})
    t.start()
import sys
import rpyc
import time
import copy
import redis
import pickle
import random
import socket
import datetime
import threading
from hashlib import md5
from bisect import bisect
from datetime import datetime
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
        # HASHMAP name in the redis
        self.HASHMAP = f'HASHMAP-{self.port}'
        # Log of requests for bg thread
        self.requests_log = dict()
        self.get_requests_log = dict()
        self.READ = 3
        self.WRITE = 4
        self.N = self.READ + self.WRITE + 1
        self.FAILURE_STATUS = 0
        self.SUCCESS_STATUS = 1
        self.IGNORE_STATUS = 2
        self.INVALID_RESOURCE = 3
        self.REPLICATE_MAX_COUNT = 2
        self.REDIS_RETRIES_COUNT = 3
        self.EXPIRE = 3

        # Daemon threads:
        
        # one for gossip with random node.
        gossip_thread = threading.Thread(target=self.start_gossip, args = (), daemon=True)
        # Other for pinging crashed nodes.
        ping_failed_nodes_thread = threading.Thread(target=self.ping_failed_nodes, args = (), daemon = True)
        # Other for replicating the put requests over the nodes
        replicate_put_thread = threading.Thread(target=self.replicate_put_thread, args=(), daemon=True)

        self.GOSSIP_TIMEOUT = 10
        self.REPLICATE_TIMEOUT = 20
        self.FAILED_NODES_PING_TIMEOUT = 30
        self.REPLICATE_PUT_THREAD_TIMEOUT = 10000

        gossip_thread.start()
        ping_failed_nodes_thread.start()
        replicate_put_thread.start()
        # Data to be kept in redis storage
        #* key -> value, version number

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
        # Add the new node to the routing table
        new_node_end_point = self.hash(new_node_config['hostname'] + '_' + str(new_node_config['port']))
        new_node_config['version_no'] = 1
        self.routing_table[str(new_node_end_point)] = self.create_or_update_routing_table_entry(new_node_config)
        # update your own start range
        my_start_point = str(int(new_node_end_point) + 1)
        self_end = self.range[1]
        self.range = (my_start_point, self_end)
        self.routing_table_lock.acquire()
        self.routing_table[str(self_end)].start_range = my_start_point
        self.routing_table_lock.release()
        # self.print_routing_table()

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
            # self.print_routing_table()
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

    def replicate_put_thread(self):
        piggyback_requests = dict()
        for request_id, nodes in self.requests_log.items():
            to_remove_nodes = []
            key, value, timestamp = self.requests_log[request_id]['info']
            for node_hash, status in nodes.items():
                if node_hash == 'info':
                    continue
                state, retry_count = status
                if state == self.FAILURE_STATUS:
                    if retry_count == self.REPLICATE_MAX_COUNT:
                        to_remove_nodes.append(node_hash)
                        continue
                    if node_hash not in piggyback_requests:
                        piggyback_requests[node_hash] = []
                    self.requests_log[request_id][node_hash] = (state, retry_count + 1)
                    piggyback_requests[node_hash].append({
                        "key": key,
                        "value": value,
                        "timestamp": timestamp,
                        "request_id": request_id
                    })
            for node in to_remove_nodes:
                del self.requests_log[request_id][node_hash]
            if request_id not in self.requests_log:
                self.requests_log[request_id] = {"info": None, 'replicated_on': 0}
            self.requests_log[request_id]['info'] = (key, value, timestamp)

        def callback(res):
            success_requests, ignored_requests, node_hash = res.value
            for request in success_requests:
                self.requests_log[request_id]['replicated_on'] += 1
                del self.requests_log[request][node_hash]
            for request in ignored_requests:
                del self.requests_log[request][node_hash]

        
        responses = []

        for node, requests in piggyback_requests.items():
            hostname, port = self.routing_table[node].hostname, self.routing_table[node].port
            conn = rpyc.connect(hostname, port)
            async_func = rpyc.async_(conn.root.bulk_put)
            res = async_func(requests)
            res.add_callback(callback)
            res.set_expiry(self.EXPIRE)
            responses.append(res)
        
        self.wait_for_responses(responses, len(responses))


    def exposed_bulk_put(self, requests):
        success_requests = []
        ignored_requests = []
        for request in requests:
            res = self.exposed_replicate_put(request['key'], request['value'], request['request_id'], request['timestamp'])
            if res.status == self.SUCCESS_STATUS:
                success_requests.append(request['request_id'])
            elif res.status == self.IGNORE_STATUS:
                ignored_requests.append(request['request_id'])
        return success_requests, ignored_requests, self.range[1]

    def print_routing_table(self):
        print("-"*10, "Active Routing table", "-" * 10)
        self.routing_table_lock.acquire()
        for node, vc in self.routing_table.items():
            print (f"[{vc.start_range}, {node}]| url = ({vc.hostname}, {vc.port}), version = {vc.version_no} | Load = {vc.load}")
        self.routing_table_lock.release()

        print("-"*10, "Failed Nodes", "-" * 10)
        self.failed_nodes_lock.acquire()
        for node, vc in self.failed_nodes.items():
            print (f"[{vc.start_range}, {node}]| url = ({vc.hostname}, {vc.port}), version = {vc.version_no} | Load = {vc.load}")   
        self.failed_nodes_lock.release()
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
    
    def exposed_replicate_put(self, key, value, request_id, timestamp):
        print(f'Received replicate put for {key} = {value}')
        store_timestamp = None
        if self.rds.get(key) == None:
            store_timestamp = timestamp
        else:
            store_timestamp = float(self.rds.get(key))
        
        if store_timestamp > timestamp:
            return {"status": self.IGNORE_STATUS, "message": "I'm more updated"}

        retries_count = 0
        with self.rds.pipeline() as pipe:
            pipe.watch(self.HASHMAP)
            while True:
                if retries_count == self.REDIS_RETRIES_COUNT:
                    return {"status": self.FAILURE_STATUS, "message": "Not able to write to redis"}
                try:
                    pipe.multi()
                    pipe.hset(self.HASHMAP, key, value)
                    pipe.set(key, timestamp)
                    pipe.execute()
                    break
                except redis.WatchError as e:
                    retries_count += 1
                    continue
        return {"status": self.SUCCESS_STATUS, "message": "Success", "request_id": request_id, "node_hash": self.range[1]}

    def get_request_id(self, key):
        curr_time = time.time()
        request_id = str(curr_time) + key
        return self.hash(request_id), curr_time
    
    def wait_for_responses(self, responses, count_responses):
        count_success_responses = 0
        count_error_responses = 0
        while True:
            for response in responses:
                if count_success_responses >= count_responses:
                    return {"status": self.SUCCESS_STATUS, "message": "Success"}
                if count_error_responses > self.N - count_responses:
                    return {"status": self.FAILURE_STATUS, "message": "Failure. Please try again"}
                if response.ready:
                    res = response.value
                    print(f'success responses = {count_success_responses}')
                    print(f'error responses = {count_error_responses}')
                    if not response.error and res['status'] == self.SUCCESS_STATUS:
                        count_success_responses += 1
                        print(f'Success responses = {count_success_responses}')
                    else:
                        print(f'Error responses = {count_error_responses}')
                        count_error_responses += 1

    def exposed_put(self, key, value):
        # print(f'Received put request: {key}: {value}')
        key_hash = self.hash(key)
        start, end = self.range
        # print(f'Put request call received by = {self.port}')
        # print(f'Start range = {self.range[0]}, end range = {self.range[1]}, hash = {key_hash}')
        replica_nodes, controller_key = self.exposed_get_routing_table_info(key, is_rpc = False)
        if (key_hash >= start and key_hash <= end) or (start > end and (key_hash >= start and key_hash <= end)):
            # print(f"It's my request: {key} = {value}")
            # Generating a unique request id
            request_id, timestamp = self.get_request_id(key)
            with self.rds.pipeline() as pipe:
                pipe.watch(self.HASHMAP)
                while True:
                    try:
                        pipe.multi()
                        pipe.hset(self.HASHMAP, key, value)
                        pipe.set(key, timestamp)
                        pipe.execute()
                        break
                    except redis.WatchError as e:
                        continue
            

            # Keeping the requests in the log to collect the status

            if len(self.routing_table.keys()) < self.WRITE:
                return {"status": self.FAILURE_STATUS, "message": "Please try again later, not enough nodes to replicate"}

            # print(f'Initializing request')
            if request_id not in self.requests_log:
                self.requests_log[request_id] = {'info': None, 'replicated_on': 0}
            self.requests_log[request_id]['info'] = (key, value, timestamp)
            
            for node_hash, replica_node in replica_nodes.items():
                if node_hash == self.range[1]:
                    continue
                self.requests_log[request_id][node_hash] = (1, 0)


            # RPC response callback
            def response_callback(response):
                res = response.value
                request_id = res['request_id']
                node_hash = res['node_hash']
                # print(f'Received response from node: {node_hash}')
                del self.requests_log[request_id][node_hash]
                if res['status'] == self.SUCCESS_STATUS:
                    self.requests_log[request_id]['replicated_on'] += 1
                # if the node ignored the put request, just update it's status, so that the background thread ignores
                # sending the request to it again, can't delete over here as the condition for returning to client wil
                # get true before replicating successfully on W nodes.

            responses = []
            for node_hash, replica_node in replica_nodes.items():
                # Ignore sending to self
                if node_hash == self.range[1]:
                    continue
                try:
                    print(f'sending put request to node: {node_hash}')
                    conn = rpyc.connect(replica_node.hostname, replica_node.port)
                    async_func = rpyc.async_(conn.root.replicate_put)
                    res = async_func(key, value, request_id, timestamp)
                    res.add_callback(response_callback)
                    res.set_expiry(self.EXPIRE)
                    responses.append(res)
                except Exception as e:
                    print(f'Exception = {e}')
                    pass

            # wait for self.WRITE nodes to send response and then send response to the client
            # background thread will handle the other responses and sending to hinted replica
            resp = self.wait_for_responses(responses, self.WRITE)
            if resp['status'] == self.SUCCESS_STATUS:
                return {"status": self.SUCCESS_STATUS, "message": f'Successfully wrote {key} = {value}'}

        else:
            print(f'Replica nodes = {replica_nodes}')
            # return the nodes which would be potentially helding the key
            return {"status": self.INVALID_RESOURCE, "controller_key": controller_key, "replica_nodes": replica_nodes}
        
    def exposed_get_key(self, key, timestamp, request_id):
        print(f'get key received for {key}')
        try:
            my_timestamp  = self.rds.get(key)
            my_value = self.rds.hget(self.HASHMAP, key)
            return {"status": self.SUCCESS_STATUS, "request_id": request_id, "timestamp": my_timestamp, "value": my_value, "node_hash": self.range[1]}
        except Exception as e:
            print(f'Error = {e}')

    def exposed_get(self, key):
        key_hash = self.hash(key)
        replica_nodes, controller_key = self.exposed_get_routing_table_info(key, is_rpc = False)
        start, end = self.range
        print(f'Received request for get')
        if (key_hash >= start and key_hash <= end) or (start > end and (key_hash >= start and key_hash <= end)):
            print(f"It's my request = {key}")
            try:
                request_id, _ = self.get_request_id(key)
                count_responses = 0
                fresh_value = self.rds.hget(self.HASHMAP, key)
                fresh_timestamp = self.rds.get(key)
                print(f'request id = {request_id}')
                self.get_requests_log[str(request_id)] = {"value": fresh_value, "timestamp": fresh_timestamp, "count_responses": count_responses}
                self.get_requests_log[request_id + "__NODE__"] = []
            except Exception as e:
                print(f"Error = {e}")
            print('got from myself')

            # RPC response callback
            def response_callback(response):
                try:
                    res = response.value
                    request_id = res['request_id']
                    print(f'get requests log = {self.get_requests_log.keys()}')
                    print(f'Present: {str(request_id) in self.get_requests_log}')
                    curr_timestamp = self.get_requests_log[str(request_id)]['timestamp']
                    node_hash = res['node_hash']
                    res_timestamp = res['timestamp']
                    res_value = res['value']
                    print(f'Received response from {node_hash}')
                    if curr_timestamp < res_timestamp:
                        self.get_requests_log[request_id]['value'] = res_value
                        self.get_requests_log[request_id]['timestamp'] = res_timestamp
                        self.get_requests_log[request_id + "__NODE__"] = [node_hash]
                    elif curr_timestamp == res_timestamp:
                        self.get_requests_log[request_id + "__NODE__"].append(node_hash)
                    print(f'Count responses = {self.get_requests_log[request_id]["count_responses"]}')
                    self.get_requests_log[request_id]['count_responses'] += 1
                except Exception as e:
                    print(f"Error in callback: {e}")
            print(f'sending requests to {len(replica_nodes)} nodes')
            responses = []
            for node_hash, replica_node in replica_nodes.items():
                if node_hash != self.range[1]:
                    print(f'Sending request to {node_hash}')
                    conn = rpyc.connect(replica_node.hostname, replica_node.port)
                    async_func = rpyc.async_(conn.root.get_key)
                    res = async_func(key, fresh_timestamp, request_id)
                    res.add_callback(response_callback)
                    res.set_expiry(self.EXPIRE)
                    responses.append(res)

            resp = self.wait_for_responses(responses, self.READ)
            print(f'wait for responses response = {resp}')
            try:
                if resp['status'] == self.SUCCESS_STATUS:
                    print('success status')
                    return {"status": self.SUCCESS_STATUS, "value": self.get_requests_log[request_id]['value']}
            except Exception as e:
                print(f'Error in sending response = {e}')
            
        else:
            return {'status': self.INVALID_RESOURCE, 'controller_key': controller_key, 'replica_nodes': replica_nodes}
    
    def exposed_get_routing_table_info(self, key, is_rpc = True):
        if is_rpc:
            print(f'Received call for get routing table info')
        key_hash = self.hash(key)
        node_keys = sorted(list(self.routing_table.keys()))
        pos = bisect(node_keys, key_hash)
        res_routing_table = dict()
        for i in range(0, min(len(self.routing_table), self.N)):
            node_hash = node_keys[(pos + i) % len(node_keys)]
            res_routing_table[node_hash] = self.routing_table[node_hash]
            if i == 0:
                controller_key = node_hash
        if is_rpc:
            res_routing_table = pickle.dumps(self.serialize(res_routing_table))
        print(f'returning routing table')
        return res_routing_table, controller_key

        

if __name__ == '__main__':
    port = int(sys.argv[1])
    redis_instance_port = 6379
    print(f"Worker listening on port: {port}")
    t = ThreadedServer(Worker(port, redis_instance_port), hostname = '0.0.0.0', port = port, protocol_config={'allow_public_attrs': True})
    t.start()
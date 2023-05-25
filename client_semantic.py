import rpyc
import time
import pickle
import random
import threading
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


class Client(rpyc.Service):
    def __init__(self, init_nodes) -> None:
        self.init_nodes = init_nodes
        # Dict: {key -> (controller's hash)}
        self.keys_cache = dict()

        # Dict: {key -> latest version number}
        self.version_number = dict()

        # Dict: {node's hash -> (VectorClock, last_updated_time)}
        self.worker_nodes_cache = dict()

        # Sorted list of nodes
        self.worker_nodes = []

        # Locks for data structures
        self.worker_nodes_cache_lock = threading.Lock()
        self.worker_nodes_lock = threading.Lock()

        self.READ = 2
        self.WRITE = 2
        self.N = 8

        self.FAILURE_STATUS = 0
        self.SUCCESS_STATUS = 1
        self.INVALID_RESOURCE = 3
        self.CACHE_TIMEOUT = 500

        self.RETRIES = 3

        self.remove_from_cache_daemon = threading.Thread(target=self.remove_from_cache, args=(), daemon=True)
        self.remove_from_cache_daemon.start()


    def remove_from_cache(self):
        while True:
            time.sleep(self.CACHE_TIMEOUT)
            curr_time = time.time()
            to_remove_nodes = []
            for hash, node in self.worker_nodes_cache.items():
                if (curr_time - node['updated_time']) > self.CACHE_TIMEOUT:
                    to_remove_nodes.append(hash)

            self.worker_nodes_cache_lock.acquire()
            self.worker_nodes_lock.acquire()
            for node in to_remove_nodes:
                del self.worker_nodes_cache[node]
                self.worker_nodes.remove(hash)
            self.worker_nodes_lock.release()
            self.worker_nodes_cache_lock.release()
                
    def deserialize(self, routing_table):
        res_dict = dict()
        for hash, node in routing_table.items():
            res_dict[hash] = self.from_dict(node)
        return res_dict

    def from_dict(self, vector_obj):
        return VectorClock(vector_obj['hostname'], vector_obj['port'], vector_obj['vid'], vector_obj['load'], vector_obj['start_range'], vector_obj['version_no'])


    def update_cache(self, key, replica_nodes, controller_key):
        curr_time = time.time()
        for hash, node in replica_nodes.items():
            # For now, checking for equal version number too as not worked on versions
            #TODO versions
            if node not in self.worker_nodes_cache or self.worker_nodes_cache[hash]['vector_clock'].version_no <= node.version_no:
                self.worker_nodes_cache_lock.acquire()
                self.worker_nodes_cache[hash] = {"vector_clock": node, "updated_time": curr_time}
                self.worker_nodes_cache_lock.release()
                # find the position of the node in the sorted list and insert there
                if node not in self.worker_nodes_cache:
                    self.worker_nodes_lock.acquire()
                    self.worker_nodes.append(hash)
                    
                    self.worker_nodes.sort()
                    self.worker_nodes_lock.release()

        self.keys_cache[key] = controller_key

    def get_routing_table_info(self, key):
        while(True):
            try:
                #TODO fetch it from self.worker_nodes
                random_node_index = random.randint(0, len(self.init_nodes) - 1)
                random_node = self.init_nodes[random_node_index]
                # random_node = self.worker_nodes_cache[random_node_hash]
                print(f'Request sent for getting routing table info: {random_node["hostname"]}: {random_node["port"]}')
                res_routing_table, controller_key = rpyc.connect(random_node['hostname'], random_node['port']).root.get_routing_table_info(key)
                res_routing_table = self.deserialize(pickle.loads(res_routing_table))
                # print(f'Received nodes = {res_routing_table} for key = {key}')
                # check if result contains the routing table
                self.update_cache(key, res_routing_table, controller_key)
                break

            except Exception as e:
                print(f'error: {e}')
                continue

    def check_stale_cache(self, key):
        curr_time = time.time()
        key_not_present = key not in self.keys_cache
        if not key_not_present:
            check_stale_metadata = (curr_time - self.worker_nodes_cache[self.keys_cache[key]]['updated_time']) > self.CACHE_TIMEOUT
            return check_stale_metadata
        return key_not_present
    
    def get_nodes_holding_key(self, key):
        if self.check_stale_cache(key):
            # print('stale cache TRUE')
            self.get_routing_table_info(key)
        pos = bisect(self.worker_nodes, self.keys_cache[key])
        pos = pos - 1 if pos > 0 else 0
        # try on the nodes and if succeeds for any one break over there only
        key_contained_by = []
        for i in range(0, min(len(self.worker_nodes), self.N)):
            key_contained_by.append(self.worker_nodes[(pos + i) % len(self.worker_nodes)])
        return key_contained_by

    def exposed_get(self, key):
        print(f'Get request for: {key}')
        retry_count = 0

        while retry_count < self.RETRIES:
            key_contained_by = self.get_nodes_holding_key(key)
            break_reason = ''
            res = None
            retry_count += 1

            for node_hash in key_contained_by:
                try:
                    node_vc = self.worker_nodes_cache[node_hash]['vector_clock']
                    print(f'Trying on {node_vc.hostname}: {node_vc.port}')
                    conn = rpyc.connect(node_vc.hostname, node_vc.port)
                    res = rpyc.connect(node_vc.hostname, node_vc.port).root.get(key)
                    conn._config['sync_request_timeout'] = 40
                    conn.root.get(key)
                    print(f'Received response: {res}')
                    if res['status'] == self.SUCCESS_STATUS:
                        return {"status": self.SUCCESS_STATUS, "value": res['value']}
                    elif res['status'] == self.INVALID_RESOURCE:
                        break_reason = 'invalid_resource'
                        break
                except Exception as e:
                    print(f'error = {e}')
                    pass

            if break_reason == 'invalid_resource':
                replica_nodes = res['replica_nodes']
                controller_key = res['controller_key']
                self.update_cache(key, replica_nodes, controller_key)
            # else:
            #     break
        return {"status": self.FAILURE_STATUS, "message": "Please try again"}
    
    def exposed_put(self, key, value):
        # print('------------------------------- Put request --------------------------------------------------')
        if key not in self.version_number:
            self.version_number[key] = 0
        print(f'Put request for key = {key}')
        retry_count = 0
        while retry_count < self.RETRIES:
            key_contained_by = self.get_nodes_holding_key(key)
            break_reason = ''
            res = None
            retry_count += 1
            for node_hash in key_contained_by:
                try:
                    allow_replicas = (node_hash != self.keys_cache[key])
                    node_vc = self.worker_nodes_cache[node_hash]['vector_clock']

                    print(f'Trying put:- {key}: {value} on {node_vc.hostname}: {node_vc.port}')

                    conn = rpyc.connect(node_vc.hostname, node_vc.port)
                    conn._config['sync_request_timeout'] = 40
                    res = conn.root.put(key, value, allow_replicas)

                    if res['status'] == self.SUCCESS_STATUS:
                        self.version_number[key] += 1
                        return {"status": self.SUCCESS_STATUS, "message": "Success"}
                    elif res['status'] == self.INVALID_RESOURCE:
                        break_reason = 'invalid_resource'
                        break
                except Exception as e:
                    print(f'Error in put: {e}')
                    pass
            if break_reason == 'invalid_resource':
                replica_nodes = res['replica_nodes']
                controller_key = res['controller_key']
                self.update_cache(key, replica_nodes, controller_key)
            # else:
            #     break
        return {"status": self.FAILURE_STATUS, "message": "Please try again"}

if __name__ == "__main__":
    nodes = [{
        "hostname": '10.237.27.245',
        "port": 3100
    },
    {
        "hostname": '10.17.10.15',
        "port": 3100
    },
    {
        "hostname": '10.237.27.245',
        "port": 3101
    },
    {
        "hostname": '10.17.10.15',
        "port": 3101
    }
    ]
    t = ThreadedServer(Client(nodes), hostname = '0.0.0.0', port = 6002, protocol_config = {'allow_public_attrs': True})
    t.start()

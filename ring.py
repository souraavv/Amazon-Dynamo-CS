import os
import time
import rpyc
import redis
from pexpect import pxssh
from dotenv import load_dotenv
from os.path import join, dirname
import subprocess as sp
from hashlib import md5
from bisect import bisect
from rpyc.utils.server import ThreadedServer


# resource:
## hostname
## port
## redis instance

# node:
## hostname
## port
## redis instance
## vnode count


class Hashring(rpyc.Service):
    '''
    Constructor
    Initialize the list of resources and all other attributes
    '''
    def __init__(self, current_worker_type, initial_port) -> None:
        # resources: list: containing the object hostname and list of virtual nodes' ports
        self.resources = [
            {
                'hostname': '10.237.27.245',
                'username': 'chintan',
                'port': initial_port,
            },
            {
                'hostname': '10.17.10.15',
                'username': 'baadalvm',
                'port': initial_port,
            }
        ]
        #* nodes: dict(): host name -> config : actual hosts
        self.nodes = dict()
        #* ring: dict(): vnode's hash -> config : virtual hosts
        self.ring = dict()
        #* keys: hashes of the nodes arranged in sorted manner denoting the positions in the ring
        self.keys = []
        self.N = 4
        #! check if needed vnodes count
        self.VNODES_COUNT = 6

        self.SPAWNING_PROC_PORT = 6666

        self.current_worker_type = current_worker_type

        # copy code to resources
        # for resource in self.resources:
        #     self.initialize_worker(resource['username'], resource['hostname'])

    def initialize_worker(self, username, hostname):
        s = pxssh.pxssh()
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)
        # generate the environment key from username and hostname
        env_key = "_".join([username.upper(), "_".join(hostname.split("."))])

        # get the password from environment
        password = os.environ.get(env_key)

        # login to the worker node and transfer files to it
        s.login(hostname, username, password, sync_multiplier=5, auto_prompt_reset=False) #Update the password
        s.prompt()
        s.sendline(f'mkdir -p Dynamo')
        s.prompt()
        uri = f'{username}@{hostname}'
        sp.run(['scp', 'spawn_workers.py', 'worker.py', 'requirements.txt', f'{uri}:~/Dynamo/']).check_returncode()

        s.sendline('cd Dynamo && python3 spawn_workers.py')
        s.prompt()


    def exposed_add_resources(self, resources):
        # add the resources to the list
        self.resources += resources
        for resource in resources:
            self.copy_code(resource['username'], resource['hostname'])
            self.send_commands(resource['username'], resource['hostname'])
        return {"status": 1, "message": 'Successfully added', "resources": resources}

    ''' 
    If nodes are available, then allocate those to the ring, else return error
    '''
    def exposed_add_nodes_to_ring(self, num_nodes):
        nodes = []
        if len(self.resources) >= num_nodes:
            for i in range(0, num_nodes):
                nodes.append(self.resources[i])
            # add nodes to ring
            if self.configure_nodes(nodes):
                self.create_ring(nodes)
            # Remove the allocated nodes from the resources list
            del self.resources[:num_nodes]
            return {"status": 1, "message": 'Successfully added'}
        else:
            return {"status": -1, "message": 'Resources not available. Please try with less nodes'}

    '''
    Create the configuration / update the configuration of the node
    '''
    def configure_nodes(self, nodes):
        update_ring = False
        for node in nodes:
            hostname = node['hostname']
            config = {
                "hostname": hostname,
                "port": node['port'],
            }
            if hostname not in self.nodes:
                self.nodes[hostname] = config
                update_ring = True
            else:
                self.nodes[hostname] = config
        return update_ring

    '''
    Get virtual host name for a virtual node
    '''
    def getVirtHostName(self, hostname, vnode_count):
        return self.hash(hostname + '_' + str(vnode_count))

    '''
    Get hash value from the key
    '''
    def hash(self, key):
        return int(md5(str(key).encode("utf-8")).hexdigest(), 16)


    '''
    get next node to ask it to change its start range and also add the currently added new node to its routing table
    '''
    def get_neighbour_nodes(self, key_hash):
        pos = bisect(self.keys, key_hash)
        pos = 0 if pos == len(self.keys) else pos
        return pos

    '''
    Create / update the ring on the basis of nodes received
    '''
    def create_ring(self, nodes):
        # keep the hashes of the newly added nodes in temporary list
        # hashes = []
        hash_to_vnode = dict()
        # spawn the processes on each of the nodes first
        print(f'nodes = {nodes}')
        for node in nodes:
            hostname = node['hostname']
            initial_port = node['port']
            for vid in range(0, self.VNODES_COUNT):
                port = initial_port + vid
                node_hash = self.hash(hostname + '_' + str(port))
                # hashes.append(node_hash)
                hash_to_vnode[node_hash] = (hostname, port, vid)
            print(f'hostname: {hostname}, port: {self.SPAWNING_PROC_PORT}')
            rpyc.connect(hostname, self.SPAWNING_PROC_PORT, config={"sync_request_timeout": 2400}).root.spawn_worker(self.VNODES_COUNT, initial_port, current_worker_type)
        # rpc call to the self node and next nodes of each node 
        time.sleep(20)
        for hash, (hostname, port, vid) in hash_to_vnode.items():
            # get the next node and the previous node and make rpc call to update its routing table
            next_node_config = None
            replica_nodes = []
            prev_node_hash = hash
            if len(self.ring) > 0:
                pos_next_node = self.get_neighbour_nodes(hash)
                next_node_config = self.ring[self.keys[pos_next_node]]
                prev_node_hash = self.keys[pos_next_node - 1]

                # fetching the replica nodes to send to the newly added node
                for i in range(min(len(self.ring), self.N)):
                    pos = (pos_next_node + i + 1) % len(self.ring)
                    secondary = self.ring[self.keys[pos]]
                    if next_node_config != secondary:
                        replica_nodes.append(secondary)
                    pass

            
            current_node_config = {
                "hostname": hostname,
                "port": port,
                "vid": vid,
                "load": 0,
                "start_range": str(prev_node_hash + 1),
                "end_range": str(hash)
            }
            # Send the current node's config and it's own updated start range to the node next after the new added one
            # If the current node is first in the ring, there is no next node, so ignore
            if len(self.ring) > 0:
                next_node_conn = rpyc.connect(next_node_config['hostname'], next_node_config['port'])
                next_node_conn._config['sync_request_timeout'] = None
                next_node_conn.root.update_routing_table(current_node_config)
            print(f"Initialize self key ranges, hostname: {hostname}, port: {port}")
            # Send the start range of it's own to the new added node
            curr_node_conn = rpyc.connect(hostname, port, config = {"sync_request_timeout": None})
            curr_node_conn.root.init_self_key_range(current_node_config, next_node_config = next_node_config, replica_nodes = replica_nodes)
            curr_node_conn._config['sync_request_timeout'] = None
            self.ring[hash] = current_node_config
            self.keys = sorted(self.ring.keys())
    
    '''
    Remove node from ring
    '''
    def exposed_remove_node_from_ring(self, num_nodes):
        ring_len = len(self.keys)
        if ring_len > num_nodes:
            for i in range(0, num_nodes):
                pass
                # TODO which nodes to remove or will the node be specified in the parameter
        pass

    '''
    To add new node to the resources list
    '''
    def exposed_add_node_to_resources(self, config):
        self.resources.append(config)

    '''
    To remove a node from the resources list
    '''
    def exposed_remove_node_from_resources(self, node):
        self.resources.remove(node)

    '''
    Get position of the node for a key
    '''
    def get_pos(self, key):
        node_keys = list(self.ring.keys())
        pos = bisect(node_keys, self.hash(key))
        if pos == len(node_keys):
            return 0
        else:
            return pos

    '''
    Get host which is nearest to the key
    '''
    def exposed_get(self, key):
        pos = self.get_pos(key)
        hostname = self.ring[self.keys[pos]]
        return hostname

    '''
    Get instance of the host for a key
    '''
    def exposed_get_instance(self, key):
        return self.nodes[self.get(key)]['instance']




if __name__ == "__main__":
    print('Listening on port 3000')
    
    worker_types = ['syntactic', 'semantic']
    current_worker_type = worker_types[1]
    initial_port = 3000 if current_worker_type == 'syntactic' else 3100
    t = ThreadedServer(Hashring(current_worker_type, initial_port), hostname = '0.0.0.0', port = 3000)
    t.start()
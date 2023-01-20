import redis 
import uuid
import rpyc
import sys
from random import choice
from string import ascii_uppercase, ascii_lowercase
from collections import Counter

def test_hashRing():
    url = ('localhost', 3000)
    count = int(sys.argv[1])
    print ("allocating nodes...")
    ret = rpyc.connect(*url).root.allocate_nodes(count)

    val = rpyc.connect(*url).root.get_all_node_location()
    print (f'All nodes: {val}')
    print (ret)
    if ret["status"] == -1:
        print(f"Reached maximum limit of resources : left {ret['output']}")
    else:
        for j in range(0, 100):
            key, value =  ''.join(choice(ascii_uppercase) for i in range(12)), ''.join(choice(ascii_lowercase) for i in range(4))
            rpyc.connect(*url).root.put(key, value)
            ret = rpyc.connect(*url).root.get(key)
            # print (ret)
        val = rpyc.connect(*url).root.get_all_node_location()
        print (f'All nodes: {val}')


def test_spawn_wokers():
    url = ('localhost', 3000)
    conn = rpyc.connect(*url)
    conn._config['sync_request_timeout'] = None 
    res = conn.root.allocate_nodes(2)
    print (res)
    
def test_workers():
    conn = rpyc.connect(*url).root
    res = conn.get()
    
# test_hashRing()
test_spawn_wokers()
# test_workers()


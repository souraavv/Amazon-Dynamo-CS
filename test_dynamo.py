import sys
import rpyc
import string
from random import choice, randint

client_url = ('localhost', 6001)

def test_createNodes():
    url = ('localhost', 3000)
    count = int(sys.argv[1])
    print ("allocating nodes...")
    ret = rpyc.connect(*url).root.add_nodes_to_ring(count)

    # val = rpyc.connect(*url).root.get_all_node_location()
    # print (f'All nodes: {val}')
    print (ret)
    if ret["status"] == -1:
        print(f"Reached maximum limit of resources : left {ret['output']}")
    # else:
    #     for j in range(0, 100):
            # key, value =  ''.join(choice(ascii_uppercase) for i in range(12)), ''.join(choice(ascii_lowercase) for i in range(4))
            # rpyc.connect(*url).root.put(key, value)
            # ret = rpyc.connect(*url).root.get(key)
            # print (ret)
        # val = rpyc.connect(*url).root.get_all_node_location()
        # print (f'All nodes: {val}')

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(choice(letters) for _ in range(length))
    return result_str


def test_spawn_wokers(num_workers):
    url = ('localhost', 3000)
    conn = rpyc.connect(*url)
    conn._config['sync_request_timeout'] = None 
    res = conn.root.add_nodes_to_ring(num_workers)
    print (res)
    
def test_put(key, value):
    conn = rpyc.connect(*client_url)
    conn._config['sync_request_timeout'] = None
    res = conn.root.put(key, value)
    print(f'Put response = {res}')

def test_get(key):
    conn = rpyc.connect(*client_url)
    conn._config['sync_request_timeout'] = None
    res = conn.root.get(key)
    print(f'Get response = {res}')

# def test_workers():
#     conn = rpyc.connect(*url).root
#     res = conn.get()

type = int(sys.argv[1])
if type == 1:
    num_workers = int(sys.argv[2])
    test_spawn_wokers(num_workers)
elif type == 2:
    for _ in range(50):
        key = get_random_string(10)
        value = randint(1, 10)
        res = test_put(key, value)
elif type == 3:
    key = sys.argv[2]
    res = test_get(key)

# test_put('test2', 'done2')
# test_get('test2')
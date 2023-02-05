import sys
import rpyc

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


def test_spawn_wokers():
    url = ('localhost', 3000)
    conn = rpyc.connect(*url)
    conn._config['sync_request_timeout'] = None 
    res = conn.root.add_nodes_to_ring(2)
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

arg = int(sys.argv[1])    
if arg == 1:
    test_spawn_wokers()
elif arg == 2:
    res = test_put('sfdf', 'done')
elif arg == 3:
    res = test_get('sfdf')

# test_put('test2', 'done2')
# test_get('test2')
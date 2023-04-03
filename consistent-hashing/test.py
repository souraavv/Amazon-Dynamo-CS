import rpyc
import string
from random import choice, randint
import time 

def get_random_string(length: int) -> str:
    letters = string.ascii_lowercase
    result_str = ''.join(choice(letters) for i in range(length))
    return result_str 

def test_hashring() -> None:
    pass 

def test_spawn_wokers() -> None:
    count: int = int(input('Allocate how much nodes ? '))
    print(f"Allocating {count} number of nodes on Hashring...")
    url: tuple = ('localhost', 3000)
    conn: rpyc.Connection = rpyc.connect(*url)
    conn._config['sync_request_timeout'] = None 
    res: dict = conn.root.allocate_nodes(count)
    print(res)
    if res["status"] == -1:
        print(f"Reached maximum limit of resources : left {res['output']}")
    

def test_client_put(key: str, value: int) -> None:
    url: tuple = ('localhost', 6001)
    conn: rpyc.Connection = rpyc.connect(*url)
    conn._config['sync_request_timeout'] = None 
    print(f'PUT REQUEST: For {key} = {value}')
    res: str = conn.root.put(key, value)
    print(f'PUT RESPONSE: {res}')

def test_client_get(key: str) -> None:
    url: tuple = ('localhost', 6001)
    conn: rpyc.Connection = rpyc.connect(*url)
    conn._config['sync_request_timeout'] = None 
    print(f'GET REQUEST : For {key}')
    res: int = conn.root.get(key)
    print(f'GET REPONSE for key {key} = {res}')


def test_semantic_put(key: str) -> None:
    select_item: str = 'y'
    ''' talk to the client of semantic '''
    url: tuple = ('localhost', 6002)
    conn: rpyc.Connection = rpyc.connect(*url)
    print(f"Semantic put:: key: {key}")
    conn._config['sync_request_timeout'] = None 
    # value: str = input('Add/Remove (+/-)')
    # value = '+' if randint(0, 1) == 1  else '-'
    value = '+'
    if value == '+':
        res: str = conn.root.put(key, 1)
        print(f'PUT RESPONSE: {res}')
    elif value == '-':
        res: str = conn.root.put(key, -1)
        print(f'PUT RESPONSE: {res}')
        test_semantic_get(key)
        

def test_semantic_get(key: str) -> None:
    url: tuple = ('localhost', 6002)
    conn: rpyc.Connection = rpyc.connect(*url)
    conn._config['sync_request_timeout'] = None 
    print(f'GET REQUEST : For {key}')
    res: int = conn.root.get(key)
    print(f'GET REPONSE for key {key} = {res}')

def test_workers() -> None:
    url: tuple = ('localhost', 3000)
    conn: rpyc.Connection = rpyc.connect(*url).root
    res: str = conn.get()

while True: 
    print (
    f'''Go with one of the option
    1. Testing hashring
    2. Test spawn workers
    3. Test Client put for syntactic
    4. Test Client get form syntatic
    5. Semantic Put
    6. Semantic Read
    ''')
    try:
        option: int = int(input('Which option ? '))
        if option == 1:
            test_hashring() #DONE
        elif option == 2: 
            test_spawn_wokers()
        elif option == 3:
            for i in range(0, 10):
                s: str = get_random_string(5)
                test_client_put(s, randint(1, 10))
        elif option == 4:
            test_client_get('sepdt')
        elif option == 5:
            hold_key: list = list()
            # for _ in range(0, 5):
            #     key: str = get_random_string(5)
            #     hold_key.append(key)
            key = 'jakkldjaflkdjfkjkldajflkd'
            test_semantic_put(key)
            # for key in hold_key:
            
        elif option == 6:
            key: str = 'jakkldjaflkdjfkjkldajflkd'
            test_semantic_get(key)
        else:
            break
    except Exception as e: 
        print ('Bad options ', e)
        continue


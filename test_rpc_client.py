import datetime
import rpyc
import time
from rpyc.utils.server import ThreadedServer

conn = rpyc.connect('localhost', 4500)

# count = 0

class Test(rpyc.Service):
    def __init__(self) -> None:
        self.count = 0


    def exposed_test(self):

        def callback(res):
            self.count += 1
            # time.sleep(10)
            # if self.count % 100 == 0:
            print(f'callback invoked, cnt = {self.count}')
            if self.count == 3:
                return "Successfully written to 3 nodes"

        results = []
        for i in range(0, 40):
            conn = rpyc.connect('localhost', 4500)
            async_func = rpyc.async_(conn.root.check_rpc)
            res = async_func('1', '1', '1', i)
            results.append(res)
            res.add_callback(callback)
            # res.set_expiry(1)
            
        # print('sleep called')
        # time.sleep(10)
        while 1:
            # pass
            for i, res in enumerate(results):
                # pass
                print(f'request: {i}, Ready: {res.ready}')


if __name__ == '__main__':
    t = ThreadedServer(Test(), hostname = '0.0.0.0', port = 4600, protocol_config={'allow_public_attrs': True})
    t.start()
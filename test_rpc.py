import random
import math
import rpyc
import time
from rpyc.utils.server import ThreadedServer

class TestRPC(rpyc.Service):
    def exposed_check_rpc(self, key, value, request_hash, i):
        # print('called')
        # callback = rpyc.async_(cb)
        # callback({"request_hash": request_hash})
        if i == 0:
            time.sleep(1000)
        else:
            time.sleep(4)
        return "Success"

if __name__ == '__main__':
    t = ThreadedServer(TestRPC(), hostname = '0.0.0.0', port = 4500, protocol_config={'allow_public_attrs': True})
    t.start()
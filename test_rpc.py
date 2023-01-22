import rpyc
import time
from rpyc.utils.server import ThreadedServer

class TestRPC(rpyc.Service):
    def exposed_check_rpc(self, key, value, key_hash):
        print('received rpc call')
        return {"key_hash": key_hash}

if __name__ == '__main__':
    t = ThreadedServer(TestRPC(), hostname = '0.0.0.0', port = 4500, protocol_config={'allow_public_attrs': True})
    t.start()
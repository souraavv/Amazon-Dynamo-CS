import rpyc
from rpyc.utils.server import ThreadedServer 
from subprocess import call, Popen, run

class SpawnWorkers(rpyc.Service):
    def __init__(self):
        self.REDIS_PORT = 6379
    def exposed_spawn_worker(self, port, vnodes):
        for i in range(0, vnodes):
            Popen(['python3', 'worker.py', str(port + i)])
        return "success"
 
if __name__ == "__main__":
    port = 4001
    print (f'Listening at port 4001...')
    ThreadedServer(SpawnWorkers(), hostname='0.0.0.0', port=port).start()

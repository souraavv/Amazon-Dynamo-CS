import rpyc
import subprocess
from rpyc.utils.server import ThreadedServer


class SpawnWorkers(rpyc.Service):
    def exposed_spawn_worker(self, num_workers, initial_port):
        # start the redis instance and flush everything
        # subprocess.Popen(['redis-cli', 'SHUTDOWN'])
        # subprocess.Popen(['nohup',  'redis-server', '&'])
        # subprocess.Popen(['redis-cli', 'flushall'])
        for i in range(0, num_workers):
            print(f'spawned worker: {i}')
            subprocess.Popen(['python3', 'worker.py', str(initial_port + i)])


if __name__ == "__main__":
    port = 6666
    print(f"Listening on port: {port}")
    t = ThreadedServer(SpawnWorkers(), hostname = '0.0.0.0', port = port)
    t.start()
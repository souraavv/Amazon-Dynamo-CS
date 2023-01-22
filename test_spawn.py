import rpyc
connection = ('localhost', 4000)

rpyc.connect('localhost', 4000).root.spawn_worker(4)
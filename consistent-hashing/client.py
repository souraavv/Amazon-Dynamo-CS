import rpyc
from rpyc.utils.server import ThreadedServer 


url = ('localhost', 3001)
val = rpyc.connect(*url).root.hello()
print (val)
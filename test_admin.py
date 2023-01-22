import rpyc
import redis

nodes = [
    {
        'hostname': 'node1.fqdn',
        'instance': redis.StrictRedis(host='localhost'),
        'port': 6379,
    },
    {
        'hostname': 'node2.fqdn',
        'instance': redis.StrictRedis(host='localhost'),
        'port': 6379,
    },
    {
        'hostname': 'node3.fqdn',
        'instance': redis.StrictRedis(host='localhost'),
        'port': 6379,
    }
]

res = rpyc.connect('localhost', 3000).root.add_resources(nodes)
print(res)
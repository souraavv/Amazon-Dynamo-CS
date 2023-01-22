import time
import redis
import hashlib
import threading
import datetime
import rpyc

controller_url = ('localhost', 4500)

rds = redis.Redis(host='localhost')
rds.incr('test')
print(rds.get())

# class Test:
# # create a new consistent hash ring with the nodes
#     def __init__(self):
#         self.requests_log = dict()

#     def hash(self, key):
#         # Get the current date and time
#         now = datetime.datetime.now()

#         # Convert the date and time to a string
#         date_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
#         date_time_str += key

#         # Create a new hash object
#         m = hashlib.sha256()

#         # Update the hash object with the date and time string
#         m.update(date_time_str.encode('utf-8'))

#         # Get the hexadecimal representation of the hash
#         hex_hash = m.hexdigest()
#         return hex_hash

#     def test(self, key, value):
#         count = 0
#         conn = rpyc.connect('localhost', 4500)
#         key_hash = self.hash(key)
#         check_thread = threading.Thread(target = self.check_thread, daemon=True, args = (key_hash,))
#         check_thread.start()
#         self.requests_log[key_hash] = 0
#         def cb(res):
#             self.requests_log[res.value['key_hash']] += 1
#         responses = []
#         for i in range(0, 10):
#             async_func = rpyc.async_(conn.root.check_rpc)#(callback = cb)
#             res = async_func(key, value, key_hash)
#             res.add_callback(cb)
#             responses.append(res.value)
#             # res = async_func()
#         while self.requests_log[key_hash] < 3:
#             continue
#         print("got response for 3 rpc calls")

#     def check_thread(self, key_hash):
#         while True:
#             if key_hash in self.requests_log:
#                 print(f'Received responses from {self.requests_log[key_hash]}')
#             time.sleep(3)

# Test().test("test", "done")


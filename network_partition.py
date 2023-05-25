import subprocess


def block_traffic(ip_address, port_set):
    print (f'Network partition is called for ip: {ip_address} and ports= {port_set}')
    for port in port_set:
        isolate_command = \
        f'''sudo iptables -I INPUT 1 -s {ip_address} -p tcp --dport {port} -j DROP; 
        sudo iptables -I OUTPUT 1 -d {ip_address} -p tcp --dport {port} -j DROP;'''
        subprocess.run([isolate_command], shell=True) # Update with password
    print ("Network is partitioned...")

def heal_firewall(ip_address, port_set):
    print (f'Network heal is called for ip: {ip_address} and ports= {port_set}')
    for port in port_set:
        heal_command = \
        f'''sudo iptables -D INPUT -s {ip_address} -p tcp --dport {port} -j DROP; 
        sudo iptables -D OUTPUT -d {ip_address} -p tcp --dport {port} -j DROP;'''
        
        subprocess.run([heal_command], shell = True) # Update with password
    print("The network partition is healed")
    

print (f'1. Network partition')
print (f'2. Heal partition')

ip = input("Enter IP: ")
which = int(input('Which option ? '))
if which == 1:
    block_traffic(ip, [3101, 3103, 3105])
elif which == 2:
    heal_firewall(ip, [3101, 3103, 3105])
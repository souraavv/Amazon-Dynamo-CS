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
        
        subprocess.run([heal_command], shell=True) # Update with password
    print("The network partition is healed")


if __name__ == '__main__':
    print (f'1. Network partition')
    print (f'2. Heal partition')

    which = int(input('Which option ? '))
    which_node = int(input('which nodes cluster (node1, node2) ? '))
    ip = '10.237.27.95' if which_node == 1 else '10.17.50.254'

    # vnodes = [3100, 3103, 3105]
    vnodes = [3100, 3101, 3102, 3103, 3104, 3105]
    if which == 1:
        block_traffic(ip, vnodes)
    elif which == 2:
        heal_firewall(ip, vnodes)
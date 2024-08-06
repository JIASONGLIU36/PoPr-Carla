from ruamel.yaml import YAML
import sys


def modify_port(file_path, new_arg, stable_port):
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096  # prevent line wrapping to a certain extent
    port_num = new_arg
    count = 0
    # Load the YAML file
    with open(file_path, 'r') as file:
        data = yaml.load(file)

    for service in data['services']:
        current_com = data['services'][service].get('command', [])
        if current_com:
            current_com[0] = str(port_num + count)
            current_com[1] = str(stable_port)
            count += 1
            data['services'][service]['command'] = current_com
            print(current_com)

    with open(file_path, 'w') as file:
        yaml.dump(data, file)

def modify_ip(file_path):
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096  # prevent line wrapping to a certain extent

    count = 2
    # Load the YAML file
    with open(file_path, 'r') as file:
        data = yaml.load(file)
    
    for service in data['services']:
        print(service)
        current_ip = data['services'][service]['networks']['carla_cav'].get('ipv4_address', [])
        current_ip = "192.168.5." + str(count)
        data['services'][service]['networks']['carla_cav']['ipv4_address'] = current_ip
        count += 1


    with open(file_path, 'w') as file:
        yaml.dump(data, file)


modify_port('./compose.yaml', int(sys.argv[1]), int(sys.argv[2]))

#modify_ip('./compose.yaml')
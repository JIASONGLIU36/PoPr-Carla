# Author: Jiasong Liu
# Notes: Carla_API is one side only
# You must run UDP_change.bat first!
import socket
import copy
import threading
import random
import select
import json
import sys
import time
import math
import queue
import uuid
import os
# open quantum safe
import oqs
# merkletree tool
from merkletools import MerkleTools
# SHA3 tool
import hashlib
# Track error
import traceback

# Make addr_list visble to threads, use set for better complexity
addr_list = set()
group_list = set()
# Store the Carla ID corresponds to the ip_addrs, the keys are strings
Carla_addr_list = {}
# Store the ip address corresponds to public key
pb_k_Carla_list = {} 
# Store the id of nodes, given by hash of public key of nodes (public key is too long)
node_hashid_list = {}
# Stores node id pair with ip address
node_id_addr_list = {}
# Stores membership of nodes, the key is nodeID and value is the membership
nodeid_membership = {}
# This is the table contains historical reputation for each block to optimize the computation
block_reputation_table = {}

# These stores 3 types of verification messages used in the block
# Remeber to clear the list when new BSM block is created
curr_DSRC_block = {}
curr_CAM_block = {}
curr_RADAR_block = {}

# This variable is for handling old duty to new group members, we cannot do it right after we compute the new group member
updated_group_list = []

# This is current time block we promoted
this_node_time_block = None

# This stores current BSM verification block for record if the node is a group member
curr_BSM_veri_block = None
# This stores the current time block
curr_time_block = None

# This dictionary stores BSM verification blockchain
curr_BSM_blockchain = {}
# This one stores time chain
curr_time_blockchain = {}

# Create node with localhost IP and assigned port
def Carla_UDP_CAVNode(host='0.0.0.0', port=int(sys.argv[1])):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF, 16000000)
    print(f"Current UDP buffer size: {server_socket.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF)}")
    server_socket.bind((host, port))
    print(f"Carla CAV node at your service at {host}:{port}")
    return server_socket

# Since Carla API is just providing the sensor and detection data. 
# The correct behavior of the Carla is communicate with the docker nodes by pre-set port numbers
# And let docker nodes to communicate in the subnet for blockchain 
def Carla_API_sock(host='0.0.0.0', port=9600):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF, 16000000)
    server_socket.bind((host, port))
    print(f"Carla API socket at {host}:{port}")
    return server_socket

# Using a TCP socket thread to download block/blockchain from group members
def Bd_TCP_dl(node_stop, start_dl, get_new_block_g, BSM_veri_start, reg_node_dl, rep_count_start):
    global curr_BSM_veri_block, curr_BSM_blockchain
    host ='0.0.0.0'
    port = 11000

    print(f"Carla block download socket start at {host}:{port}")
    block_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    block_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    block_socket.bind((host, port))
    block_socket.listen(30)

    input_list = [block_socket]
    output_list = []
    exception_list = []

    try:
        while not node_stop.is_set(): 
            sys.stdout.flush()
            read_sockets, _, _ = select.select(input_list, output_list, exception_list, 2)
            
            for tcp_socket in read_sockets:
                # This part is for group members
                if tcp_socket == block_socket:
                    conn, client_address = block_socket.accept()
                    print(f"Accept connection at {client_address}")
                    input_list.append(conn)
                elif tcp_socket in input_list:
                    sys.stdout.flush()
                    # Receive order message
                    block_data_order = tcp_socket.recv(1024)
                    # The message received is always a json
                    msg_receive = json.loads(block_data_order.decode('utf-8'))
                    print(f"Receiving from {tcp_socket.getpeername()} Message: {msg_receive}")
                    if msg_receive['type'] == 'new_block_dl' and len(group_list) != 0:
                        if curr_addr in group_list and len(curr_BSM_blockchain) != 0:
                            key, latest_bsm_b = find_latest_block(curr_BSM_blockchain)
                            tcp_socket.sendall(json.dumps({key: latest_bsm_b}).encode('utf-8'))
                        elif len(curr_BSM_blockchain) != 0:
                            # we will let nodes to download the block after group switch due the unnecessary race condition. In this case the node will reply with a block it have in the latest 
                            key, latest_bsm_b = find_latest_block(curr_BSM_blockchain)
                            tcp_socket.sendall(json.dumps({key: latest_bsm_b}).encode('utf-8'))
                        else:
                            tcp_socket.sendall(json.dumps({"type": "faulty", "error_msg": "This node is empty"}).encode('utf-8'))
                    elif msg_receive['type'] == 'new_db_consensus':
                        if curr_addr == new_leader[0]:
                            tcp_socket.sendall(json.dumps(curr_BSM_veri_block).encode('utf-8'))
                        else:
                            tcp_socket.sendall(json.dumps({"type": "faulty", "error_msg": "This node is Not leader node"}).encode('utf-8'))
                    else:
                        print("Faulty message, ignored")
                    
                    # only remove the socket after response
                    input_list.remove(tcp_socket)
                    tcp_socket.close()

                    print("Request complete")
                    sys.stdout.flush()
        
            # This part is for non-group members
            # This happens when the current node want new block/blockchain, the request should not send by a group member
            if start_dl.is_set() and len(group_list) != 0:
                #and (curr_addr not in group_list)
                conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
                group_ip = None
                # if we are ready to collect a new block, then we are going to download the new block from the leader
                if get_new_block_g.is_set():
                    group_ip = new_leader[0]
                    conn_socket.connect((group_ip[0], port))
                    conn_socket.sendall(json.dumps({"type": "new_db_consensus"}).encode('utf-8'))
                    print(f"New block consensus message sent :{time.time()}")
                if reg_node_dl.is_set():
                    group_ip = random.choice(list(group_list))
                    conn_socket.connect((group_ip[0], port))
                    conn_socket.sendall(json.dumps({"type": "new_block_dl"}).encode('utf-8'))
                    print(f"New block download message sent :{time.time()}")

                # Receive entire message
                entire_msg = bytearray()
                while True:
                    block_data_order = conn_socket.recv(4096)
                    if block_data_order == b'':
                        break
                    else:
                        entire_msg.extend(block_data_order)
                print(f"Receiving from {(group_ip[0], port)}")
                msg_receive = json.loads(entire_msg.decode('utf-8'))
                print(f"New block consensus message loaded :{time.time()}")
                key, _ = next(iter(msg_receive.items()))
                if key == 'type':
                    if msg_receive['type'] == 'faulty':
                        print(msg_receive['error_msg'])
                    elif get_new_block_g.is_set():
                        curr_BSM_veri_block = msg_receive
                        print(f"New block consensus message saved :{time.time()}")
                        BSM_veri_start.set()
                else:
                    key, block = next(iter(msg_receive.items()))
                    curr_BSM_blockchain[key] = block
                    #### We start the reputation count for regular node members here ####
                    rep_count_start.set()
                    if well_known_node == True:
                        # Save the blockchain as wellknown node
                        save_json(curr_BSM_blockchain, 'blockchain')

                sys.stdout.flush()
                get_new_block_g.clear()
                start_dl.clear()
                reg_node_dl.clear()

    except Exception as e:
        tb = traceback.format_exc()
        print(tb, e) 
        sys.stdout.flush()
        block_socket.close()

    finally:
        print('TCP_module closed')
        sys.stdout.flush()
        block_socket.close()

# This is the thread for the group members to verify the camera detection result
# Sometime we have hash collision because the degree of tolerance. This is fine since every BSM will get one verification
def group_camera_verifier(node_stop, veri_start, list_veri):
    try:
        while not node_stop.is_set():
            veri_start.wait()
            if list_veri.is_set():
                while not cam_veri_queue.empty():
                    this_cam_veri = cam_veri_queue.get()
                    this_BSM = this_cam_veri['ori_BSM']
                    hash_of_thisBSM = hash_sha3(f"{this_BSM['orientation']}||{this_BSM['timestamp']}||{this_BSM['latitude']}||{this_BSM['longitude']}")
                    # First we verify the hash of origin BSM
                    if hash_of_thisBSM == this_cam_veri['ori_BSM']['hash'] and signature_verifier(hash_of_thisBSM, this_BSM['digital_signature'], node_hashid_list[this_BSM['node_id']]):
                        # This part is for CAV and RSU
                        if this_cam_veri['member_type'] == "CAV":
                            # Then, let us verify the hash from camera verification 
                            hash_of_CAM_verifier = hash_sha3(f"{this_cam_veri['front_hash']}||{this_cam_veri['rear_hash']}||{this_cam_veri['veri_lati']}||{this_cam_veri['veri_longi']}||{this_cam_veri['cam_azim']}||{hash_of_thisBSM}")
                            if hash_of_CAM_verifier == this_cam_veri['hash'] and signature_verifier(hash_of_CAM_verifier, this_cam_veri['digital_signature'], node_hashid_list[this_cam_veri['node_id']]):
                                # Now we need to verify the hash of images from camera
                                for veri_msg in BSM_Cam_veri:
                                    if veri_msg['hash_front'] == this_cam_veri['front_hash'] and veri_msg['hash_rear'] == this_cam_veri['rear_hash'] and veri_msg['member_type'] == "CAV":
                                        # Now we verify the azimuth from BSM to Cam
                                        if ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                                            if ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < this_cam_veri['cam_azim'] < ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                                                veri_id = hash_sha3(hash_of_thisBSM + hash_of_CAM_verifier + this_cam_veri['node_id'])
                                                curr_CAM_block[veri_id] = this_cam_veri

                                        elif ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 > ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                                            if ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < this_cam_veri['cam_azim'] or ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360 > this_cam_veri['cam_azim']:
                                                veri_id = hash_sha3(hash_of_thisBSM + hash_of_CAM_verifier + this_cam_veri['node_id'])
                                                curr_CAM_block[veri_id] = this_cam_veri
                        elif this_cam_veri['member_type'] == "RSU":
                            hash_of_CAM_verifier = hash_sha3(f"{this_cam_veri['front_hash']}||{this_cam_veri['veri_lati']}||{this_cam_veri['veri_longi']}||{this_cam_veri['cam_azim']}||{hash_of_thisBSM}")
                            if hash_of_CAM_verifier == this_cam_veri['hash'] and signature_verifier(hash_of_CAM_verifier, this_cam_veri['digital_signature'], node_hashid_list[this_cam_veri['node_id']]):
                                for veri_msg in BSM_Cam_veri:
                                    # Here are the option to verify RSU camera result, just the front
                                    if veri_msg['hash_front'] == this_cam_veri['front_hash'] and veri_msg['member_type'] == "RSU":
                                        if ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                                            if ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < this_cam_veri['cam_azim'] < ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                                                veri_id = hash_sha3(hash_of_thisBSM + hash_of_CAM_verifier + this_cam_veri['node_id'])
                                                curr_CAM_block[veri_id] = this_cam_veri

                                        elif ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 > ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                                            if ((this_cam_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < this_cam_veri['cam_azim'] or ((this_cam_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360 > this_cam_veri['cam_azim']:
                                                veri_id = hash_sha3(hash_of_thisBSM + hash_of_CAM_verifier + this_cam_veri['node_id'])
                                                curr_CAM_block[veri_id] = this_cam_veri

            if not node_stop.is_set(): 
                veri_start.clear()

    except Exception:
        tb = traceback.format_exc()
        print(tb)
        
# This is the thread for the group members to verify radar detection result
def group_radar_verifier(node_stop, veri_start):
    try:
        while not node_stop.is_set():
            veri_start.wait()
            while not radar_veri_queue.empty():
                this_radar_veri = radar_veri_queue.get()
                this_BSM = this_radar_veri['ori_BSM']
                hash_of_this_bsm = hash_sha3(f"{this_BSM['orientation']}||{this_BSM['timestamp']}||{this_BSM['latitude']}||{this_BSM['longitude']}")
                # First we check if the hash of the origin BSM match
                if hash_of_this_bsm == this_BSM['hash'] and signature_verifier(hash_of_this_bsm, this_BSM['digital_signature'], node_hashid_list[this_BSM['node_id']]):
                    # Second, Let use verify the verification message
                    hash_of_radar_verifier = hash_sha3(f"{this_radar_veri['radar_point']}||{this_radar_veri['veri_lati']}||{this_radar_veri['veri_longi']}||{this_radar_veri['bsm_azim']}||{this_radar_veri['bsm_depth']}||{hash_of_this_bsm}")
                    if hash_of_radar_verifier == this_radar_veri['hash'] and signature_verifier(hash_of_radar_verifier, this_radar_veri['digital_signature'], node_hashid_list[this_radar_veri['node_id']]):
                        # Now we can verify the radar point's depth and azimuth
                        if ((this_radar_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < ((this_radar_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                            if ((this_radar_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < this_radar_veri['radar_point'][0] < ((this_radar_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                                # Now let us verify the depth
                                if (this_radar_veri['bsm_depth'] - Tolerance_depth_range) < this_radar_veri['radar_point'][1] < (this_radar_veri['bsm_depth'] + Tolerance_depth_range):
                                    veri_id = hash_sha3(hash_of_this_bsm + hash_of_radar_verifier + this_radar_veri['node_id'])
                                    curr_RADAR_block[veri_id] = this_radar_veri

                        elif ((this_radar_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 > ((this_radar_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360:
                            if ((this_radar_veri['bsm_azim'] - Tolerance_angle_range) + 360) % 360 < this_radar_veri['radar_point'][0] or ((this_radar_veri['bsm_azim'] + Tolerance_angle_range) + 360) % 360 > this_radar_veri['radar_point'][0]:
                                 if (this_radar_veri['bsm_depth'] - Tolerance_depth_range) < this_radar_veri['radar_point'][1] < (this_radar_veri['bsm_depth'] + Tolerance_depth_range):
                                    veri_id = hash_sha3(hash_of_this_bsm + hash_of_radar_verifier + this_radar_veri['node_id'])
                                    curr_RADAR_block[veri_id] = this_radar_veri

            # This ensures the thread is going to quit when we also quit the node
            if not node_stop.is_set():
                veri_start.clear()

    except Exception:
        tb = traceback.format_exc()
        print(tb)        

# We are using dilithium2 as the digital signature scheme
def generate_ppkeys():
    sig = oqs.Signature('Dilithium2')
    This_public_key = sig.generate_keypair().hex()
    This_private_key = sig.export_secret_key().hex()

    return (This_public_key, This_private_key)

# This function signs any hash of the messages
def signature_signer(hash, private_key):
    sign = oqs.Signature('Dilithium2', bytes.fromhex(private_key))
    return sign.sign(hash.encode()).hex()

# This function verifies hash of the messages
def signature_verifier(hash, signature, public_key):
    veri = oqs.Signature('Dilithium2')
    return veri.verify(hash.encode(), bytes.fromhex(signature), bytes.fromhex(public_key))

# This function verifies hash and signature of BSM:
def BSM_verifier(BSM, public_key):
    Sha3_hash = hashlib.sha3_256()
    BSM_encode = f"{BSM['orientation']}||{BSM['timestamp']}||{BSM['latitude']}||{BSM['longitude']}"
    Sha3_hash.update(json.dumps(BSM_encode).encode('utf-8'))
    BSM_hash = Sha3_hash.hexdigest()
    if BSM_hash == BSM['hash']:
        veri_result = signature_verifier(BSM_hash, BSM['digital_signature'], public_key)
    else:
        veri_result = False

    return veri_result

# This function start the initial consensus
def init_consensus(sock, wk_ip, wk_port):
    Initial_Consensus = {"type": "wellknown"}
    sock.sendto(json.dumps(Initial_Consensus).encode('utf-8'), (wk_ip, wk_port))

# A thread boardcast ip list for nodes and group member in a certain interval
# We are also going to boardcast the membership of the nodes
def node_group_ip_bd(sock, stop_event, well_known_ip):
    while not stop_event.is_set():
        sys.stdout.flush() # flush the stdout
        for ip in addr_list:
            if ip != well_known_ip and group_list:
                ip_list = {"type": "well_known_bd", "ip_list": list(addr_list), "group_list": list(group_list), "Carla_addr": Carla_addr_list, "addr_members": nodeid_membership}
                sock.sendto(json.dumps(ip_list).encode('utf-8'), ip)
                for ip_addr, public_key in pb_k_Carla_list.items():
                    pk_list = {"type": "wk_bd_public_key", "ip_addr": ip_addr, "public_key": public_key}
                    sock.sendto(json.dumps(pk_list).encode('utf-8'), ip)

        time.sleep(7)


# Start by well-known node. This function start the initial group consensus. This consensus does not rely on reputation (except RSU).
def Init_group_consensus(sock):
    global Init_thread_fin
    time.sleep(3) # wait some time after the well-known node collected enough nodes' addresses 
    # This is initial consensus for group members, reputation is no concern for CAV/RSU
    Initial_gm_consensus = {"type": "Init_group_consensus"}
    for ip_p in addr_list:
        if ip_p != (well_known_ip, well_known_port):
            sock.sendto(json.dumps(Initial_gm_consensus).encode('utf-8'), ip_p)

    Init_thread_fin = True

def Init_group_assign(sock, Group_number):
    # pick 7 random group members for starting the blockchain
    group_addr = random.sample(Init_node_list, Group_number)
    Group_mem_assi_consensus = {"type": "Init_group_member", "message": "New_member"}
    for addr in group_addr:
        sock.sendto(json.dumps(Group_mem_assi_consensus).encode('utf-8'), addr[0])

# This function construct BSM for delivery
def BSM_for_CAV_node(BSM, node_id, private_key):
    # Compute hash for this message
    BSM_node = {'type': 'BSM_node', 'node_id': node_id, 'orientation': BSM['orientation'], 'timestamp': BSM['timestamp'], 'latitude': BSM['latitude'], 'longitude': BSM['longitude']}
    BSM_encode = f"{BSM['orientation']}||{BSM['timestamp']}||{BSM['latitude']}||{BSM['longitude']}"
    Sha3_hash = hashlib.sha3_256()
    Sha3_hash.update(json.dumps(BSM_encode).encode('utf-8'))
    BSM_hash = Sha3_hash.hexdigest()
    ds = signature_signer(BSM_hash, private_key)
    BSM_node['hash'] = BSM_hash
    BSM_node['digital_signature'] = ds

    return BSM_node

# This function creates BSM approve messages, the message will send to group members and sender 
def BSM_approve_response(BSM, node_id, curr_private_key):
    BSM_approve_encode = f"{BSM['orientation']}||{BSM['timestamp']}||{BSM['latitude']}||{BSM['longitude']}||{BSM['digital_signature']}"
    BSM_approve_hash = hash_sha3(BSM_approve_encode)
    BSM_approve_msg = {'type': 'BSM_DSRC_approve', 'node_id': node_id, 'ori_BSM': BSM, 'hash_ori': BSM_approve_hash, 'digital_signature': signature_signer(BSM_approve_hash, curr_private_key)}
    return BSM_approve_msg

# This thread will verify the BSM approve message. if the message is OK, the message will be kept for block creation 
def BSM_DSRC_verifier(node_stop, veri_start):
    try:
        while not node_stop.is_set():
            veri_start.wait()
            while not dsrc_veri_queue.empty():
                BSM_approve = dsrc_veri_queue.get()
                BSM_msg = BSM_approve['ori_BSM']
                BSM_encode = f"{BSM_msg['orientation']}||{BSM_msg['timestamp']}||{BSM_msg['latitude']}||{BSM_msg['longitude']}"
                BSM_approve_encode = f"{BSM_msg['orientation']}||{BSM_msg['timestamp']}||{BSM_msg['latitude']}||{BSM_msg['longitude']}||{BSM_msg['digital_signature']}"
                
                if BSM_msg['hash'] == hash_sha3(BSM_encode) and BSM_approve['hash_ori'] == hash_sha3(BSM_approve_encode):
                    ori_sig_veri = signature_verifier(hash_sha3(BSM_encode), BSM_msg['digital_signature'], node_hashid_list[BSM_msg['node_id']])
                    appr_sig_veri = signature_verifier(hash_sha3(BSM_approve_encode), BSM_approve['digital_signature'], node_hashid_list[BSM_approve['node_id']])

                if ori_sig_veri and appr_sig_veri:
                    if curr_addr in group_list:
                        message_key = hash_sha3(BSM_approve['hash_ori'] + BSM_approve['node_id'])
                        #print(f"DSRC message {BSM_approve['hash_ori']} from {BSM_approve['node_id']} is verified, will put into the current block. ID: {message_key}")
                        # Stores DSRC verification in BSM DSRC verification block
                        curr_DSRC_block[message_key] = BSM_approve
                else:
                    print("Faulty DSRC verification message received.")

            # This ensures the thread is going to quit when we also quit the node
            if not node_stop.is_set():
                veri_start.clear()

    except Exception:
        tb = traceback.format_exc()
        print(tb)   
    

# A general SHA3 hash method
def hash_sha3(msg):
    Sha3_hash = hashlib.sha3_256()
    Sha3_hash.update(json.dumps(msg).encode('utf-8'))
    return Sha3_hash.hexdigest()

# This function save json file to volume
def save_json(message, label):
    #test_json = {"This node": int(sys.argv[1])}
    #test_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, str(time.ctime()+sys.argv[1])) 
    file_stamp = "new_block"
    name = "./data/Node_{}_{}.json".format(curr_addr[0], label)

    out = json.dumps(message, indent=4)
    with open(name, "w") as outfile:
        outfile.write(out)

# This function will update the BSM by timestamp
# There is a RACE condition with timestamp, Some node receive BSM earlier will send the BSM immediately before 
# this node receive its BSM
def BSM_message_updater(BSM_msg_list, curr_time):
    # Remove staled BSMs
    for bsm in list(BSM_msg_list):
        if bsm['timestamp'] != curr_time and curr_time != 0:
            BSM_msg_list.remove(bsm)

# This function remove the stale verification messages and update the list with new camera verification
# We don't have to change this one, because Camera list is strictly after the node's BSM is ready
def CAM_veri_handler(CAM_veri_list, veri_msg, curr_time):
    for entry in list(CAM_veri_list):
        if entry['timestamp'] != curr_time:
            CAM_veri_list.remove(entry)

    CAM_veri_list.append(veri_msg)

# This function compute the CAV in range for camera, the camera range is about 40 meters
# The purpose is to narrow down a list of CAVs that could be captured by cameras
def BSM_CAV_in_range(BSM_msg_list, curr_latitude, curr_longitude, detect_range):
    CAV_in_range = []
    for bsm in BSM_msg_list:
        range = haversine(curr_latitude, curr_longitude, bsm['latitude'], bsm['longitude'])
        if range <= detect_range:
            CAV_in_range.append(bsm)

    return CAV_in_range

# This function check if the azimuth detected by CV model match with BSM in range
def BSM_azimuth_check(BSM_in_range, Cam_result, nodeID, curr_pri_key, curr_lalo, allowed_range):
    curr_latitude = curr_lalo[0]
    curr_longitude = curr_lalo[1]
    veri_result = []

    CAM_bsm_result = {'type': 'cam_veri_node', 'node_id': nodeID, 'ori_BSM': "null", "member_type": Cam_result["member_type"]}

    #-#-# We are now only using front azimuth, will change later for cv is ready
    simu_detected_azimuth = list(Cam_result['azimuth_front'])

    for bsm in BSM_in_range:
        CAM_bsm_result = CAM_bsm_result.copy()
        # The BSM azimuth should be the ground truth for detect the CAV's presence
        bsm_azimuth = azimuth_compute(curr_latitude, curr_longitude, bsm['latitude'], bsm['longitude'])
        for azi_cav in simu_detected_azimuth:
            if Cam_result["member_type"] == 'CAV':
                # It could be tricky when cross the 0 degree line
                # Case for when not crossing the 0 degree line
                if ((bsm_azimuth-allowed_range) + 360) % 360 < ((bsm_azimuth+allowed_range) + 360) % 360:
                    if ((bsm_azimuth-allowed_range) + 360) % 360 < azi_cav < ((bsm_azimuth+allowed_range) + 360) % 360:
                        CAM_bsm_result['ori_BSM'] = bsm
                        CAM_bsm_result['veri_lati'] = curr_latitude
                        CAM_bsm_result['veri_longi'] = curr_longitude
                        CAM_bsm_result['front_hash'] = Cam_result['hash_front']
                        CAM_bsm_result['rear_hash'] = Cam_result['hash_rear']
                        CAM_bsm_result['bsm_azim'] = bsm_azimuth
                        CAM_bsm_result['cam_azim'] = azi_cav
                        CAM_bsm_result['hash'] = hash_sha3(f"{CAM_bsm_result['front_hash']}||{CAM_bsm_result['rear_hash']}||{CAM_bsm_result['veri_lati']}||{CAM_bsm_result['veri_longi']}||{CAM_bsm_result['cam_azim']}||{bsm['hash']}")
                        CAM_bsm_result['digital_signature'] = signature_signer(CAM_bsm_result['hash'], curr_pri_key)
        
                        veri_result.append(CAM_bsm_result)
                # Case for crossing 0 degree line
                elif ((bsm_azimuth-allowed_range) + 360) % 360 > ((bsm_azimuth+allowed_range) + 360) % 360:
                    if ((bsm_azimuth+allowed_range) + 360) % 360 > azi_cav or ((bsm_azimuth-allowed_range) + 360) % 360 < azi_cav:
                        CAM_bsm_result['ori_BSM'] = bsm
                        CAM_bsm_result['veri_lati'] = curr_latitude
                        CAM_bsm_result['veri_longi'] = curr_longitude
                        CAM_bsm_result['front_hash'] = Cam_result['hash_front']
                        CAM_bsm_result['rear_hash'] = Cam_result['hash_rear']
                        CAM_bsm_result['bsm_azim'] = bsm_azimuth
                        CAM_bsm_result['cam_azim'] = azi_cav
                        CAM_bsm_result['hash'] = hash_sha3(f"{CAM_bsm_result['front_hash']}||{CAM_bsm_result['rear_hash']}||{CAM_bsm_result['veri_lati']}||{CAM_bsm_result['veri_longi']}||{CAM_bsm_result['cam_azim']}||{bsm['hash']}")
                        CAM_bsm_result['digital_signature'] = signature_signer(CAM_bsm_result['hash'], curr_pri_key)

                        veri_result.append(CAM_bsm_result)
            elif Cam_result["member_type"] == 'RSU':
                # RSU only have front camera, so the cam_veri_node hash and ds will be different
                if ((bsm_azimuth-allowed_range) + 360) % 360 < ((bsm_azimuth+allowed_range) + 360) % 360:
                    if ((bsm_azimuth-allowed_range) + 360) % 360 < azi_cav < ((bsm_azimuth+allowed_range) + 360) % 360:
                        CAM_bsm_result['ori_BSM'] = bsm
                        CAM_bsm_result['veri_lati'] = curr_latitude
                        CAM_bsm_result['veri_longi'] = curr_longitude
                        CAM_bsm_result['front_hash'] = Cam_result['hash_front']
                        CAM_bsm_result['bsm_azim'] = bsm_azimuth
                        CAM_bsm_result['cam_azim'] = azi_cav
                        CAM_bsm_result['hash'] = hash_sha3(f"{CAM_bsm_result['front_hash']}||{CAM_bsm_result['veri_lati']}||{CAM_bsm_result['veri_longi']}||{CAM_bsm_result['cam_azim']}||{bsm['hash']}")
                        CAM_bsm_result['digital_signature'] = signature_signer(CAM_bsm_result['hash'], curr_pri_key)
        
                        veri_result.append(CAM_bsm_result)
                # Case for crossing 0 degree line
                elif ((bsm_azimuth-allowed_range) + 360) % 360 > ((bsm_azimuth+allowed_range) + 360) % 360:
                    if ((bsm_azimuth+allowed_range) + 360) % 360 > azi_cav or ((bsm_azimuth-allowed_range) + 360) % 360 < azi_cav:
                        CAM_bsm_result['ori_BSM'] = bsm
                        CAM_bsm_result['veri_lati'] = curr_latitude
                        CAM_bsm_result['veri_longi'] = curr_longitude
                        CAM_bsm_result['front_hash'] = Cam_result['hash_front']
                        CAM_bsm_result['bsm_azim'] = bsm_azimuth
                        CAM_bsm_result['cam_azim'] = azi_cav
                        CAM_bsm_result['hash'] = hash_sha3(f"{CAM_bsm_result['front_hash']}||{CAM_bsm_result['veri_lati']}||{CAM_bsm_result['veri_longi']}||{CAM_bsm_result['cam_azim']}||{bsm['hash']}")
                        CAM_bsm_result['digital_signature'] = signature_signer(CAM_bsm_result['hash'], curr_pri_key)

                        veri_result.append(CAM_bsm_result)
    
    return veri_result

# This function will check if the azimuth and distance from radar results have something match with the BSMs
def BSM_Radar_check(BSMs_in_range, radar_result, nodeID, curr_pri_key, curr_lalo, allowed_depth, allowed_angle, membership):
    curr_latitude = curr_lalo[0]
    curr_longitude = curr_lalo[1]
    veri_result = []

    Radar_bsm_result = {'type': 'radar_veri_node', 'node_id': nodeID, 'ori_BSM': 'null'}

    def parse_result(radar_result, bsm, curr_lati, curr_longi, radar_p, bsm_azi, bsm_dist, curr_pk):
        # instead append same pointer, we copy a new dict everytime
        radar_result_copy = radar_result.copy()
        radar_result_copy['ori_BSM'] = bsm
        radar_result_copy['veri_lati'] = curr_lati
        radar_result_copy['veri_longi'] = curr_longi
        radar_result_copy['radar_point'] = radar_p
        radar_result_copy['bsm_azim'] = bsm_azi
        radar_result_copy['bsm_depth'] = bsm_dist
        radar_result_copy['hash'] = hash_sha3(f"{radar_result_copy['radar_point']}||{radar_result_copy['veri_lati']}||{radar_result_copy['veri_longi']}||{radar_result_copy['bsm_azim']}||{radar_result_copy['bsm_depth']}||{bsm['hash']}")
        radar_result_copy['digital_signature'] = signature_signer(radar_result_copy['hash'], curr_pk)
        return radar_result_copy

    if membership == 'CAV':
        radar_front = radar_result['front_result']
        radar_rear = radar_result['rear_result']

        for bsm in BSMs_in_range:
            # First we are going to check the azimuth of front
            bsm_azimuth = azimuth_compute(curr_latitude, curr_longitude, bsm['latitude'], bsm['longitude'])
            bsm_distance = haversine(curr_latitude, curr_longitude, bsm['latitude'], bsm['longitude'])
            for radar_point in radar_front:
                if ((bsm_azimuth-allowed_angle) + 360) % 360 < ((bsm_azimuth+allowed_angle) + 360) % 360:
                    if ((bsm_azimuth-allowed_angle) + 360) % 360 < radar_point[0] < ((bsm_azimuth+allowed_angle) + 360) % 360:
                        # Then we check if the detected object in BSM's range or not
                        if (bsm_distance - allowed_depth) < radar_point[1] < (bsm_distance + allowed_depth):
                            radar_veri = parse_result(Radar_bsm_result, bsm, curr_latitude, curr_longitude, radar_point, bsm_azimuth, bsm_distance, curr_pri_key)
                            veri_result.append(radar_veri)

                elif ((bsm_azimuth-allowed_angle) + 360) % 360 > ((bsm_azimuth+allowed_angle) + 360) % 360:
                    if ((bsm_azimuth-allowed_angle) + 360) % 360 < radar_point[0] or ((bsm_azimuth+allowed_angle) + 360) % 360 > radar_point[0]:
                        if (bsm_distance - allowed_depth) < radar_point[1] < (bsm_distance + allowed_depth):
                            radar_veri = parse_result(Radar_bsm_result, bsm, curr_latitude, curr_longitude, radar_point, bsm_azimuth, bsm_distance, curr_pri_key)
                            veri_result.append(radar_veri)

            # Then we check back
            for radar_point in radar_rear:
                if ((bsm_azimuth-allowed_angle) + 360) % 360 < ((bsm_azimuth+allowed_angle) + 360) % 360:
                    if ((bsm_azimuth-allowed_angle) + 360) % 360 < radar_point[0] < ((bsm_azimuth+allowed_angle) + 360) % 360:
                        if (bsm_distance - allowed_depth) < radar_point[1] < (bsm_distance + allowed_depth):
                            radar_veri = parse_result(Radar_bsm_result, bsm, curr_latitude, curr_longitude, radar_point, bsm_azimuth, bsm_distance, curr_pri_key)
                            veri_result.append(radar_veri)

                elif ((bsm_azimuth-allowed_angle) + 360) % 360 > ((bsm_azimuth+allowed_angle) + 360) % 360:
                    if ((bsm_azimuth-allowed_angle) + 360) % 360 < radar_point[0] or ((bsm_azimuth+allowed_angle) + 360) % 360 > radar_point[0]:
                        if (bsm_distance - allowed_depth) < radar_point[1] < (bsm_distance + allowed_depth):
                            radar_veri = parse_result(Radar_bsm_result, bsm, curr_latitude, curr_longitude, radar_point, bsm_azimuth, bsm_distance, curr_pri_key)
                            veri_result.append(radar_veri)
    elif membership == 'RSU':
        radar_front = radar_result['front_result']
        for bsm in BSMs_in_range:
            # First we are going to check the azimuth of front
            bsm_azimuth = azimuth_compute(curr_latitude, curr_longitude, bsm['latitude'], bsm['longitude'])
            bsm_distance = haversine(curr_latitude, curr_longitude, bsm['latitude'], bsm['longitude'])
            for radar_point in radar_front:
                if ((bsm_azimuth-allowed_angle) + 360) % 360 < ((bsm_azimuth+allowed_angle) + 360) % 360:
                    if ((bsm_azimuth-allowed_angle) + 360) % 360 < radar_point[0] < ((bsm_azimuth+allowed_angle) + 360) % 360:
                        # Then we check if the detected object in BSM's range or not
                        if (bsm_distance - allowed_depth) < radar_point[1] < (bsm_distance + allowed_depth):
                            radar_veri = parse_result(Radar_bsm_result, bsm, curr_latitude, curr_longitude, radar_point, bsm_azimuth, bsm_distance, curr_pri_key)
                            veri_result.append(radar_veri)

                elif ((bsm_azimuth-allowed_angle) + 360) % 360 > ((bsm_azimuth+allowed_angle) + 360) % 360:
                    if ((bsm_azimuth-allowed_angle) + 360) % 360 < radar_point[0] or ((bsm_azimuth+allowed_angle) + 360) % 360 > radar_point[0]:
                        if (bsm_distance - allowed_depth) < radar_point[1] < (bsm_distance + allowed_depth):
                            radar_veri = parse_result(Radar_bsm_result, bsm, curr_latitude, curr_longitude, radar_point, bsm_azimuth, bsm_distance, curr_pri_key)
                            veri_result.append(radar_veri)

    return veri_result


# This is time chain block consensus, we first estimate when the start time/end time should be and use real time after 1st block created
def time_chain_consensus(stop_event, timeout_event, sock):
    global this_node_time_block, This_time_block_ID, final_vote_time
    try:
        while not stop_event.is_set():
            pb_k_ready = True # avoid race condition for public key assignment
            for addr in group_list:
                if addr not in pb_k_Carla_list:
                    pb_k_ready = False

            if curr_addr in group_list and len(group_list) == init_group_num and pb_k_ready:
                # if no measure result for time interval means the node start initial time consensus
                if len(curr_time_blockchain) == 0:
                    curr_db_start_time = init_tb_starttime_interval + time.time() # We are going to use decimal place to make a difference in time
                    curr_db_finish_time = int(init_max_blockcreate_time + curr_db_start_time) # we want time count in seconds for finish time
                    # Here is the genesis block ID for timeblock, hash 0
                    prev_tb_hash = hash_sha3("0")
                    # The message block number is going to be 0 since it is genesis data block
                    data_block_num = 0

                    # Sequence is matter in hash!
                    # in initial consensus we just sort the ip from lower number to larger number
                    sort_group_list = sorted(group_list, key=lambda ip: tuple(map(int, ip[0].split('.'))))

                    group_id_total = ''
                    for group_ip in sort_group_list:
                        group_id_total = group_id_total+pb_k_Carla_list[group_ip] 

                    # This gives the initial group ID hash
                    group_id_hash = hash_sha3(group_id_total)

                    # This is the hash of the first time block
                    time_block_hash = hash_sha3(f"{group_id_hash}||{prev_tb_hash}||{curr_db_start_time}||{curr_db_finish_time}||{data_block_num}||{This_node_id}")

                    d_signature = signature_signer(time_block_hash, private_k)
                    
                    init_time_consensus_block = {"type": "time_block_cons", "group_id_hash": group_id_hash, "prev_tb_hash": prev_tb_hash, "data_block_start_t": curr_db_start_time, 
                                                 "data_block_end_t": curr_db_finish_time, "db_number": data_block_num, "hash": time_block_hash, "addr_ip": curr_addr}
                    init_time_consensus_block['digital_signature'] = d_signature
                    init_time_consensus_block['node_id'] = This_node_id

                    # Assign certain value for group member to consensus
                    this_node_time_block = init_time_consensus_block
                    This_time_block_ID = prev_tb_hash
                    for ip in group_list:
                        if ip != curr_addr:
                            sock.sendto(json.dumps(init_time_consensus_block).encode('utf-8') , ip)

                    # We first put our own block in for avoiding len() == 0 case for initial block consensus
                    # This also means we don't have to change our blockchain if we got consensus approves
                    curr_time_blockchain[prev_tb_hash] = init_time_consensus_block
                    print(f"Initial time block consensus sent {time_block_hash}")
                    # Now we tell our node we are ready to receive the consensus block from others
                    tb_consensus_sent = True

                    # Before the group member votes, we need to compute the time for this node to vote
                    vote_interval = base_t_vote + int(prev_tb_hash, 16) % max_t_vote_mod # The max time for waiting the new time block are 5 seconds
                    final_vote_time = int(time.time()) + vote_interval
                    # Put wait event to wait again
                    timeout_event.clear()
                elif len(curr_time_blockchain) > 0:
                    # Then we deal with BSM timeout, the timeout only happens when new block is failed to created
                    _, timeblock = find_latest_block(curr_time_blockchain)
                    _, BSMblock = find_latest_block(curr_BSM_blockchain)
                    # We first going to check if the case is a timeout case or regular case
                    if time.time() > final_vote_time and This_time_block_cons == False:
                        # Here is the creation of new time block, Given the fact the time block failed to consensus or new consensus
                        # We all need to create a new time block
                        if timeblock['hash'] == BSMblock['time_block_hash']:
                            print("Regular new time block creation begin")
                            # New duration for BSM block
                            if len(curr_time_blockchain) == 1:
                                manual_time_interval = 36 # we need to use manual time here because the second time block is different
                                curr_db_start_time = manual_time_interval + time.time() 
                                curr_db_finish_time = int(init_max_blockcreate_time + curr_db_start_time) 
                            elif len(curr_time_blockchain) > 1:
                                # We need more than 1 time block to count the correct interval between end of BSM block creation and start of radar interval
                                _, timeblock_2 = find_second_latest_tb(curr_time_blockchain)
                                offset = 2 # This is offset for final start time
                                tb_starttime_interval = (final_time_gather_BSM_veri - timeblock_2['data_block_end_t']) + offset
                                print("Current wait interval for CARLA (should be around 34-46):", tb_starttime_interval)
                                # The final_time_gather_BSM_veri should always received!!!!! Something wrong if we have a negative number

                                curr_db_start_time = tb_starttime_interval + time.time() 
                                curr_db_finish_time = int(init_max_blockcreate_time + curr_db_start_time)

                            # hash of the previous block, also the ID of the current time block
                            prev_tb_hash = timeblock['hash']
                            # Data block ID
                            data_block_num = timeblock['db_number'] + 1

                            # For the time block order, we will sort by host identifier
                            sort_group_list = sorted(group_list, key=lambda ip: tuple(map(int, ip[0].split('.'))))

                            group_id_total = ''
                            for group_ip in sort_group_list:
                                group_id_total = group_id_total+pb_k_Carla_list[group_ip] 

                            # This gives the initial group ID hash
                            group_id_hash = hash_sha3(group_id_total)

                            # Hash of this block
                            time_block_hash = hash_sha3(f"{group_id_hash}||{prev_tb_hash}||{curr_db_start_time}||{curr_db_finish_time}||{data_block_num}||{This_node_id}")
                            d_signature = signature_signer(time_block_hash, private_k)

                            this_time_consensus_block = {"type": "time_block_cons", "group_id_hash": group_id_hash, "prev_tb_hash": prev_tb_hash, "data_block_start_t": curr_db_start_time, 
                                                        "data_block_end_t": curr_db_finish_time, "db_number": data_block_num, "hash": time_block_hash, "addr_ip": curr_addr}
                            this_time_consensus_block['digital_signature'] = d_signature
                            this_time_consensus_block['node_id'] = This_node_id

                            # Assign certain value for group member to consensus
                            this_node_time_block = this_time_consensus_block
                            This_time_block_ID = prev_tb_hash
                            for ip in group_list:
                                if ip != curr_addr:
                                    sock.sendto(json.dumps(this_time_consensus_block).encode('utf-8') , ip)

                            curr_time_blockchain[prev_tb_hash] = this_time_consensus_block
                            print(f"Initial time block consensus sent {time_block_hash}")
                            # Now we tell our node we are ready to receive the consensus block from others
                            tb_consensus_sent = True

                            # Before the group member votes, we need to compute the time for this node to vote
                            vote_interval = base_t_vote + int(prev_tb_hash, 16) % max_t_vote_mod # The max time for waiting the new time block are 5 seconds
                            final_vote_time = int(time.time()) + vote_interval
                        else:
                            print("This is a timeout for timeblock session")
                        # Put wait event to wait again
                        timeout_event.clear()

                    # This is the initial case for BSM block timeout
                    # The first case check if the block is created during the time of block creation
                    if (len(curr_BSM_blockchain) == 0 and time.time() > timeblock['data_block_end_t']) and This_time_block_cons == True:
                        print("The BSM initial block failed to created after BSM block timeout!")
                    
                    # This case is check if the block is created during the time after initial phrase
                    elif (len(curr_BSM_blockchain) > 0 and time.time() > timeblock['data_block_end_t']) and This_time_block_cons == True:
                        if timeblock['hash'] != BSMblock['time_block_hash']:
                            print("The BSM block failed to created after BSM block timeout!")

                # This time block condition is for vote the time block process
                # We define two cases, first case is we receive all time blocks created by the group member within the vote time.
                # Second case is we receive at least 2 time blocks and the rest time for voting is less or equal than 0.5 seconds
                if (timeblock_queue.qsize() == (init_group_num-1) and time.time() < final_vote_time) or (timeblock_queue.qsize() >= 2 and (final_vote_time - time.time() <= 0.5)) and tb_consensus_sent == True:
                    print(f"Receive full votes? : {(timeblock_queue.qsize() == (init_group_num-1) and time.time() < final_vote_time)}")
                    tb_list = []
                    while not timeblock_queue.empty():
                        tb_list.append(timeblock_queue.get())

                    # For voting the block, the group member is going to check few things
                    # First, it is going to check if the signature and hash match
                    for tb in list(tb_list):
                        tb_hash = hash_sha3(f"{tb['group_id_hash']}||{tb['prev_tb_hash']}||{tb['data_block_start_t']}||{tb['data_block_end_t']}||{tb['db_number']}||{tb['node_id']}")
                        if tb_hash != tb["hash"] or not signature_verifier(tb_hash, tb['digital_signature'], node_hashid_list[tb['node_id']]):
                            tb_list.remove(tb)
 
                    # Then, the start time and finish time need to be checked
                    for tb in list(tb_list):
                       if not (tb['data_block_start_t'] > time.time()) or not (tb['data_block_end_t'] > tb['data_block_start_t']):
                           tb_list.remove(tb)

                    # Finally, the group node is going to choose the block with the smallest value 
                    sort_tb = sorted(tb_list, key=lambda t: t['data_block_start_t'])
                    # Theoretically, we should just vote the first block. However, in case of the same start time, we will hash the nodeID and starttime and compare. The smallest will win.
                    # First, let us check if there are any block with same timestamp of the smallest start time block
                    same_block = [stb for stb in sort_tb if sort_tb[0]['data_block_start_t'] == stb['data_block_start_t']]

                    block_hash_tuple = []
                    if len(same_block) == 1:
                        # if we find the block to vote, we send the vote to the consensus block
                        sock.sendto(json.dumps({"type": "time_block_approve", "block_hash": sort_tb[0]['hash'], "block_id": sort_tb[0]['prev_tb_hash'], "digital_signature": signature_signer(sort_tb[0]['hash'], private_k), "node_id": This_node_id}).encode('utf-8'), tuple(sort_tb[0]['addr_ip']))
                    elif len(same_block) > 1:
                        for stb in same_block:
                            rank_hash = int(hash_sha3(f"{stb['node_id']}||{stb['data_block_start_t']}"), 16)
                            block_hash_tuple.append((rank_hash, stb))

                        sort_tb_sst = sorted(block_hash_tuple, key=lambda hash: hash[0])
                        sock.sendto(json.dumps({"type": "time_block_approve", "block_hash": sort_tb_sst[0][1]['hash'], "block_id": sort_tb_sst[0][1]['prev_tb_hash'], "digital_signature": signature_signer(sort_tb_sst[0][1]['hash'], private_k), "node_id": This_node_id}).encode('utf-8') , tuple(sort_tb_sst[0][1]['addr_ip']))
                    else:
                        print("error, same_block should not be 0 size")

                    # The node should stop receiving consensus after vote
                    tb_consensus_sent = False
                    # Put wait event to wait again
                    timeout_event.clear()

            # This is a timeout wait, we have to use it because we need to immediately start our inital time block consensus
            timeout_event.wait(timeout=2.5)

    except Exception:
        tb = traceback.format_exc()
        print(tb)   

# This thread is for group members to compute the leader by using hash mod group size
def group_leader_election(stop_event, start_elec, BSM_creation_start):
    global This_time_block_ID, new_leader
    try:
        while not stop_event.is_set():
            start_elec.wait()
            # The initial group leaderr election is judged by size of blocks
            if len(curr_time_blockchain) == 1 and len(curr_BSM_blockchain) == 0 and not stop_event.is_set():
                # just use the ID group members used for consensus. The block ID are same (because it is chained)
                Next_block_start_time = curr_time_blockchain[This_time_block_ID]['data_block_start_t']
                db_number = curr_time_blockchain[This_time_block_ID]['db_number']
                # This is genesis data block hash, should only contains a 00 string
                prevhashblock = hash_sha3("00")
                # Now we can compute the group leader for creating the data block
                new_leader_index = int(hash_sha3(f"{prevhashblock}||{db_number}||{Next_block_start_time}"), 16) % init_group_num

                # For initial group election, we don't use reputation but ip sequence of group members
                sort_group_list = sorted(group_list, key=lambda ip: tuple(map(int, ip[0].split('.'))))

                # Now we assign new leader to the global variable
                new_leader = (sort_group_list[new_leader_index], This_time_block_ID)

                # Start BSM block creation right after 
                BSM_creation_start.set()

            elif len(curr_time_blockchain) > 1 and len(curr_BSM_blockchain) > 0 and not stop_event.is_set():
                # just use the ID group members used for consensus. The block ID are same (because it is chained)
                Next_block_start_time = curr_time_blockchain[This_time_block_ID]['data_block_start_t']
                db_number = curr_time_blockchain[This_time_block_ID]['db_number']
                # Use the last block hash
                _, BSMblock = find_latest_block(curr_BSM_blockchain)
                # Now we can compute the group leader for creating the data block
                new_leader_index = int(hash_sha3(f"{BSMblock['hash']}||{db_number}||{Next_block_start_time}"), 16) % init_group_num
                # To simplify the task, we are still using sorted IP 
                sort_group_list = sorted(group_list, key=lambda ip: tuple(map(int, ip[0].split('.'))))

                # Now we assign new leader to the global variable
                new_leader = (sort_group_list[new_leader_index], This_time_block_ID)

                # Start BSM block creation right after 
                BSM_creation_start.set()
            
            print("Current leader: ", new_leader)

            if not stop_event.is_set():
                start_elec.clear()
    except Exception:
        tb = traceback.format_exc()
        print(tb)   

# This function verifies if the BSM is pair with DSRC to each other nodes
def veri_DSRC_pair(DSRC_list):
    DSRC_result = {}
    for key_d, dsrc in DSRC_list.items():
        for key_b, dsrc_pair in DSRC_list.items():
            # Make sure DSRC veri msgs are paired
            if dsrc['node_id'] == dsrc_pair['ori_BSM']['node_id'] and dsrc_pair['node_id'] == dsrc['ori_BSM']['node_id']:
                DSRC_result[key_d] = dsrc
                DSRC_result[key_b] = dsrc_pair

    return DSRC_result

# This function verifies Camera and Radar messages. They will find the corresponding DSRC veri
def veri_CAM_Radar_pair(DSRC_list, CAM_Radar):
    Result_list = {}
    for key in CAM_Radar:
        for _, dsrc in DSRC_list.items():
            if CAM_Radar[key]['ori_BSM']['node_id'] == dsrc['ori_BSM']['node_id']:
                Result_list[key] = CAM_Radar[key]
    return Result_list

# The condition of start creating a BSM block, it starts right after the group leader election
# For this simulation we are going to simply put every legitmate messages to the new block
def BSM_data_block_consensus(stop_event, start_blockcreation, sock):
    consensus_data_block_header = {"type": "New_consensus_block", "time_stamp": "null", "block_id" : "null", "leader_id": This_node_id, "merkle_root": "null", "hash": "null"}
    global curr_BSM_veri_block
    try:
        while not stop_event.is_set():
            start_blockcreation.wait()
            # First, the initial block condition
            if len(curr_time_blockchain) >= 1 and new_leader != None and not stop_event.is_set():
                if new_leader[0] == curr_addr:
                    # We need to organize the BSM data block before we start the block consensus
                    print(time.time(), "Waiting till start time: " + str(curr_time_blockchain[new_leader[1]]['data_block_start_t']))
                    while curr_time_blockchain[new_leader[1]]['data_block_start_t'] > time.time():
                        # Time out until it is time to boardcast new BSM block
                        time.sleep(0.5)
                    print(time.time(), "Block start time: " + str(curr_time_blockchain[new_leader[1]]['data_block_start_t']))

                    # Now we are going to verify pair of DSRC
                    this_DSRC_list = veri_DSRC_pair(copy.deepcopy(curr_DSRC_block))
                    # Then we verify the Camera and radar data
                    this_CAM_list = veri_CAM_Radar_pair(this_DSRC_list, copy.deepcopy(curr_CAM_block))
                    this_RADAR_list = veri_CAM_Radar_pair(this_DSRC_list, copy.deepcopy(curr_RADAR_block))
                    # ^^^ The above steps ensure the attacker must collaborate with other attack nodes to gain reputation 

                    # Delayed 3 seconds result of DSRC are same, means start time is fine.
                    # (We can extend few seconds if we few less veri data are captured)

                    # Now we can compute the merkle root 
                    root_hash = MerkleTools(hash_type="sha3_256")

                    # We first add DSRC leaves
                    for key, dsrc in this_DSRC_list.items():
                        pair_hash = f"{key}||{dsrc['hash_ori']}"
                        root_hash.add_leaf(pair_hash, True)

                    for key, cam in this_CAM_list.items():
                        pair_hash = f"{key}||{cam['hash']}"
                        root_hash.add_leaf(pair_hash, True)

                    for key, radar in this_RADAR_list.items():
                        pair_hash = f"{key}||{radar['hash']}"
                        root_hash.add_leaf(pair_hash, True)
                    
                    root_hash.make_tree()
                    consensus_data_block_header['merkle_root'] = root_hash.get_merkle_root()
                    # The block id is the genesis block hash
                    if len(curr_BSM_blockchain) == 0:
                        consensus_data_block_header['block_id'] = hash_sha3("00")
                    elif len(curr_BSM_blockchain) > 0:
                        _, block = find_latest_block(curr_BSM_blockchain)
                        consensus_data_block_header['block_id'] = block['hash']
                    # Time stamp should within the start and end time in time block
                    consensus_data_block_header['time_stamp'] = time.time()
                    # We also need data block number and hash of the related time block (always last time block are used to create new db)
                    _, relate_time_block = find_latest_block(curr_time_blockchain)
                    consensus_data_block_header['db_number'] = relate_time_block['db_number']
                    consensus_data_block_header['time_block_hash'] = relate_time_block['hash']

                    # Now we compute the hash of the header as block hash
                    consensus_data_block_header['hash'] = hash_sha3(f"{consensus_data_block_header['time_block_hash']}||{consensus_data_block_header['time_stamp']}||{consensus_data_block_header['block_id']}||{consensus_data_block_header['db_number']}||{consensus_data_block_header['leader_id']}||{consensus_data_block_header['merkle_root']}")
                    # Then we sign the hash of the block header
                    consensus_data_block_header['digital_signature'] = signature_signer(consensus_data_block_header['hash'], private_k)

                    # Block size average 5 MegaBytes
                    curr_BSM_veri_block = dict(consensus_data_block_header)
                    curr_BSM_veri_block['DSRC'] = copy.deepcopy(this_DSRC_list)
                    curr_BSM_veri_block['CAM'] = copy.deepcopy(this_CAM_list)
                    curr_BSM_veri_block['RADAR'] = copy.deepcopy(this_RADAR_list)
                    # We send header for group members to grab new block
                    for ip in group_list:
                        if ip != curr_addr:
                            sock.sendto(json.dumps(consensus_data_block_header).encode('utf-8') , ip)

            if not stop_event.is_set():
                start_blockcreation.clear()

    except Exception:
        tb = traceback.format_exc()
        print(tb) 

# This function check if every node is included in the DSRC list
# It checks BSM node id for every verified message with its own data, if it find unmatched data it will return false
# !! This function create trouble if the leader create the block before they receive all BSM veri messages
def verify_DSRC_nodes_present(DSRC_list):
    dsrc_bsm_ids = [dsrc['ori_BSM']['node_id'] for _, dsrc in DSRC_list.items()]
    curr_bsm_ids = [dsrc_curr['ori_BSM']['node_id'] for _, dsrc_curr in curr_DSRC_block.items()]
    return all(node_id in dsrc_bsm_ids for node_id in curr_bsm_ids)

# This function find latest block by blockID (HASH). Although we could assume last block in dictionary is latest in python after 3.8.
# But the correct behavior of finding the last block is by hash
def find_latest_block(blockchain_list):
    genesis_ID_time = hash_sha3("0")
    genesis_ID_BSM = hash_sha3("00")
    key = None
    block = None
    if genesis_ID_time in blockchain_list:
        key = genesis_ID_time
        block = blockchain_list[key]
        # The hash of the block is the ID of next block (prev_hash)
        next_hashID_timeblock = blockchain_list[genesis_ID_time]['hash']
        check_id_exist = next_hashID_timeblock in blockchain_list
        while check_id_exist:
            key = next_hashID_timeblock
            block = blockchain_list[next_hashID_timeblock]

            next_hashID_timeblock = blockchain_list[next_hashID_timeblock]['hash']
            check_id_exist = next_hashID_timeblock in blockchain_list
    elif genesis_ID_BSM in blockchain_list:
        key = genesis_ID_BSM
        block = blockchain_list[key]
        next_hashID_BSMblock = blockchain_list[key]['hash']
        check_id_exist = next_hashID_BSMblock in blockchain_list
        while check_id_exist:
            key = next_hashID_BSMblock
            block = blockchain_list[next_hashID_BSMblock]

            next_hashID_BSMblock = blockchain_list[next_hashID_BSMblock]['hash']
            check_id_exist = next_hashID_BSMblock in blockchain_list

    return key, block

# This function is for finding the correct interval between end of last block creation and radar data received (aHh)
def find_second_latest_tb(tb_list):
    genesis_ID_time = hash_sha3("0")
    blockchain_size = len(tb_list)

    if genesis_ID_time in tb_list:
        key = genesis_ID_time
        block = tb_list[key]

        next_hashID_timeblock = tb_list[genesis_ID_time]['hash']
        check_id_exist = next_hashID_timeblock in tb_list
        count = 1
        while check_id_exist and count < (blockchain_size - 1):
            key = next_hashID_timeblock
            block = tb_list[next_hashID_timeblock]

            next_hashID_timeblock = tb_list[next_hashID_timeblock]['hash']
            check_id_exist = next_hashID_timeblock in tb_list
            count += 1
    return key, block


# This thread is only triggered when we receive a new consensus block, this block is for verifying the new block
def BSM_db_consensus(event_stop, new_db_block, sock):
    global curr_BSM_blockchain
    try:
        while not event_stop.is_set():
            new_db_block.wait()
            if not event_stop.is_set():
                header = {"type": curr_BSM_veri_block['type'], "time_stamp": curr_BSM_veri_block['time_stamp'], "block_id" : curr_BSM_veri_block['block_id'], "db_number": curr_BSM_veri_block['db_number'], 
                        "leader_id": curr_BSM_veri_block['leader_id'], "time_block_hash": curr_BSM_veri_block['time_block_hash'], "merkle_root": curr_BSM_veri_block['merkle_root'], 
                        "hash": curr_BSM_veri_block['hash'], "digital_signature": curr_BSM_veri_block['digital_signature']}
                dsrc_list = curr_BSM_veri_block['DSRC']
                cam_list = curr_BSM_veri_block['CAM']
                radar_list = curr_BSM_veri_block['RADAR']
                # We first verify the merkle tree
                root_hash = MerkleTools(hash_type="sha3_256")
                for key, dsrc in dsrc_list.items():
                    pair_hash = f"{key}||{dsrc['hash_ori']}"
                    root_hash.add_leaf(pair_hash, True)

                for key, cam in cam_list.items():
                    pair_hash = f"{key}||{cam['hash']}"
                    root_hash.add_leaf(pair_hash, True)

                for key, radar in radar_list.items():
                    pair_hash = f"{key}||{radar['hash']}"
                    root_hash.add_leaf(pair_hash, True) 
                root_hash.make_tree()
                merkle_root_match = (root_hash.get_merkle_root() == header['merkle_root'])
                # Then we check block ID
                if len(curr_BSM_blockchain) == 0:
                    block_id_match = (curr_BSM_veri_block['block_id'] == hash_sha3("00"))
                else:
                    # In here we use last block header hash as the block ID
                    _, block = find_latest_block(curr_BSM_blockchain)
                    block_id_match = (curr_BSM_veri_block['block_id'] == block['hash'])
                # Then we verify if BSM data pair match
                this_DSRC_list = veri_DSRC_pair(copy.deepcopy(dsrc_list))
                # Then we verify the Camera and radar data
                this_CAM_list = veri_CAM_Radar_pair(dsrc_list, copy.deepcopy(cam_list))
                this_RADAR_list = veri_CAM_Radar_pair(dsrc_list, copy.deepcopy(radar_list))
                block_msg_pair_match = (len(this_DSRC_list) == len(dsrc_list)) and (len(cam_list) == len(this_CAM_list)) and (len(radar_list) == len(this_RADAR_list))
                # We now check if the time block is match with the lastest time block
                _, db_relate_time_block = find_latest_block(curr_time_blockchain)
                time_block_hash_match = db_relate_time_block['hash'] == header['time_block_hash']
                block_number_match = db_relate_time_block['db_number'] == header['db_number']
                # Now we can check if the new db block is in the time block's creation interval
                in_creation_interval = db_relate_time_block['data_block_start_t'] < time.time() < db_relate_time_block['data_block_end_t']
                hash_verification = hash_sha3(f"{header['time_block_hash']}||{header['time_stamp']}||{header['block_id']}||{header['db_number']}||{header['leader_id']}||{header['merkle_root']}") == header['hash']
                ds_verification = signature_verifier(header['hash'], header['digital_signature'],  node_hashid_list[header['leader_id']])
                #print(verify_DSRC_nodes_present(dsrc_list), merkle_root_match, block_id_match, block_msg_pair_match, time_block_hash_match, block_number_match, in_creation_interval, hash_verification, ds_verification)
                if all([verify_DSRC_nodes_present(dsrc_list), merkle_root_match, block_id_match, block_msg_pair_match, time_block_hash_match, block_number_match, in_creation_interval, hash_verification, ds_verification]):
                    sock.sendto(json.dumps({"type": "BSM_block_approve", "block_hash": header['hash'], "block_id": header['block_id'], "digital_signature": signature_signer(header['hash'], private_k), "node_id": This_node_id}).encode('utf-8'), node_id_addr_list[header['leader_id']])
                    print("New BSM block verification complete, send approve message to the leader")
                else:
                    print("Faulty Block received, ignored")
            if not event_stop.is_set():
                new_db_block.clear()
    except Exception:
        tb = traceback.format_exc()
        print(tb)

# This thread exist because our leader could just submit their approval message once they got 5 votes. This causes a race condition
# with group nodes that still downloading the new BSM block
def block_Ready_checker(stop_event, receive_cons, rep_count_start, sock):
    global curr_BSM_veri_block, This_BSM_block_cons, curr_BSM_blockchain
    dl_clear = False

    while not stop_event.is_set():
        receive_cons.wait()
        if not stop_event.is_set():
            _, relate_time_block = find_latest_block(curr_time_blockchain)

            if curr_BSM_veri_block != None:
                curr_BSM_veri_block['app_signs'] = {}
                if curr_addr in group_list and approve_nbsm_q.qsize() == group_consensus_num and relate_time_block['db_number'] == curr_BSM_veri_block['db_number']:
                    while not approve_nbsm_q.empty():
                        db_veri_msg = approve_nbsm_q.get()
                        curr_BSM_veri_block['app_signs'][db_veri_msg['node_id']] = db_veri_msg['digital_signature']
                        veri_result = signature_verifier(db_veri_msg['block_hash'], db_veri_msg['digital_signature'], node_hashid_list[db_veri_msg['node_id']])
                        if veri_result == False:
                            break
                    if veri_result == True:
                        This_BSM_block_cons = True
                        curr_BSM_blockchain[curr_BSM_veri_block['block_id']] = curr_BSM_veri_block
                        print(f"New data block confirmed, timeout for new time block stopped.")
                        # We call new leader to tell them our new block is ready
                        sock.sendto(json.dumps({"type": "group_block_readydl"}).encode('utf-8'), new_leader[0])
                        #### We start the reputation count here for "last" group member ####
                        rep_count_start.set()
                    else:
                        print("Failed to confirm the new data block, the time out continues")
                    dl_clear = False # Clear to stop the thread
                else:
                    # This is the case when the node is still downloading the new block
                    time.sleep(0.2)
                    dl_clear = True
            else:
                # This is the case when the node is still downloading the new block
                time.sleep(0.2)
                dl_clear = True

        if not stop_event.is_set() and dl_clear == False:
            receive_cons.clear()

# This thread is the most critial thread that is going to decide who are going to be our next leader and group members
# This thread starts right after the new block's consensus is complete. That means when group member finish verification of the block;
# When CAV receive new block from group members and the leader receive enough vote for new block
def Reputation_computing(stop_event, rep_count_start, sock):
    global block_reputation_table, group_list, updated_group_list
    while not stop_event.is_set():
        rep_count_start.wait()
        if not stop_event.is_set():
            # First, let us initialize the reputation list. Because we need to count reputation everytime, and the vehicle come and go everytime.
            nodeid_reputation = {}
            for nodeID in nodeid_membership:
                nodeid_reputation[nodeID] = 0

            _, This_BSM_block = find_latest_block(curr_BSM_blockchain)
            print("Now we start to count the Reputation based on current & previous blocks")

            # Here is the reputation table for current BSM block
            for node_id in nodeid_reputation:
                # let us count the verification reputation
                for dsrc in This_BSM_block['DSRC']:
                    if This_BSM_block['DSRC'][dsrc]['node_id'] == node_id:
                        nodeid_reputation[node_id] += BSM_veri_weight[0]

                for cam in This_BSM_block['CAM']:
                    if This_BSM_block['CAM'][cam]['node_id'] == node_id:
                        nodeid_reputation[node_id] += BSM_veri_weight[1]

                for radar in This_BSM_block['RADAR']:
                    if This_BSM_block['RADAR'][radar]['node_id'] == node_id:
                        nodeid_reputation[node_id] += BSM_veri_weight[2]

                # Then let us count the being verified reputation
                for dsrc in This_BSM_block['DSRC']:
                    if This_BSM_block['DSRC'][dsrc]['ori_BSM']['node_id'] == node_id:
                        nodeid_reputation[node_id] += BSM_ori_weight[0]

                for cam in This_BSM_block['CAM']:
                    if This_BSM_block['CAM'][cam]['ori_BSM']['node_id'] == node_id:
                        nodeid_reputation[node_id] += BSM_ori_weight[1]

                for radar in This_BSM_block['RADAR']:
                    if This_BSM_block['RADAR'][radar]['ori_BSM']['node_id'] == node_id:
                        nodeid_reputation[node_id] += BSM_ori_weight[2]

            # Creating a new reputation table
            block_reputation_table[This_BSM_block['block_id']] = {}

            # Now we put reputation into sigmoid function to get scaled scores [0, 1]
            for rep in nodeid_reputation:
                block_reputation_table[This_BSM_block['block_id']][rep] = sigmoid_rep(nodeid_reputation[rep], coeff_a, coeff_b)

            # Finally, we could compute the final reputation based on the table of reputation
            Final_rep_score = {}
            for nodeID in nodeid_membership:
                Final_rep_score[nodeID] = 0

            # We compute every node's final rep score here
            count_block = 0
            for block_id in reversed(block_reputation_table.keys()):
                # The counted block is either 8 blocks back or the farthest block you can go
                if bc_max_block_count == count_block:
                    break

                for node_id in Final_rep_score:
                    Final_rep_score[node_id] += (block_reputation_table[block_id][node_id] * time_decay_e((bc_max_block_count-count_block), bc_max_block_count, coeff_decay))
                    Final_rep_score[node_id] = min(Final_rep_score[node_id], 1)
                count_block += 1

            # Now we assign bonus reputation to RSU members 
            for node_id in Final_rep_score:
                if nodeid_membership[node_id] == "RSU":
                    Final_rep_score[node_id] += RSU_bonus
                    Final_rep_score[node_id] = min(Final_rep_score[node_id], 1)

            # Now we need to sort the reputation for reputation node
            Final_rep = sort_reputation(Final_rep_score, This_BSM_block['time_stamp'], This_BSM_block['hash'])
            # Now we can decide who will be the next group member, and send consensus to the members
            new_group_list = []
            group_count = 0
            for node in Final_rep:
                if group_count == init_group_num:
                    break
                new_group_list.append(node_id_addr_list[node])
                group_count += 1

            # Assign new group list. We will handle the power switch by allowing old group members to work until they receive the response from new group members
            # In short: they will compute the new group member. But they won't change the group member list right away
            updated_group_list = set(new_group_list)
            # If the node are neither members of current group or next group, they can update their group_list with no problem
            if curr_addr not in group_list and curr_addr not in new_group_list:
                group_list = set(new_group_list)     
                # Here, as the last step of the last block. we can refresh the variable and start new timeblock consensus right away
                # Only the non group related nodes can refresh their variables here
                refresh_variables()

            # if the node is a new group member, they are going to send consensus to other group members
            if curr_addr in updated_group_list:
                for ip in updated_group_list:
                    if curr_addr != ip:
                        sock.sendto(json.dumps({"type": "new_group_consensus", "message": "check"}).encode('utf-8'), ip)

            # Save the reputation result to volume for wellknown node
            if well_known_node == True:
                save_json(curr_time_blockchain, 'time_chain')
                save_json(block_reputation_table, 'reputation')
                print("Here are the new group member computed by block")
                for ip in new_group_list:
                    print("new", ip)
 
        if not stop_event.is_set():
            rep_count_start.clear()

# This is the sort function for sorting the reputation
def sort_reputation(rep_dict, timestamp, BSM_blockhash):
    # hash(node_id + time_stamp + BSM_blockhash)
    return dict(sorted(rep_dict.items(), key=lambda rep: (-rep[1], -int(hash_sha3(rep[0] + str(timestamp) + BSM_blockhash), 16) )))

# Here is the sigmoid function to scale the final reputation to range [0, 1]
# the variable a and b are reputation coeffients, means we have to fine-tune this hyperparameter to reflect the blockchain's reputation result
def sigmoid_rep(Rep, a, b):
    return 1/(1+math.exp(b*(a-Rep)))

# This is time decay factor, we only consider limited number of block for reputation due to the most recent BSMs are important forr
# reputation update
def time_decay_e(curr, max_b, a):
    return math.exp(-a*(max_b-curr)**2)

# this is the function to reset every flag and switch we did. After we reset every switch, we can start a new time chain
def refresh_variables():
    global time_block_vote_rec, new_time_block_bd, This_time_block_cons, BSM_block_vote, new_BSM_block_bd, new_leader, final_vote_time, curr_DSRC_block, curr_CAM_block, curr_RADAR_block, updated_group_list
    time_block_vote_rec = 0
    new_time_block_bd = False
    This_time_block_cons = False

    final_vote_time = 0

    BSM_block_vote = 0
    new_BSM_block_bd = False
    # avoid race condition
    if not group_nb_dl.is_set():
        new_leader = None

    approve_bsm_q.queue.clear() 
    approve_nbsm_q.queue.clear() 
    approve_tb_q.queue.clear() 
    approve_ntb_accq.queue.clear() 

    curr_DSRC_block.clear()
    curr_CAM_block.clear()
    curr_RADAR_block.clear()
    updated_group_list.clear() # clear the updated group list to avoid unexpected behaviors

    cam_list_veri.clear()



#-----------------------------------------------------------------------------
# Below are the functions to handle the BSM data

# Compute distance between two coordinates, This is to compute if the CAV sending BSM to us is in our range for camera and radar check
def haversine(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0
    
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Difference in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    
    return distance * 1000

# This compute the azimuth from coordinate 1 to coordinate 2
def azimuth_compute(lat1, lon1, lat2, lon2):
    # latitude longitude to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # compute difference
    dlon = lon2 - lon1
    # compute 
    x =  math.cos(lat2) * math.sin(dlon)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    initial_bearing = math.atan2(x, y)
    # 0 to 360 degrees
    compass_bearing = (math.degrees(initial_bearing) + 360) % 360
    
    return compass_bearing



# Create public and private keys for this node
public_k, private_k = generate_ppkeys()

# Here are the weights for different BSM verification message
BSM_veri_weight = [0.5, 1, 2]
# Here are the weights for BSM messages being verified. It is slightly higher because Presence of vehicle is important to ensure the data is not from
# an unpresented CAV/RSU
BSM_ori_weight = [weight*1.2 for weight in BSM_veri_weight]

# Switch for check if the current node is wellknown or not
well_known_node = False

# Switch for check if the bd service is start or not
start_bd_service = False

# Switch for check if this node's address is sent
addr_sent = False

# Store current node's IP address
curr_addr = 'null'
curr_membership = 'null'

# inital group number is 8
init_group_num = 8
# This is max group number node need for consensus
group_consensus_num = math.ceil((init_group_num-1)*(2/3))


# This is max block for reputation number, We only count these much block to decide every nodes' reputation
bc_max_block_count = 8
# This is decay variable factor, set to 0.15 for consider the behavior of most 4 blocks back
coeff_decay = 0.15

# These are sigmoid function coefficient, it is decided by multiple factors
coeff_a = 35
coeff_b = 0.2

# This is RSU bonus score, RSU can provide extra credit from CA
RSU_bonus = 0.2


# This Node ID
This_node_id = hash_sha3(public_k)

# The node which is going to handle the list of carla nodes
well_known_ip = '192.168.5.2'
well_known_port = int(sys.argv[2])
addr_list.add((well_known_ip, well_known_port))

# Wellknown socket created
Carla_API_socket = Carla_API_sock()

# Create a UDP socket
CAV_sock = Carla_UDP_CAVNode()
timeout = 2 # Start with timeout 2
input = [CAV_sock, Carla_API_socket]
output = []
exception_list = []

# This is the startup consensus messages
Addr_Consensus = {"type": "addr", "Carla_ID": 'null', "public_key": public_k}
Addr_ack_msg = {'type': "addr_ack", 'ip_port': "null"}

# This is consensus message after blockchain consensus has started
Addr_Consensus_bc = {"type": "addr", "Carla_ID": 'null', "public_key": public_k, "membership": "null"}

# This is initial consensus for group and intent to only use ONCE
# We will let Carla_API decide whether this node is CAV or RSU
Init_gm_consensus = {"type": "init_gm_reply", "membership": "null"} 
Init_node_list = []
Init_gm_consensus_sw = False
Init_gm_assi = False
Init_thread_fin = False
Init_group_assign_complete = False

# This is new block ready message. It is send to every non-group member for them to download the block from a random group member
Block_ready_msg = {"type": "New_block", "block_id": "null"}

# This is faulty message, when the message are received means something wrong happened
Faulty_msg = {"type": "faulty", "error_msg": "null"} 

# These are flags for initial group members
init_group_list = []

# This gives the ID of their CAV
Carla_ID_assign = None
Initial_Cons_msg_status = False

# This list stores this node's BSM for every interval
curr_BSM = None
# This list stores DSRC target for BSM
curr_DSRC = None

# These variables stores the current CAV data
curr_orientation = 0
curr_timestamp = 0
curr_lati = 0
curr_longi = 0

# This variable decide the range of the CAV will be detected by Camera (meters)
Camera_range = 35
# This variable decide the range of CAVs will be detected by Radar (meters)
Radar_range = 30

# This is angle tolerance for Camera detection and radar, the range is +-value.
# We set to 2 degrees
Tolerance_angle_range = 2

# This is the depth tolerance for radar detection
Tolerance_depth_range = 2


# Here is a list to store BSM received from DSRC
BSM_DSRC_msgs = []
# Here is a list to store the Camera verification result
BSM_Cam_veri = []

# This stores the incoming DSRC verification messages (tons of it)
dsrc_veri_queue = queue.Queue()
# This is stop event for DSRC verification thread
dsrc_stop = threading.Event()
# This event set when DSRC are incoming
dsrc_veri = threading.Event()
# dsrc thread for verifing dsrc message
dsrc_veri_thread = threading.Thread(target=BSM_DSRC_verifier, args=(dsrc_stop, dsrc_veri))
dsrc_veri_thread.start()

# This stores the incoming camera verification messages
cam_veri_queue = queue.Queue()
# This is stop event for camera verification thread
cam_stop = threading.Event()
# This event is set when incoming CAM verify messages
cam_veri = threading.Event()
# This event tells the new verification list is ready
cam_list_veri = threading.Event()

# Cam thread for dealing cam verification message
cam_veri_thread = threading.Thread(target=group_camera_verifier, args=(cam_stop, cam_veri, cam_list_veri))
cam_veri_thread.start()

# Another queue to stores the incoming radar verification message
radar_veri_queue = queue.Queue()
# This is stop event for radar verification thread
radar_stop = threading.Event()
# This event is set when incoming radar verify messages
radar_veri = threading.Event()

# radar thread for verifing radar verification message
radar_veri_thread = threading.Thread(target=group_radar_verifier, args=(radar_stop, radar_veri))
radar_veri_thread.start()

# Time block thread for verifing message
# This is time block consensus queue
timeblock_queue = queue.Queue()
# This is stop event for time block
timeblock_stop = threading.Event()
# This is timeout event for time block, time block thread is going to check condition for every 2 seconds or when they finish computing everyone's reputation
timeblock_timeout_call = threading.Event()
# This thread is for group node doing time consensus
timeblock_thread = threading.Thread(target=time_chain_consensus, args=(timeblock_stop, timeblock_timeout_call, CAV_sock))
timeblock_thread.start()

# This thread is used to compute the new BSM data block
bsm_block_stop = threading.Event()
# This event is used to start the BSM block creation
start_BSM_block = threading.Event()
# Main thread of BSM block creation
BSM_db_creation = threading.Thread(target=BSM_data_block_consensus, args=(bsm_block_stop, start_BSM_block, CAV_sock))
BSM_db_creation.start()

# This thread is used for computing the next group leader in each group node
leader_elec_stop = threading.Event()
start_elec_call = threading.Event()

leader_elect_thread = threading.Thread(target=group_leader_election, args=(leader_elec_stop, start_elec_call, start_BSM_block))
leader_elect_thread.start()

# This thread is for group member to used for verifying the new BSM data block
BSM_block_cons_stop = threading.Event()
# This thread is used to trigger the verification of new block
BSM_veri_start = threading.Event()
BSM_block_veri_thread = threading.Thread(target=BSM_db_consensus, args=(BSM_block_cons_stop, BSM_veri_start, CAV_sock))
BSM_block_veri_thread.start()

# This thread compute the reputation for each nodes to decide who will be the next group member
Rep_score_stop = threading.Event()
Rep_count_start = threading.Event() # block_Ready_checker and Bd_TCP_dl
Rep_score_thread = threading.Thread(target=Reputation_computing, args=(Rep_score_stop, Rep_count_start, CAV_sock))
Rep_score_thread.start()

# Start tcp service for download data
tcp_stop = threading.Event()
# Send download block once receive UDP confirm message
get_block = threading.Event()
# This is the event for group members to download the new block
group_nb_dl = threading.Event()
# This is the event for regular nodes to collect new BSM block
reg_node_dl = threading.Event()
# This thread will download the block when new block is ready
tcp_thread = threading.Thread(target=Bd_TCP_dl, args=(tcp_stop, get_block, group_nb_dl, BSM_veri_start, reg_node_dl, Rep_count_start))
tcp_thread.start()

# These are the related event and start event for BSM block ready download checker thread
block_ready_stop = threading.Event()
block_receive = threading.Event()
block_ready_thread = threading.Thread(target=block_Ready_checker, args=(block_ready_stop, block_receive, Rep_count_start, CAV_sock))
block_ready_thread.start()


# These are time-block's initial start variables
init_tb_starttime_interval = 28
# After optimization, we now taking 1 seconds
init_max_blockcreate_time = 8

# This is the estimated time for the start time of a new db. It is used to compute the new interval between start time and end time of db block creation
# The starttime_interval will be: (final time gather veri - last db block end time)
final_time_gather_BSM_veri = 0

# The max vote time for time block is 6 seconds
# This is the base time for group node to process and vote the time block
base_t_vote = 4
# This variable is the modular value for compute the max voting time.
max_t_vote_mod = 3
# This is the final vote time for time block
final_vote_time = 0

# This is vote count for the time chain. Reset once reputation count is complete
time_block_vote_rec = 0
# This is the flag for record if we bd our time block. Reset once reputation count is complete
new_time_block_bd = False
# This is the flag for group member to stop time block consensus. Reset once reputation count is complete
This_time_block_cons = False
# This is the ID of the current time block we create. The block ID are always same for all consensus
This_time_block_ID = None

# This is the new leader for creating the data block, going to update once new time block is created
new_leader = None

# This is the vote count for new data block
BSM_block_vote = 0
# This is the flag to ensure we only send stop consensus once
new_BSM_block_bd = False
# This flag is ensuring the timeout for BSM block is over or not
This_BSM_block_cons = False 

# This is the queue keep track of approve messages for this BSM block
approve_bsm_q = queue.Queue()
# This is the queue keep track of the approve message send by leader
approve_nbsm_q = queue.Queue()

# This queue keep track of approve message for this node's time block
approve_tb_q = queue.Queue()
# This queue keep track of the approve message send by the accepted time block node
approve_ntb_accq = queue.Queue()

# This counter tracks when the group node is ready for let CAV members to download the new block
new_BSMb_counter = 0

# This counter tracks how many new group members are ready for switch group, reset when either new group member receive enough response
# Or the new group node receive message from a node claim new group is ready
new_group_ack_count = 0

# Stop switch
stop_switch = False

try:
    while True:
        # manually flush stdout
        sys.stdout.flush()

        # We start subnet communication after we assigned IDs from CarlaAPIServer
        # This message should only send once
        if Carla_ID_assign != None and Initial_Cons_msg_status == False:
            init_consensus(CAV_sock, well_known_ip, well_known_port)
            Initial_Cons_msg_status = True

        readable, writable, exception = select.select(input, output, exception_list, timeout)

        for source in readable:
            if source is CAV_sock:
                msg, addr = source.recvfrom(14500)
                CAV_node_msg = json.loads(msg.decode())
                if CAV_node_msg['type'] == 'wellknown':
                    well_known_node = True
                elif CAV_node_msg['type'] == 'addr' and well_known_node:
                    # All non well-known address are add to well-known address set
                    addr_list.add(addr)
                    # Store the pairs of ID and Address
                    Carla_addr_list[CAV_node_msg['Carla_ID']] = addr
                    # Store the pairs of addr and public key 
                    pb_k_Carla_list[addr] = CAV_node_msg["public_key"]
                    # Compute and store the hash of the public key
                    node_hashid_list[hash_sha3(pb_k_Carla_list[addr])] = pb_k_Carla_list[addr]
                    # Add nodeID to address here for well-known node
                    node_id_addr_list[hash_sha3(pb_k_Carla_list[addr])] = addr

                    # Add new membership to node come after at least one block is created
                    if len(curr_BSM_blockchain) > 0:
                        nodeid_membership[hash_sha3(pb_k_Carla_list[addr])] = CAV_node_msg["membership"]

                    # Sent address back to the node for them to realize who they are
                    Addr_ack_msg['ip_port'] = addr
                    CAV_sock.sendto(json.dumps(Addr_ack_msg).encode('utf-8'), addr)
                elif CAV_node_msg['type'] == 'addr_ack':
                    curr_addr = tuple(CAV_node_msg['ip_port'])
                    print(f"addr_ack: {curr_addr}")
                    addr_sent = True
                elif CAV_node_msg['type'] == 'well_known_bd':
                    # Parse the bd from well-known node
                    income_set = set()
                    for elem in CAV_node_msg['ip_list']:
                        income_set.add(tuple(elem))
                    if addr_list != income_set:
                        for ip in income_set:
                            if ip not in addr_list:
                                addr_list.add(ip)
                    
                    # This should use only once. It should not be used after the new BSM veri Block is created
                    income_group = set()
                    for elem in CAV_node_msg['group_list']:
                        income_group.add(tuple(elem))
                    if group_list != income_group:
                        for ip in income_group:
                            if ip not in group_list:
                                # After blockchain is started, this routine is for checking if the group size is match with wellknown node or not
                                if len(curr_BSM_blockchain) == 0:
                                    group_list.add(ip)

                        if len(curr_BSM_blockchain) > 0:
                            print(f"Group size is not match with boardcast address, could be error or the group members are switching")

                    income_Carla_id = set()
                    for elem in CAV_node_msg['Carla_addr']:
                        income_Carla_id.add(int(elem))
                    if set(Carla_addr_list) != income_Carla_id:
                        for id in income_Carla_id:
                            if id not in Carla_addr_list:
                                Carla_addr_list[id] = tuple(CAV_node_msg['Carla_addr'][str(id)])

                    # Key-pair of id and membership
                    income_group_ms = set()
                    for elem in CAV_node_msg['addr_members']:
                        income_group_ms.add(elem)
                    if set(nodeid_membership) != income_group_ms:
                        for id in income_group_ms:
                            if id not in nodeid_membership:
                                nodeid_membership[id] = CAV_node_msg['addr_members'][id]

                elif CAV_node_msg['type'] == 'wk_bd_public_key':
                    # Special case for public keys, the key is too long for a single UDP message to handle
                    curr_pk = set()
                    for elem in pb_k_Carla_list:
                        curr_pk.add(tuple(elem))
                    if tuple(CAV_node_msg['ip_addr']) not in curr_pk:
                        pb_k_Carla_list[tuple(CAV_node_msg['ip_addr'])] = CAV_node_msg['public_key']

                    # Compute the hash of public key from every nodes as BSM/general ID
                    for elem in pb_k_Carla_list:
                        node_hashid_list[hash_sha3(pb_k_Carla_list[elem])] = pb_k_Carla_list[elem]
                        # We also compute the id and addr pair here
                        node_id_addr_list[hash_sha3(pb_k_Carla_list[elem])] = elem
                elif CAV_node_msg['type'] == 'Init_group_consensus':
                    # Reply with the type of the node
                    CAV_sock.sendto(json.dumps(Init_gm_consensus).encode('utf-8'), addr)
                elif CAV_node_msg['type'] == 'init_gm_reply':
                    # Store the gm reply (type of node) in the list
                    Init_node_list.append((addr, CAV_node_msg['membership']))
                    # Assign to a bigger dictionary
                    nodeid_membership[hash_sha3(pb_k_Carla_list[addr])] = CAV_node_msg["membership"]
                elif CAV_node_msg['type'] == 'Init_group_member':
                    # messages for initial group assignment. The members receive this message will become initial group members
                    if CAV_node_msg['message'] == 'New_member':
                        Group_mem_assi_ack = {"type": "Init_group_member", "message": "ack"}
                        CAV_sock.sendto(json.dumps(Group_mem_assi_ack).encode('utf-8'), addr)
                    elif CAV_node_msg['message'] == 'ack' and len(init_group_list) < init_group_num:
                        print("ack", addr)
                        init_group_list.append(addr)
                elif CAV_node_msg['type'] == 'BSM_node':
                    v_result = BSM_verifier(CAV_node_msg, node_hashid_list[CAV_node_msg['node_id']])
                    if v_result:
                        print(f"BSM Message: {CAV_node_msg['hash']} at {CAV_node_msg['timestamp']} from {addr} verification complete")
                        # Send DSRC approve message to the sender and group members 
                        DSRC_approve_msg = BSM_approve_response(CAV_node_msg, This_node_id, private_k)
                        CAV_sock.sendto(json.dumps(DSRC_approve_msg).encode('utf-8'), addr)
                        for group_addr in group_list:
                            CAV_sock.sendto(json.dumps(DSRC_approve_msg).encode('utf-8'), group_addr)
                        # Save incoming BSM here, we are going to remove unused BSM later
                        BSM_DSRC_msgs.append(CAV_node_msg)

                    elif not v_result:
                        # Return a faulty message if v_result failed
                        Faulty_msg["error_msg"] = 'Digital Signature verification Failed'
                        CAV_sock.sendto(json.dumps(Faulty_msg).encode('utf-8'), addr)
                elif CAV_node_msg['type'] == 'faulty':
                    # Handle faulty messages
                    print(f"Error message: {CAV_node_msg['error_msg']}")

                elif CAV_node_msg['type'] == 'BSM_DSRC_approve':
                    # When receive an approve message, the group node (and sender) should verify the signatures
                    if curr_addr in group_list:
                        dsrc_veri_queue.put(CAV_node_msg)
                        dsrc_veri.set() # set to let DSRC thread work
                    elif CAV_node_msg['ori_BSM']['node_id'] == This_node_id:
                        print(f"This node's Message {CAV_node_msg['hash_ori']} from {CAV_node_msg['node_id']} is verified")
                    else:
                        print('invalid message received, going to ignore it.')
                elif CAV_node_msg['type'] == 'cam_veri_node':
                    # We put the incoming camera verification message in the queue for thread to process
                    if curr_addr in group_list:
                        cam_veri_queue.put(CAV_node_msg)
                        cam_veri.set() # Get thread to work
                    else:
                        print('invalid message received, going to ignore it.')
                elif CAV_node_msg['type'] == 'radar_veri_node':
                    # Now we put the incoming radar verification message in the queue for thread to process
                    if curr_addr in group_list:
                        radar_veri_queue.put(CAV_node_msg)
                        radar_veri.set() # start verify thread
                    else:
                        print('invalid message received, going to ignore it.')
                elif CAV_node_msg['type'] == 'time_block_cons':
                    if curr_addr in group_list:
                        timeblock_queue.put(CAV_node_msg)
                        timeblock_timeout_call.set()
                elif CAV_node_msg['type'] == 'tb_stop_cons':
                    # When we receive stop consensus msg, we will stop consensus and recognize current block as new time block
                    approve_ntb_accq.put(CAV_node_msg)
                    if approve_ntb_accq.qsize() == group_consensus_num and CAV_node_msg['block_id'] in curr_time_blockchain:
                        while not approve_ntb_accq.empty():
                            tb_veri_msg = approve_ntb_accq.get()
                            veri_result = signature_verifier(tb_veri_msg['block_hash'], tb_veri_msg['digital_signature'], node_hashid_list[tb_veri_msg['node_id']])
                            if veri_result == False:
                                break
                        if veri_result == True:
                            This_time_block_cons = True
                            print(f"New time block confirmed, timeout for new time block stopped. ST: {curr_time_blockchain[CAV_node_msg['block_id']]['data_block_start_t']}")
                            start_elec_call.set() # Group members should ready for compute leader after new time block is ready
                        else:
                            print("Failed to confirm the new time block, the consensus continues")
                elif CAV_node_msg['type'] == 'time_block':
                    # When receive a time_block consensus, we will simply assign the block ID to block
                    # This is because when a new consensus happens, we will do this again for every nodes
                    # The order of the time block could be arranged by hash (and data_block_start_t)
                    curr_time_blockchain[CAV_node_msg['prev_tb_hash']] = CAV_node_msg
                    if approve_ntb_accq.qsize() == group_consensus_num and CAV_node_msg['block_id'] in curr_time_blockchain:
                        while not approve_ntb_accq.empty():
                            tb_veri_msg = approve_ntb_accq.get()
                            veri_result = signature_verifier(tb_veri_msg['block_hash'], tb_veri_msg['digital_signature'], node_hashid_list[tb_veri_msg['node_id']])
                            if veri_result == False:
                                break
                        if veri_result == True:
                            This_time_block_cons = True
                            print(f"New time block confirmed, timeout for new time block stopped. ST: {curr_time_blockchain[CAV_node_msg['prev_tb_hash']]['data_block_start_t']}")
                            start_elec_call.set() # Group members should ready for compute leader after new time block is ready
                        else:
                            print("Failed to confirm the new time block, the consensus continues")
                elif CAV_node_msg['type'] == 'time_block_approve':
                    if curr_addr in group_list:
                        if time_block_vote_rec < group_consensus_num:
                            if signature_verifier(CAV_node_msg['block_hash'], CAV_node_msg['digital_signature'], node_hashid_list[CAV_node_msg['node_id']]):
                                time_block_vote_rec += 1
                                # We need to keep all approve message in a queue, and boardcast to group members once we have enough vote
                                approve_tb_q.put(CAV_node_msg)
                        elif time_block_vote_rec >= group_consensus_num and new_time_block_bd == False:
                            print(f"Our time block has reach max group vote, going to send to everyone. ST: {this_node_time_block['data_block_start_t']}")
                            This_time_block_cons = True # Stop timeout for ourselves
                            new_time_block = copy.deepcopy(this_node_time_block)
                            new_time_block['type'] = 'time_block'

                            # Send new time block to every node (except itself)
                            for addr_ip in addr_list:
                                if addr_ip != curr_addr:
                                    CAV_sock.sendto(json.dumps(new_time_block).encode('utf-8'), addr_ip)
                            # Send approve message to every group node to stop them starting new consensus
                            while not approve_tb_q.empty():
                                approve_msg = copy.deepcopy(approve_tb_q.get())
                                approve_msg['type'] = 'tb_stop_cons'
                                for addr_ip in group_list:
                                    if addr_ip != curr_addr:
                                        CAV_sock.sendto(json.dumps(approve_msg).encode('utf-8'), addr_ip)
                            start_elec_call.set() # Group members should ready for compute leader after new time block is ready
                            new_time_block_bd = True
                    else:
                        print('invalid message received, going to ignore it.')
                elif CAV_node_msg['type'] == 'New_consensus_block':
                    print("Consensus block request received, going to download the block from leader")
                    if curr_addr in group_list:
                        group_nb_dl.set()
                        get_block.set()
                elif CAV_node_msg['type'] == 'BSM_block_approve':
                    if curr_addr in group_list:
                        if BSM_block_vote < group_consensus_num:
                            if signature_verifier(CAV_node_msg['block_hash'], CAV_node_msg['digital_signature'], node_hashid_list[CAV_node_msg['node_id']]):
                                BSM_block_vote += 1
                                approve_bsm_q.put(CAV_node_msg)
                        elif BSM_block_vote >= group_consensus_num and new_BSM_block_bd == False:
                            print(f"Our Data block has reach max group vote, going to send to everyone the signatures.")
                            This_BSM_block_cons = True # Stop the timeout for ourselves
                            # We are going to store the signatures to the new block 
                            curr_BSM_veri_block['app_signs'] = {}
                            while not approve_bsm_q.empty():
                                approve_msg = copy.deepcopy(approve_bsm_q.get())
                                # Store signature to newly created BSM block
                                curr_BSM_veri_block['app_signs'][approve_msg['node_id']] = approve_msg['digital_signature']
                                approve_msg['type'] = 'db_stop_cons'
                                for addr_ip in group_list:
                                    if addr_ip != curr_addr:
                                        CAV_sock.sendto(json.dumps(approve_msg).encode('utf-8'), addr_ip)
                            # assign new block to blockchain
                            curr_BSM_blockchain[curr_BSM_veri_block['block_id']] = curr_BSM_veri_block 
                            Rep_count_start.set()
                            new_BSM_block_bd = True
                elif CAV_node_msg['type'] == 'db_stop_cons':
                # This is the consensus for stop time out for time chain when new BSM db created
                    approve_nbsm_q.put(CAV_node_msg)
                    block_receive.set() # Set the event to start to check if stop condition is match
                elif CAV_node_msg['type'] == 'group_block_readydl':
                    new_BSMb_counter += 1
                    if new_BSMb_counter == (len(group_list) - 1):
                        for cav_ip in addr_list:
                            if cav_ip not in group_list:
                                CAV_sock.sendto(json.dumps({"type": "new_block_ready"}).encode('utf-8'), cav_ip)
                        new_BSMb_counter = 0
                elif CAV_node_msg['type'] == 'new_block_ready':
                    # Download new block from any group member
                    print("New BSM block seems ready, Now download new BSM block!")
                    get_block.set()
                    reg_node_dl.set()
                
                elif CAV_node_msg['type'] == 'new_group_consensus':
                    if addr in updated_group_list:
                        if CAV_node_msg['message'] == 'check' and curr_addr in updated_group_list:
                            CAV_sock.sendto(json.dumps({"type": "new_group_consensus", "message": "ack"}).encode('utf-8'), addr)
                        elif CAV_node_msg['message'] == 'ack':
                            new_group_ack_count += 1
                            print('ack group:', addr)
                            if new_group_ack_count == (init_group_num - 1):
                                print("We collect enough response, move on to switch group")
                                for group_mem in updated_group_list:
                                    CAV_sock.sendto(json.dumps({"type": "new_group_consensus", "message": "Switch_group"}).encode('utf-8'), group_mem)

                                for old_group_mem in group_list:
                                    CAV_sock.sendto(json.dumps({"type": "new_group_consensus", "message": "Safe_switch"}).encode('utf-8'), old_group_mem)
                        elif CAV_node_msg['message'] == 'Switch_group':
                            new_group_ack_count = 0
                            group_list = set(updated_group_list)
                            print("New group consensus complete")
                            # We refresh all needed variables here for new group members
                            refresh_variables()
                        # The old group members will switch to new group members list after they receive consensus from new group member
                        if curr_addr in group_list:
                            if CAV_node_msg['message'] == 'Safe_switch':
                                group_list = set(updated_group_list)
                                print("group safely switched")
                                # We refresh all needed variables here for old group members
                                refresh_variables()


            elif source is Carla_API_socket:
                msg, addr = source.recvfrom(8192)
                CAV_API_msg = json.loads(msg.decode())

                if CAV_API_msg['type'] == 'BSM':
                    curr_BSM = BSM_for_CAV_node(CAV_API_msg, This_node_id, private_k)

                    curr_orientation = curr_BSM['orientation']
                    curr_timestamp = curr_BSM['timestamp']
                    curr_lati = curr_BSM['latitude']
                    curr_longi = curr_BSM['longitude']
                    curr_DSRC = CAV_API_msg["DSRCList"]

                    # Send BSM to its DSRC receivers
                    for cav_id in curr_DSRC:
                        CAV_sock.sendto(json.dumps(curr_BSM).encode('utf-8'), Carla_addr_list[cav_id]) 

                    # Update BSM when current time stamp are definitely updated (if we don't want do this way, we have to use a thread)
                    BSM_message_updater(BSM_DSRC_msgs, curr_timestamp)
                elif CAV_API_msg['type'] == 'CAM_result':
                    # We first compute the received BSM is in range of camera or not
                    CAV_inrange = BSM_CAV_in_range(BSM_DSRC_msgs, curr_lati, curr_longi, Camera_range)
                    # Then we compute if the azimuth is match with BSM
                    Cam_veri_result = BSM_azimuth_check(CAV_inrange, CAV_API_msg, This_node_id, private_k, (curr_lati, curr_longi), Tolerance_angle_range)
                    # Now we send the Camera verification result to group members
                    for cam_msg in Cam_veri_result:
                        for group_mem in group_list:
                            CAV_sock.sendto(json.dumps(cam_msg).encode('utf-8'), group_mem)

                # This should be the initial msg (Start of CARLA API)
                elif CAV_API_msg['type'] == 'CARLA_ID':
                    Carla_ID_assign = CAV_API_msg['ID']
                    curr_membership = CAV_API_msg['MEMBER']
                    Init_gm_consensus["membership"] = curr_membership

                    print(f"This node is assigned as {curr_membership}")
                elif CAV_API_msg['type'] == 'cam_veri_result':
                    # This section is because we don't have direct access to API
                    # We are going to keep a dictionary for camera verification result.
                    CAM_veri_handler(BSM_Cam_veri, CAV_API_msg, curr_timestamp)
                    if len(BSM_Cam_veri) == len(addr_list) and curr_addr in group_list:
                        cam_list_veri.set() # Tell the thread the verification list is ready
                        cam_veri.set() # Also let the thread handle the tasks
                        # We will tackle Carla API a bit to handle the possible attack in camera
                elif CAV_API_msg['type'] == 'radar':
                    # We are going to check if there are CAV match with our radar's result
                    # We first narrow down the targets to BSM in radar range
                    CAV_inrange = BSM_CAV_in_range(BSM_DSRC_msgs, curr_lati, curr_longi, Radar_range)
                    radar_veri_result = BSM_Radar_check(CAV_inrange, CAV_API_msg, This_node_id, private_k, (curr_lati, curr_longi), Tolerance_depth_range, Tolerance_angle_range, curr_membership)
    
                    # Now we send radar result to all group members for verification
                    for radar_msg in radar_veri_result:
                        for group_mem in group_list:
                            CAV_sock.sendto(json.dumps(radar_msg).encode('utf-8'), group_mem)

                    # Yes, the final time for nodes to wait to create a new block are the time they receive a radar veri msg + 5 seconds
                    final_time_gather_BSM_veri = time.time() + 5
                elif CAV_API_msg['type'] == 'Stop':
                    print("Receive stop message")
                    stop_switch = True
            

        # Well known node consensus
        if well_known_node == False and addr_sent == False and Carla_ID_assign != None and Initial_Cons_msg_status == True:
            Addr_Consensus["Carla_ID"] = Carla_ID_assign
            print("Node is not Main peer! sending address and Carla ID to the wellknown node")
            CAV_sock.sendto(json.dumps(Addr_Consensus).encode('utf-8'), (well_known_ip, well_known_port))

        # IP boardcast service
        if well_known_node == True and start_bd_service == False and Carla_ID_assign != None:
            curr_addr = (well_known_ip, well_known_port)
            # Assign the carla ID to the well known node's address
            Carla_addr_list[Carla_ID_assign] = curr_addr
            # Assign the addr to the public key
            pb_k_Carla_list[curr_addr] = public_k
            # Add quick access list (node ID)
            node_hashid_list[This_node_id] = public_k
            # Add membership pair
            nodeid_membership[This_node_id] = curr_membership

            # Start ip boardcast service
            bd_stop = threading.Event()
            # The boardcast service only start by well_known node
            bd_thread = threading.Thread(target=node_group_ip_bd, args=(CAV_sock, bd_stop, (well_known_ip, well_known_port)))
            bd_thread.start()
            # Start only 1 thread for boardcast
            start_bd_service = True

        # Group Consensus Service
        if start_bd_service == True and Init_gm_consensus_sw == False:
            print("Start Initial group assignment")
            init_thread = threading.Thread(target=Init_group_consensus, args=(CAV_sock, ))
            init_thread.start()
            Init_gm_consensus_sw = True

        if len(Init_node_list) > 0 and len(Init_node_list) == (len(addr_list)-1) and Init_gm_consensus_sw == True and Init_gm_assi == False and Init_thread_fin == True:
            print("Start initial group assignment by sending request")
            Init_group_assign(CAV_sock, init_group_num)
            Init_gm_assi = True

        # if group number in the init_group_list reach the preset group member, then the well-known node is going to boardcast the group
        if len(init_group_list) == init_group_num and Init_group_assign_complete == False:
            print("Initial group assignment complete!")
            group_list = set(init_group_list)
            Init_group_assign_complete = True

        # Stop condition
        if stop_switch == True:
            sys.stdout.flush()
            tcp_stop.set()
            # These two are required to stop the dsrc thread
            dsrc_stop.set()
            dsrc_veri.set()
            # These two are required to stop the cam thread
            cam_stop.set()
            cam_veri.set()
            # These two are required to stop the radar thread
            radar_stop.set()
            radar_veri.set()
            # This requires to stop time block thread
            timeblock_stop.set()
            # These are for stopping the election thread
            leader_elec_stop.set()
            start_elec_call.set()
            # These are for stopping the BSM block creation
            bsm_block_stop.set()
            start_BSM_block.set()
            # These are for stop consensus group thread
            BSM_block_cons_stop.set()
            BSM_veri_start.set()
            # These are for BSM ready thread
            block_ready_stop.set()
            block_receive.set()
            # Thread are events for reputation count
            Rep_score_stop.set()
            Rep_count_start.set()

            if well_known_node == True:
                bd_stop.set()
            break

except Exception as e:
    tb = traceback.format_exc()
    print(tb)


except KeyboardInterrupt:
    print('Closing socket (keyboard)')
    tcp_thread.join()
    cam_veri_thread.join()
    dsrc_veri_thread.join()
    radar_veri_thread.join()
    timeblock_thread.join()
    leader_elect_thread.join()
    BSM_db_creation.join()
    BSM_block_veri_thread.join()
    block_ready_thread.join()
    Rep_score_thread.join()
    if well_known_node:
        bd_thread.join()
        init_thread.join()
    CAV_sock.close()
    Carla_API_socket.close()


finally:
    sys.stdout.flush()
    print('Closing socket (final)')
    tcp_thread.join()
    cam_veri_thread.join()
    dsrc_veri_thread.join()
    radar_veri_thread.join()
    timeblock_thread.join()
    BSM_db_creation.join()
    leader_elect_thread.join()
    BSM_block_veri_thread.join()
    block_ready_thread.join()
    Rep_score_thread.join()
    if well_known_node:
        bd_thread.join()
        init_thread.join()
    CAV_sock.close()
    Carla_API_socket.close()
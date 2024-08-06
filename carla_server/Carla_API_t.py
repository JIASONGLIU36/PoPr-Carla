import carla
import random
import math
import time
import queue
import numpy as np
import cv2
import sys
import threading
import json
import hashlib

import socket
import select

from Sensor_class import Sensors

# Use UDP setting to reduce overhead of handshaking
def Carla_UDP_socket(host='0.0.0.0', port=8011):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((host, port))
    print(f"Carla Server at your service at {host}:{port}")

    return server_socket

def status_id(status_num):
    if status_num == 0:
        return "Simulation time"
    elif status_num == 1:
        return "Capture and processing time"
    elif status_num == 2:
        return "Server time"

def thread_simu_capture_serv_switch(stop_event):
    while not stop_event.is_set():
        global serv_capture_simu_act
        global processing_switch
        global timeout
        # Capture and processing time will keep running if the main is processing data
        if serv_capture_simu_act == 0 or processing_switch == True:
            # capture time
            serv_capture_simu_act = 1
            timeout = 0
            Status = status_id(serv_capture_simu_act)
            print(f"Turn switch {Status}: 2 seconds message")
            time.sleep(2)     # Cannot estimate precise time!
        elif serv_capture_simu_act == 1 and processing_switch == False:
            # server time
            serv_capture_simu_act = 2
            timeout = 1
            Status = status_id(serv_capture_simu_act)
            print(f"Turn switch {Status}")
            time.sleep(15)
        elif serv_capture_simu_act == 2 and processing_switch == False:
            # simulation time
            serv_capture_simu_act = 0
            timeout = 0
            Status = status_id(serv_capture_simu_act)
            print(f"Turn switch {Status}")
            time.sleep(25) # This is super slow

# A special case for ego_vehicle
def get_BSM_ego(ego_sensor, sensor_list, RSU_list,  DSRC_range):
    dsrc_CAV = [] # store a list for DSRC reachable vehicles
    curr_vehi_latitude = ego_sensor.get_GNSS().latitude
    curr_vehi_longitude = ego_sensor.get_GNSS().longitude
    curr_timestamp = ego_sensor.get_timestamp()
    curr_orientation = ego_sensor.get_front_orientation()

    # need to add RSU to the entire list
    full_list = dict(sensor_list)
    full_list.update(RSU_list)
    for sensor in full_list:
        if Sensors.haversine(curr_vehi_latitude, curr_vehi_longitude, full_list[sensor].get_GNSS().latitude, full_list[sensor].get_GNSS().longitude) <= DSRC_range:
            dsrc_CAV.append(sensor)


    BSM_central_msg = {"type": "BSM", "orientation": curr_orientation, "timestamp": curr_timestamp, "latitude": curr_vehi_latitude, "longitude": curr_vehi_longitude, "DSRCList": dsrc_CAV}
    json_msg = json.dumps(BSM_central_msg, indent=1).encode('UTF-8')
    return json_msg

# Compute who could receive BSM by range, then send BSM message and a set of CAV ip for delivering the BSM
def get_BSM(carID, ego, sensor_list, RSU_list, DSRC_range):
    dsrc_CAV = [] # store a list for DSRC reachable vehicles
    curr_vehi_latitude = sensor_list[carID].get_GNSS().latitude
    curr_vehi_longitude = sensor_list[carID].get_GNSS().longitude
    curr_timestamp = sensor_list[carID].get_timestamp()
    curr_orientation = sensor_list[carID].get_front_orientation()

    # Need to add ego_vehicle to the list for BSM!
    full_list = dict(sensor_list)
    full_list[ego.get_carID()] = ego
    full_list.update(RSU_list)
    for sensor in full_list:
        if carID != sensor and Sensors.haversine(curr_vehi_latitude, curr_vehi_longitude, full_list[sensor].get_GNSS().latitude, full_list[sensor].get_GNSS().longitude) <= DSRC_range:
            dsrc_CAV.append(sensor)

    BSM_central_msg = {"type": "BSM", "orientation": curr_orientation, "timestamp": curr_timestamp, "latitude": curr_vehi_latitude, "longitude": curr_vehi_longitude, "DSRCList": dsrc_CAV}
    json_msg = json.dumps(BSM_central_msg, indent=1).encode('UTF-8')

    return json_msg

def get_RSU_BSM(rsuID, ego, sensor_list, RSU_list, DSRC_range):
    dsrc_CAV = [] # store a list for DSRC reachable vehicles
    curr_vehi_latitude = RSU_list[rsuID].get_GNSS().latitude
    curr_vehi_longitude = RSU_list[rsuID].get_GNSS().longitude
    curr_timestamp = RSU_list[rsuID].get_timestamp()
    curr_orientation = RSU_list[rsuID].get_front_orientation()

    # Need to add ego_vehicle to the list for BSM!
    full_list = dict(sensor_list)
    full_list[ego.get_carID()] = ego
    full_list.update(RSU_list)
    for sensor in full_list:
        if rsuID != sensor and Sensors.haversine(curr_vehi_latitude, curr_vehi_longitude, full_list[sensor].get_GNSS().latitude, full_list[sensor].get_GNSS().longitude) <= DSRC_range:
            dsrc_CAV.append(sensor)

    BSM_central_msg = {"type": "BSM", "orientation": curr_orientation, "timestamp": curr_timestamp, "latitude": curr_vehi_latitude, "longitude": curr_vehi_longitude, "DSRCList": dsrc_CAV}
    json_msg = json.dumps(BSM_central_msg, indent=1).encode('UTF-8')

    return json_msg


# Sha3 function
def hash_sha3_img(msg):
    Sha3_hash = hashlib.sha3_256()
    Sha3_hash.update(msg)
    return Sha3_hash.hexdigest()

# This function create a list of verification results CAV_ID: [hash_f, hash_r], [azimuth_f, azimuth_r] for verification
# the verification result is sending to every nodes in this simulation
def camera_veri_output(cam_veri_list, ego, sensor_list, RSU_list, camera_range):
    image_f = ego_sensor.get_FrontCam()
    image_r = ego_sensor.get_RearCam()

    image_f_hash = hash_sha3_img(np.copy(image_f.raw_data).tobytes())
    image_r_hash = hash_sha3_img(np.copy(image_r.raw_data).tobytes())

    azi_out_list = []
    for azi in ego_sensor.get_arzimuth_to_ego_nearby(camera_range, sensor_list):
        azi_out_list.append(azi[3])

    # only azimuth front will contain all azimuth results. This will change after YOLOv8 and neuron-network is ready
    cam_veri_reply = {"type": "cam_veri_result", "timestamp": ego.get_timestamp(), "CAV_ID": ego.get_carID(), "hash_front": image_f_hash, "hash_rear": image_r_hash, "azimuth_front": azi_out_list, "azimuth_rear": [], "member_type": "CAV"}
    cam_veri_list.append(json.dumps(cam_veri_reply, indent=1).encode('UTF-8'))

    # Add ego vehicle to the list
    full_list = dict(sensor_list)
    full_list[ego_sensor.get_carID()] = ego_sensor

    for key in sensor_list:
        image_f = sensor_list[key].get_FrontCam()
        image_r = sensor_list[key].get_RearCam()

        image_f_hash = hash_sha3_img(np.copy(image_f.raw_data).tobytes())
        image_r_hash = hash_sha3_img(np.copy(image_r.raw_data).tobytes())
        
        azi_out_list = []
        for azi in sensor_list[key].get_arzimuth_to_ego_nearby(camera_range, full_list):
            azi_out_list.append(azi[3])

        #-#-# only azimuth front will contain all azimuth results. This will change after YOLOv8 and neuron-network is ready
        cam_veri_reply = {"type": "cam_veri_result", "timestamp": sensor_list[key].get_timestamp(), "CAV_ID": sensor_list[key].get_carID(), "hash_front": image_f_hash, "hash_rear": image_r_hash, "azimuth_front": azi_out_list, "azimuth_rear": [], "member_type": "CAV"}
        cam_veri_list.append(json.dumps(cam_veri_reply, indent=1).encode('UTF-8'))

    for key in RSU_list:
        image_f = RSU_list[key].get_FrontCam()

        image_f_hash = hash_sha3_img(np.copy(image_f.raw_data).tobytes())
        azi_out_list = []
        for azi in RSU_list[key].get_arzimuth_to_ego_nearby(camera_range, full_list):
            azi_out_list.append(azi[3])

        cam_veri_reply = {"type": "cam_veri_result", "timestamp": RSU_list[key].get_timestamp(), "CAV_ID": RSU_list[key].get_carID(), "hash_front": image_f_hash, "azimuth_front": azi_out_list, "member_type": "RSU"}
        cam_veri_list.append(json.dumps(cam_veri_reply, indent=1).encode('UTF-8'))


# This function will output the result of YOLOv8 and neuro-network: azimuth by using camera data
# Currently is done by computing the distance between ego vehicle
def get_CAM_result_ego_simu(ego_sensor, sensor_list, camera_range):
    # The distance check is set to 35 for the simulation
    azimuth_list = ego_sensor.get_arzimuth_to_ego_nearby(camera_range, sensor_list)
    azi_out_list = []
    for azi in azimuth_list:
        azi_out_list.append(azi[3])

    image_f = ego_sensor.get_FrontCam()
    image_r = ego_sensor.get_RearCam()

    image_f_hash = hash_sha3_img(np.copy(image_f.raw_data).tobytes())
    image_r_hash = hash_sha3_img(np.copy(image_r.raw_data).tobytes())


    CAM_result = {"type": "CAM_result", "timestamp": ego_sensor.get_timestamp() , "azimuth_front": azi_out_list, "azimuth_rear": [], "hash_front": image_f_hash, "hash_rear": image_r_hash, "member_type": "CAV"}
    json_msg = json.dumps(CAM_result, indent=1).encode('UTF-8')
    return json_msg

def get_CAM_result_simu(carID, ego, sensor_list, camera_range):
    image_f = sensor_list[carID].get_FrontCam()
    image_r = sensor_list[carID].get_RearCam()

    image_f_hash = hash_sha3_img(np.copy(image_f.raw_data).tobytes())
    image_r_hash = hash_sha3_img(np.copy(image_r.raw_data).tobytes())

    # Add ego vehicle to the list
    full_list = dict(sensor_list)
    full_list[ego.get_carID()] = ego

    azimuth_list = sensor_list[carID].get_arzimuth_to_ego_nearby(camera_range, full_list)
    azi_out_list = []
    for azi in azimuth_list:
        azi_out_list.append(azi[3])


    CAM_result = {"type": "CAM_result", "timestamp": sensor_list[carID].get_timestamp() , "azimuth_front": azi_out_list, "azimuth_rear": [], "hash_front": image_f_hash, "hash_rear": image_r_hash, "member_type": "CAV"}
    json_msg = json.dumps(CAM_result, indent=1).encode('UTF-8')
    return json_msg

def get_RSU_CAM_result_simu(rsuID, ego, sensor_list, RSU_list, camera_range):
    image_f = RSU_list[rsuID].get_FrontCam()

    image_f_hash = hash_sha3_img(np.copy(image_f.raw_data).tobytes())

    # Add ego vehicle to the list
    full_list = dict(sensor_list)
    full_list[ego.get_carID()] = ego

    azimuth_list = RSU_list[rsuID].get_arzimuth_to_ego_nearby(camera_range, full_list)
    azi_out_list = []
    for azi in azimuth_list:
        azi_out_list.append(azi[3])

    CAM_result = {"type": "CAM_result", "timestamp": RSU_list[rsuID].get_timestamp() , "azimuth_front": azi_out_list, "hash_front": image_f_hash, "member_type": "RSU"}
    json_msg = json.dumps(CAM_result, indent=1).encode('UTF-8')
    return json_msg

# This function will output the result of radar
def get_radar_ego(ego):
    radar_bsm = {"type": "radar", "front_result": ""}
    radar_front_result = []

    for detect_points in ego.get_FrontRadar():
        radar_front_result.append([ego.Azimuth_for_radar(detect_points.azimuth, ego.get_front_orientation()), detect_points.depth])
    
    radar_bsm['front_result'] = radar_front_result

    radar_rear_result = []
    for detect_points in ego.get_RearRadar():
        radar_rear_result.append([ego.Azimuth_for_radar(detect_points.azimuth, ego.get_rear_orientation()), detect_points.depth])

    radar_bsm['rear_result'] = radar_rear_result

    return json.dumps(radar_bsm).encode('UTF-8')


def get_radar_result(sensor_list):
    json_result_list = {}

    for sensor in sensor_list:
        radar_bsm = {"type": "radar", "front_result": ""}
        radar_front_result = []

        for detect_points in sensor_list[sensor].get_FrontRadar():
            radar_front_result.append([sensor_list[sensor].Azimuth_for_radar(detect_points.azimuth, sensor_list[sensor].get_front_orientation()), detect_points.depth])
        
        radar_bsm['front_result'] = radar_front_result


        radar_rear_result = []
        for detect_points in sensor_list[sensor].get_RearRadar():
            radar_rear_result.append([sensor_list[sensor].Azimuth_for_radar(detect_points.azimuth, sensor_list[sensor].get_rear_orientation()), detect_points.depth])

        radar_bsm['rear_result'] = radar_rear_result

        json_result_list[sensor_list[sensor].get_carID()] = json.dumps(radar_bsm).encode('UTF-8')

    return json_result_list

def get_rsu_radar_result(rsu_list):
    json_result_list = {}

    for key in rsu_list:
        radar_bsm = {"type": "radar", "front_result": ""}
        radar_front_result = []

        for detect_points in rsu_list[key].get_FrontRadar():
            radar_front_result.append([rsu_list[key].Azimuth_for_radar(detect_points.azimuth, rsu_list[key].get_front_orientation()), detect_points.depth])
        
        radar_bsm['front_result'] = radar_front_result

        json_result_list[rsu_list[key].get_carID()] = json.dumps(radar_bsm).encode('UTF-8')
    return json_result_list


# Connect to the client and retrieve the world object
client = carla.Client('localhost', 2000)
world = client.get_world()

# client.load_world('Town05')
client.load_world('Town05_Opt', carla.MapLayer.Buildings | carla.MapLayer.ParkedVehicles)

# Toggle all buildings off
world.unload_map_layer(carla.MapLayer.Buildings)
world.unload_map_layer(carla.MapLayer.Walls)
world.unload_map_layer(carla.MapLayer.StreetLights)
world.unload_map_layer(carla.MapLayer.ParkedVehicles)

# Get spawn points
spawn_points = world.get_map().get_spawn_points()

bp_lib = world.get_blueprint_library()
vehicle_bp=bp_lib.find('vehicle.nissan.micra')
vehicle_bp.set_attribute('color', '0,100,200')

vehicle = world.try_spawn_actor(vehicle_bp, spawn_points[101])

# Set controllable vehicle
vehicle.set_autopilot(True)

# 9 colors for vehicles
color = ['0,100,200', '50, 168, 50', '70, 0, 200', '200, 0, 200', '200, 0, 0', '20, 0, 200', '0, 190, 200', '200, 200, 0', '200, 100, 0' ]

# spawn road side unit
rsu_list = []
# spawn simulate vehicles
car_list = []
# GNSS List
GNSS_List = {}
# Camera List
Front_Camera_List = {}
Rear_Camera_List = {}
# Radar List
Radar_front_list = {}
Radar_rear_list = {}

# First, we set RSU's attribute
RSU_bp = world.get_blueprint_library().find('walker.pedestrian.0016')

# We set 3 RSU units, here are the locations
RSU_1_transform = carla.Transform(carla.Location(x=20, y=100, z=1), carla.Rotation(yaw = -45))
RSU_2_transform = carla.Transform(carla.Location(x=20, y=10, z=1), carla.Rotation(yaw = -45))
RSU_3_transform = carla.Transform(carla.Location(x=22, y=-100, z=1), carla.Rotation(yaw = 45))

for loc in [RSU_1_transform, RSU_2_transform, RSU_3_transform]:
    try:
        rsu_list.append(world.try_spawn_actor(RSU_bp, loc))
    except Exception as e:
        print(e)

# bp for RSU, one radar and one camera
rsu_camera_bp = bp_lib.find('sensor.camera.rgb')
rsu_camera_bp.set_attribute('image_size_y', '608')
camera_rsu_trans = carla.Transform(carla.Location(z=1))

rsu_Radar_bp = bp_lib.find('sensor.other.radar')
Radar_rsu_trans = carla.Transform(carla.Location(z=1), carla.Rotation(pitch=1))
rsu_Radar_bp.set_attribute('horizontal_fov', '90')
rsu_Radar_bp.set_attribute('vertical_fov', '20')
rsu_Radar_bp.set_attribute('points_per_second', '4000')
rsu_Radar_bp.set_attribute('range', '30')


# initial vehicle size 30
for i in range(30):
    try:
        # choose different color for cars
        vehicle_bp.set_attribute('color', random.choice(color))
        car_list.append(world.spawn_actor(vehicle_bp, spawn_points[i]))
    except Exception as e:
        print(e)


# set autopilot for every vehicle
for car in car_list:
    car.set_autopilot(True)

# Sensor data dict
Sensor_dict = {}
# Rsu data dict
RSU_dict = {}

# Set sensors
GNSS_bp = bp_lib.find('sensor.other.gnss')
GNSS_trans = carla.Transform(carla.Location(z=1))
GNSS_ele = world.spawn_actor(GNSS_bp, GNSS_trans, attach_to=vehicle)

# Spawn Front Camera
camera_bp = bp_lib.find('sensor.camera.rgb')
camera_bp.set_attribute('image_size_y', '608')
camera_front_trans = carla.Transform(carla.Location(z=2))
camera_front = world.spawn_actor(camera_bp, camera_front_trans, attach_to=vehicle)

# spawn Rear camera
camera_rear_trans = carla.Transform(carla.Location(x=-1 ,z=1.8), carla.Rotation(yaw = 180))
camera_rear = world.spawn_actor(camera_bp, camera_rear_trans, attach_to=vehicle)


# spawn radar
Radar_bp = bp_lib.find('sensor.other.radar')
Radar_front_trans = carla.Transform(carla.Location(z=1, x=1), carla.Rotation(pitch=1))
Radar_bp.set_attribute('horizontal_fov', '90')
Radar_bp.set_attribute('vertical_fov', '20')
Radar_bp.set_attribute('points_per_second', '4000')
Radar_bp.set_attribute('range', '30')
Radar_front = world.spawn_actor(Radar_bp, Radar_front_trans, attach_to=vehicle)

# Spawn rear radar
Radar_rear_trans = carla.Transform(carla.Location(z=1, x=-1), carla.Rotation(pitch=1, yaw = 180))
Radar_rear = world.spawn_actor(Radar_bp, Radar_rear_trans, attach_to=vehicle)

# Spawn sensors for other cars
for car in car_list:
    GNSS_spawn = world.spawn_actor(GNSS_bp, GNSS_trans, attach_to=car)
    Front_cam_spawn = world.spawn_actor(camera_bp, camera_front_trans, attach_to=car)
    Rear_cam_spawn = world.spawn_actor(camera_bp, camera_rear_trans, attach_to=car)
    Radar_front_spawn = world.spawn_actor(Radar_bp, Radar_front_trans, attach_to=car)
    Radar_rear_spawn = world.spawn_actor(Radar_bp, Radar_rear_trans, attach_to=car)

    GNSS_List[car.id] = GNSS_spawn
    Front_Camera_List[car.id] = Front_cam_spawn
    Rear_Camera_List[car.id] = Rear_cam_spawn
    Radar_front_list[car.id] = Radar_front_spawn
    Radar_rear_list[car.id] = Radar_rear_spawn

# Spawn sensors for RSUs
for rsu in rsu_list:
    rsu_gnss_spawn = world.spawn_actor(GNSS_bp, GNSS_trans, attach_to=rsu)
    rsu_cam_spawn = world.spawn_actor(rsu_camera_bp, camera_rsu_trans, attach_to=rsu)
    rsu_radar_spawn = world.spawn_actor(rsu_Radar_bp, Radar_rsu_trans, attach_to=rsu)

    GNSS_List[rsu.id] = rsu_gnss_spawn
    Front_Camera_List[rsu.id] = rsu_cam_spawn
    Radar_front_list[rsu.id] = rsu_radar_spawn


# synchoronous setting (block of setting is important)
settings = world.get_settings()
settings.synchronous_mode = True # Enables synchronous mode
settings.fixed_delta_seconds = 0.03
world.apply_settings(settings)

# listeners and queues

ego_sensor = Sensors(vehicle.id)

GNSS_ele.listen(ego_sensor.sensor_callback_GNSS)
camera_front.listen(ego_sensor.sensor_callback_Front_Camera)
camera_rear.listen(ego_sensor.sensor_callback_Rear_Camera)
Radar_front.listen(ego_sensor.sensor_callback_Front_Radar)
Radar_rear.listen(ego_sensor.sensor_callback_Rear_Radar)

# Use new class instead of Queues for better performance
for car in car_list:
    # create new object for listener
    Sensor_vehicle = Sensors(car.id)
    GNSS_List[car.id].listen(Sensor_vehicle.sensor_callback_GNSS)
    # Listener for camera
    Front_Camera_List[car.id].listen(Sensor_vehicle.sensor_callback_Front_Camera)
    Rear_Camera_List[car.id].listen(Sensor_vehicle.sensor_callback_Rear_Camera)
    # listener for radar
    Radar_front_list[car.id].listen(Sensor_vehicle.sensor_callback_Front_Radar)
    Radar_rear_list[car.id].listen(Sensor_vehicle.sensor_callback_Rear_Radar)
    Sensor_dict[car.id] = Sensor_vehicle

for rsu in rsu_list:
    Sensor_rsu = Sensors(rsu.id)
    GNSS_List[rsu.id].listen(Sensor_rsu.sensor_callback_GNSS)
    Front_Camera_List[rsu.id].listen(Sensor_rsu.sensor_callback_Front_Camera)
    Radar_front_list[rsu.id].listen(Sensor_rsu.sensor_callback_Front_Radar)
    RSU_dict[rsu.id] = Sensor_rsu


# Define before thread start
serv_capture_simu_act = 2 # To start the simulation first, we need to set number to 2 
processing_switch = False # For handling thread
p_c_switch = False # For handling loop status

Carla_socket = Carla_UDP_socket()

Node_addr_ip = '127.0.0.1'
Node_addr_port_start = 49300

# A flag to check if BSM and related messages are sent or not yet
Finish_sending_BSM = False
DSRC_range = 100
camera_range = 35

try:
    carID = ego_sensor.get_carID()
    print("Now we assign IDs to the docker nodes.")

    ID_msg = json.dumps({"type": "CARLA_ID", "ID": carID, "MEMBER": "CAV"}).encode('UTF-8')
    Carla_socket.sendto(ID_msg, (Node_addr_ip, Node_addr_port_start))
    ego_sensor.set_ipaddr((Node_addr_ip, Node_addr_port_start))

    Node_port = Node_addr_port_start

    for carID in Sensor_dict:
        # start to count the ports
        Node_port = Node_port + 1
        ID_msg = json.dumps({"type": "CARLA_ID", "ID": carID, "MEMBER": "CAV"}).encode('UTF-8')
        sent = Carla_socket.sendto(ID_msg, (Node_addr_ip, Node_port))
        Sensor_dict[carID].set_ipaddr((Node_addr_ip, Node_port))

    for rsuID in RSU_dict:
        Node_port = Node_port + 1
        ID_msg = json.dumps({"type": "CARLA_ID", "ID": rsuID, "MEMBER": "RSU"}).encode('UTF-8')
        sent = Carla_socket.sendto(ID_msg, (Node_addr_ip, Node_port))
        RSU_dict[rsuID].set_ipaddr((Node_addr_ip, Node_port))

    print("ID assignment is done!")
    print(f"{ego_sensor.get_carID()}, {ego_sensor.get_ipaddr()}")
    for id in Sensor_dict:
        print(f"{id}, {Sensor_dict[id].get_ipaddr()}")

    for id in RSU_dict:
        print(f"{id}, {RSU_dict[id].get_ipaddr()}")

except Exception as e:
    print(e)

stop_event = threading.Event()
Simu_capture_serv_thread = threading.Thread(target=thread_simu_capture_serv_switch, args=(stop_event,))
Simu_capture_serv_thread.start()

try:
    while True:
        # Simulation mode
        if serv_capture_simu_act == 0:

            world.tick()
            # Tell next status are available
            p_c_switch = True
        # Capture and processing
        elif serv_capture_simu_act == 1 and p_c_switch == True:
            processing_switch = True
            frame_list = []
            vehi_list = []
            ego_sensor.set_capture(True)
            for car in car_list:
                Sensor_dict[car.id].set_capture(True)
            for rsu in rsu_list:
                RSU_dict[rsu.id].set_capture(True)

            # Tick twice to get all data at same frame
            world.tick()
            print("-------------------Begin_processing-------------------")
            world.tick()
            ego_sensor.set_capture(False)
            for car in car_list:
                Sensor_dict[car.id].set_capture(False)
            for rsu in rsu_list:
                RSU_dict[rsu.id].set_capture(False)

            # Get sync frames
            ego_sensor.get_sync_frame()
            for car in car_list:
                Sensor_dict[car.id].get_sync_frame()
            for rsu in rsu_list:
                RSU_dict[rsu.id].get_sync_frame()
            # initialize compass orientation
            ego_sensor.compass_orientation_init(vehicle.get_transform().rotation.yaw)
            for car in car_list:
                Sensor_dict[car.id].compass_orientation_init(car.get_transform().rotation.yaw)
            for rsu in rsu_list:
                RSU_dict[rsu.id].compass_orientation_init(rsu.get_transform().rotation.yaw)

            # Store the camera hash and verification results
            # This is a dict for storing the verification results of camera images
            camera_veri_list = []
            camera_veri_output(camera_veri_list, ego_sensor, Sensor_dict, RSU_dict, camera_range)

            # Get the data format of radar
            ego_radar_result = get_radar_ego(ego_sensor)
            sensor_radar_result = get_radar_result(Sensor_dict)
            rsu_radar_result = get_rsu_radar_result(RSU_dict)

            time.sleep(2)
            print("Processing complete!")

            # Reset the BSM flag to False
            Finish_sending_BSM_related = False

            processing_switch = False
            # Stop processing once it is done, we don't want to run this block twice
            p_c_switch = False
        # Server mode, Just going to send all related data and wait
        elif serv_capture_simu_act == 2:
            if Finish_sending_BSM_related == False:
                reply_BSM = get_BSM_ego(ego_sensor, Sensor_dict, RSU_dict, DSRC_range)
                Carla_socket.sendto(reply_BSM, ego_sensor.get_ipaddr())
                for carID in Sensor_dict:
                    reply_BSM = get_BSM(carID, ego_sensor, Sensor_dict, RSU_dict, DSRC_range)
                    # Send message to well_known node for the blockchain subnet to catch processed messages
                    Carla_socket.sendto(reply_BSM, Sensor_dict[carID].get_ipaddr())
                for rsuID in RSU_dict:
                    reply_rsu_BSM = get_RSU_BSM(rsuID, ego_sensor, Sensor_dict, RSU_dict, DSRC_range)
                    Carla_socket.sendto(reply_rsu_BSM, RSU_dict[rsuID].get_ipaddr())

                print("All BSMs are sent...")
                # Now we deal with camera data, wait some time for nodes to process the BSM data
                time.sleep(1)
                reply_CAM = get_CAM_result_ego_simu(ego_sensor, Sensor_dict, camera_range)
                Carla_socket.sendto(reply_CAM, ego_sensor.get_ipaddr())
                for carID in Sensor_dict:
                    reply_CAM = get_CAM_result_simu(carID, ego_sensor, Sensor_dict, camera_range)
                    Carla_socket.sendto(reply_CAM, Sensor_dict[carID].get_ipaddr())
                for rsuID in RSU_dict:
                    reply_rsu_CAM = get_RSU_CAM_result_simu(rsuID, ego_sensor, Sensor_dict, RSU_dict, camera_range)
                    Carla_socket.sendto(reply_rsu_CAM, RSU_dict[rsuID].get_ipaddr())

                print("All Camera results are sent....")
                # We need to send the verification list to every node for their verification
                for veri in camera_veri_list:
                    Carla_socket.sendto(veri, ego_sensor.get_ipaddr())

                for carID in Sensor_dict:
                    for veri in camera_veri_list:
                        Carla_socket.sendto(veri, Sensor_dict[carID].get_ipaddr())

                for rsuID in RSU_dict:
                    for veri in camera_veri_list:
                        Carla_socket.sendto(veri, RSU_dict[rsuID].get_ipaddr())

                # Now we deal with radar data, wait some time for nodes to process the Camera data
                time.sleep(1)
                # Send the radar detection result to ego vehicle
                Carla_socket.sendto(ego_radar_result, ego_sensor.get_ipaddr())
                for carID in Sensor_dict:
                    Carla_socket.sendto(sensor_radar_result[carID], Sensor_dict[carID].get_ipaddr())

                for rsuID in RSU_dict:
                    Carla_socket.sendto(rsu_radar_result[rsuID], RSU_dict[rsuID].get_ipaddr())

                print("All Radar results are sent....\nNow wait for next round")
                Finish_sending_BSM_related = True
                
            

except KeyboardInterrupt:
    print("Stop")
    reply_stop = json.dumps({"type": "Stop"}).encode('UTF-8')
    Carla_socket.sendto(reply_stop, ego_sensor.get_ipaddr())
    # Sent stop message to nodes
    for carID in Sensor_dict:
        # Send message to well_known node for the blockchain subnet to catch processed messages
        Carla_socket.sendto(reply_stop, Sensor_dict[carID].get_ipaddr())
    for rsuID in RSU_dict:
        Carla_socket.sendto(reply_stop, RSU_dict[rsuID].get_ipaddr())
#-----------------------
stop_event.set()
Simu_capture_serv_thread.join()
# It seems socket close has sth doing with thread. better move with block
Carla_socket.close()
#----------------------
for rsu in rsu_list:
    GNSS_List[rsu.id].destroy()
    Front_Camera_List[rsu.id].destroy()
    Radar_front_list[rsu.id].destroy()
    rsu.destroy()

for car in car_list:
    GNSS_List[car.id].destroy()
    Front_Camera_List[car.id].destroy()
    Rear_Camera_List[car.id].destroy()
    Radar_front_list[car.id].destroy()
    Radar_rear_list[car.id].destroy()

for car in car_list:
    car.destroy()

vehicle.destroy()
GNSS_ele.destroy()
camera_front.destroy()
camera_rear.destroy()
Radar_front.destroy()
Radar_rear.destroy()

settings.synchronous_mode = False
world.apply_settings(settings)

print("Exit")

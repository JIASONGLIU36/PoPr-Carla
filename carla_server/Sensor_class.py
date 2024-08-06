import uuid
import time
import math

# A class for store every sensor results and provide related functions for analysis/simulation

class Sensors:

    Data_path = 'V://Test2//'

    def __init__(self, *args):
        if len(args) == 1:
            self.vehicleID = args[0]
            self.current_GNSS = None
            self.Front_cam = None
            self.Rear_cam = None
            self.Front_radar = None
            self.Rear_radar = None
        elif len(args) == 6:
            self.vehicleID = args[0]
            self.current_GNSS = args[1]
            self.Front_cam = args[2]
            self.Rear_cam = args[3]
            self.Front_radar = args[4]
            self.Rear_radar = args[5]
        else:
            raise ValueError("Invaild arguments! Check Constructor.")
        # sync lists
        self.GNSS_list = []
        self.Front_cam_list = []
        self.Rear_cam_list = []
        self.Front_radar_list = []
        self.Rear_radar_list = []

        self.capture = False

        self.timestamp = None
        self.ip_addr = None

        self.Front_fov_orientation = None
        self.Front_orientation = None
        self.Rear_fov_orientation = None
        self.Rear_orientation = None

    def set_ipaddr(self, ipaddr):
        self.ip_addr = ipaddr

    def set_capture(self, switch):
        if switch == True:
            self.capture = True
        else:
            self.capture = False

    def sensor_callback_GNSS(self, sensor_dat):
        if self.capture == True:
            self.GNSS_list.append(sensor_dat)

    def sensor_callback_Front_Camera(self, sensor_dat):
        if self.capture == True:
            self.Front_cam_list.append(sensor_dat)

    def sensor_callback_Rear_Camera(self, sensor_dat):
        if self.capture == True:
            self.Rear_cam_list.append(sensor_dat)

    def sensor_callback_Front_Radar(self, sensor_dat):
        if self.capture == True:
            self.Front_radar_list.append(sensor_dat)

    def sensor_callback_Rear_Radar(self, sensor_dat):
        if self.capture == True:
            self.Rear_radar_list.append(sensor_dat)

    def compass_orientation_init(self, yaw):
        self.Front_fov_orientation = yaw
        self.Front_orientation = (yaw + 450) % 360
        self.Rear_fov_orientation = self.Front_fov_orientation + 180
        self.Rear_orientation = (self.Rear_fov_orientation + 450) % 360

    def get_GNSS(self):
        return self.current_GNSS
    
    def get_FrontCam(self):
        return self.Front_cam
    
    def get_RearCam(self):
        return self.Rear_cam
    
    def get_FrontRadar(self):
        return self.Front_radar
    
    def get_RearRadar(self):
        return self.Rear_radar
    
    def get_carID(self):
        return self.vehicleID
    
    def get_timestamp(self):
        return self.timestamp
    
    def get_ipaddr(self):
        return self.ip_addr
    
    def get_front_orientation(self):
        return self.Front_orientation
    
    def get_rear_orientation(self):
        return self.Rear_orientation
    
    def get_sync_frame(self):
        list_index = self.get_unique_frame()
        list_index = list(list_index)
        list_index.sort()

        sync_index = -1

        for index in list_index:
            curr_index = 0
            # This is the case for RSU, no time for do a decoupling
            if not self.Rear_cam_list and not self.Rear_radar_list:
                for obj in self.GNSS_list:
                    if obj.frame == index:
                        curr_index += 1

                for obj in self.Front_cam_list:
                    if obj.frame == index:
                        curr_index += 1

                for obj in self.Front_radar_list:
                    if obj.frame == index:
                        curr_index += 1


                if curr_index == 3:
                    sync_index = index
            else:
                for obj in self.GNSS_list:
                    if obj.frame == index:
                        curr_index += 1

                for obj in self.Front_cam_list:
                    if obj.frame == index:
                        curr_index += 1

                for obj in self.Rear_cam_list:
                    if obj.frame == index:
                        curr_index += 1

                for obj in self.Front_radar_list:
                    if obj.frame == index:
                        curr_index += 1

                for obj in self.Rear_radar_list:
                    if obj.frame == index:
                        curr_index += 1

                if curr_index == 5:
                    sync_index = index

        # Reassign sync frame
        for obj in self.GNSS_list:
            if obj.frame == sync_index:
                self.current_GNSS = obj
                self.timestamp = obj.timestamp

        for obj in self.Front_cam_list:
            if obj.frame == sync_index:
                self.Front_cam = obj

        for obj in self.Rear_cam_list:
            if obj.frame == sync_index:
                self.Rear_cam = obj       

        for obj in self.Front_radar_list:
            if obj.frame == sync_index:
                self.Front_radar = obj 

        for obj in self.Rear_radar_list:
            if obj.frame == sync_index:
                self.Rear_radar = obj

        # Only use when you are ready
        '''
        curr_time = time.ctime()
        uuid_name = uuid.uuid5(uuid.NAMESPACE_DNS, str(curr_time+str(sync_index)))
        self.Front_cam.save_to_disk(self.Data_path+"Front_{}_{}.png".format(uuid_name, self.vehicleID))
        #self.Rear_cam.save_to_disk(self.Data_path+"Rear_{}_{}.png".format(uuid_name, self.vehicleID))
        '''
        
        self.GNSS_list = []
        self.Front_cam_list = []
        self.Rear_cam_list = []
        self.Front_radar_list = []
        self.Rear_radar_list = []

    def get_current_data(self):
        print(f"GNSS: {self.GNSS_list}")
        print(f"FC: {self.Front_cam_list}")
        print(f"RC: {self.Rear_cam_list}")
        print(f"FR: {self.Front_radar_list}")
        print(f"RR: {self.Rear_radar_list}")
        print(f"FO: {self.Front_orientation}")
        print(f"RO: {self.Rear_orientation}")


    def get_unique_frame(self):
        total_list = []

        for obj in self.GNSS_list:
            total_list.append(obj.frame)

        for obj in self.Front_cam_list:
            total_list.append(obj.frame)

        for obj in self.Rear_cam_list:
            total_list.append(obj.frame)

        for obj in self.Front_radar_list:
            total_list.append(obj.frame)

        for obj in self.Rear_radar_list:
            total_list.append(obj.frame)

        return set(total_list)

    # compute bearing angle
    @staticmethod
    def calculate_bearing(lat1, lon1, lat2, lon2):
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

    # Compute distance between two coordinates
    @staticmethod
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
    
    # Radar angle in degrees
    @staticmethod
    def Azimuth_for_radar(azimuth, orientation):
        return (math.degrees(azimuth) + orientation) % 360

    # Check all arzimuth from ego to nearby vehicle base on distance
    def get_arzimuth_to_ego_nearby(self, preset_dist, sensor_list):
        # Given the fact the multi-modal is designed to detect the vehicle in image and estimate the arzimuth, the calculated result is actually for camera message.
        Camera_result = []
        for sensor in sensor_list:
            actual_dist = Sensors.haversine(self.current_GNSS.latitude, self.current_GNSS.longitude, sensor_list[sensor].get_GNSS().latitude, sensor_list[sensor].get_GNSS().longitude)
            if actual_dist <= preset_dist and self.vehicleID != sensor:
                arzimuth_to_ego = Sensors.calculate_bearing(self.current_GNSS.latitude, self.current_GNSS.longitude, sensor_list[sensor].get_GNSS().latitude, sensor_list[sensor].get_GNSS().longitude)
                Camera_result.append([self.vehicleID, sensor, actual_dist, arzimuth_to_ego])
        return Camera_result
    
    # This checks the matched radar distance result and return
    def get_radar_result_nearby(self, preset_dist, sensor_list, detect_angle, detect_range):
        Radar_result = []
        for sensor in sensor_list:
            actual_dist = Sensors.haversine(self.current_GNSS.latitude, self.current_GNSS.longitude, sensor_list[sensor].get_GNSS().latitude, sensor_list[sensor].get_GNSS().longitude)
            if actual_dist <= preset_dist and self.vehicleID != sensor:
                azimuth_to_ego = Sensors.calculate_bearing(self.current_GNSS.latitude, self.current_GNSS.longitude, sensor_list[sensor].get_GNSS().latitude, sensor_list[sensor].get_GNSS().longitude)
                for target in self.Front_radar:
                    curr_azimuth = Sensors.Azimuth_for_radar(target.azimuth, self.Front_orientation)
                    if (azimuth_to_ego - detect_angle) <= curr_azimuth <= (azimuth_to_ego + detect_angle) and (actual_dist - detect_range) <= target.depth <= (actual_dist + detect_range):
                        Radar_result.append(["front", sensor, target.depth, curr_azimuth])

                for target in self.Rear_radar:
                    curr_azimuth = Sensors.Azimuth_for_radar(target.azimuth, self.Rear_orientation)
                    if (azimuth_to_ego - detect_angle) <= curr_azimuth <= (azimuth_to_ego + detect_angle) and (actual_dist - detect_range) <= target.depth <= (actual_dist + detect_range):
                        Radar_result.append(["rear", sensor, target.depth, curr_azimuth])
        return Radar_result
      


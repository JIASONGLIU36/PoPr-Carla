# PoPr-Carla
This code is just demo of the PoPr project and should only be use as education only. The code is not intended to use as a real blockchain system.

## To run the code
The environment running and testing the code are Windows 11. If you are using Linux, you may need to change setting.
Create python virtual environment, python version 3.10.14. Install carla package by `pip install carla==0.9.15` under your virtual environment.

Please download carla simulator from latest release. I used version [0.9.15](https://tiny.carla.org/carla-0-9-15-windows). Also download docker desktop for running the blockchain nodes.

Please run `CarlaUE4.exe` to start the Carla Simulator. Before running the blockchain demo, please run `UDP_change.bat` to increase UDP buffer.

In the path `/carla_nodes/`, please run `docker compose up -d --build` to build the docker image through yaml file (You may need to open the docker desktop). After containers are build, they
are going to wait requests from CarlaAPI. 

Run `/carla_server/Carla_API_t.py` by `python Carla_API_t.py`. You should be able to see the blockchain nodes are running. To collect the entire blockchain and related files, check the `Blockchain_volume` and download it through docker.

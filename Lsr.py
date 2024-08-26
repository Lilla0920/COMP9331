import sys
import socket
import time
import sys
import threading
import pickle
import copy

UPDATE_INTERVAL = 1
ROUTE_UPDATE_INTERVAL = 30
ROUTER_ID = sys.argv[1]
PORT_NUM = int(sys.argv[2])
FILENAME = sys.argv[3]

class Neighbor:
    def __init__(self, id, port, cost):
        self.id = id
        self.port = port
        self.cost = cost

class Router:
    def __init__(self, router_id, port, filename):
        self.router_id = router_id
        self.port = port
        self.neighbors_list = []
        self.local_topology = {}
        self.lost_counts = {}
        self.message_queue = []
        self.dead_neighbors = set()
        self.live_neighbors = set()
        self.prev_dead_neighbors = set()
        self.dead_router_table = {}
        self.sock = None
        self.setup_socket(port)
        self.read_file(filename)
        self.local_topology[self.router_id] = self.neighbors_list
        for neighbor in self.neighbors_list:
            self.lost_counts[neighbor.id] = 0

    # ----------------------------- initialise functions --------------------------
    # set up socket udp
    def setup_socket(self, port):
        """Setup a blocking UDP socket."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('127.0.0.1', port + 1000))
        sock.setblocking(True)  # Make the socket blocking
        self.sock = sock

    # get file information
    def read_file(self, filename):
        """Read neighbors and their info from a config file."""
        with open(filename, 'r') as file:
            num_neighbors = int(file.readline().strip())
            for _ in range(num_neighbors):
                line = file.readline().strip()
                neighbor_id, cost, neighbor_port = line.split()
                self.add_neighbor(neighbor_id, int(neighbor_port), float(cost))
                
    # manage neighbors
    def add_neighbor(self, neighbor_id, cost, neighbor_port):
        """Add a neighbor with its port and cost."""
        neighbor = Neighbor(neighbor_id, cost, neighbor_port)
        if neighbor not in self.neighbors_list:
            self.neighbors_list.append(neighbor)

    # ----------------------------functions for sending---------------------------------------------------

    def send_message(self, message, neighbor):
        # Send a message to a specific port
        self.sock.sendto(message, ('127.0.0.1', neighbor.port))

    def forward_message(self, message, router_from):
        # Forward received message to all neighbors, excluding the source.
        data = pickle.loads(message)
        source = data['source']
        exclude_set = set([router_from, source])

        for neighbor in self.neighbors_list:
            # exlucde the neighbors of router_from
            # ---- realises no repeat send
            if self.local_topology.get(router_from) and (not data.get('dead_neighbors')):
                for nb in self.local_topology[router_from]:
                    exclude_set.add(nb.id)
            # exlucde the neighbors of source
            if self.local_topology.get(source) and (not data.get('dead_neighbors')):
                for nb in self.local_topology[source]:
                    exclude_set.add(nb.id)
            if neighbor.id not in exclude_set:
                self.send_message(message, neighbor)

    def broadcast_self_neighbors(self):
        # Send full neighbor information to all neighbors
        data = {
            'source': self.router_id,
            'neighbors': self.neighbors_list
        }
        message = pickle.dumps(data)
        for neighbor in self.neighbors_list:
            print('sending iiiiiiinitial message from', self.router_id, ' to', neighbor.id)
            self.send_message(message, neighbor)

    def broadcast_heartbeat(self):
        # Send heartbeat message to all neighbors
        data = {
            'source': self.router_id,
            'heartbeat': True
        }
        message = pickle.dumps(data)
        for neighbor in self.neighbors_list:
            self.send_message(message, neighbor)

    def broadcast_dead_neighbors(self, timestamp):
        data = {
            'source': self.router_id,
            'dead_neighbors': self.dead_neighbors,
            'live_neighbors': self.live_neighbors,
            'timestamp': timestamp
        }
        message = pickle.dumps(data)
        for neighbor in self.neighbors_list:
            self.send_message(message, neighbor)


    def calculate_dead_gloabal_routers(self):
        global_routers_status = {}
        # different sources can have differect dead or live status
        # in this case we need to compare timestamp, such that for a same router,
        # the dead or not status depends on the source that update it latest
        # Assumption: timestamps of all routers are almost synchronized
        for source, (timestamp, dead_neighbors, live_neighbors) in self.dead_router_table.items():
            for dead_router in dead_neighbors:
                if not global_routers_status.get(dead_router):
                    global_routers_status[dead_router] = (False, timestamp)
                elif timestamp > global_routers_status[dead_router][1]:
                    global_routers_status[dead_router] = (False, timestamp)
            for live_router in live_neighbors:
                if not global_routers_status.get(live_router):
                    global_routers_status[live_router] = (True, timestamp)
                elif timestamp > global_routers_status[live_router][1]:
                    global_routers_status[live_router] = (True, timestamp)
        for dead_router in self.dead_neighbors:
            global_routers_status[dead_router] = (False, -1)
        for live_router in self.live_neighbors:
            global_routers_status[live_router] = (True, -1)
        global_dead_routers = set([router for router, (status, timestamp) in global_routers_status.items() if not status])
        return global_dead_routers


    def run(self):
        # Main loop for sending, receiving, and forwarding messages
        timecount = 0
        while True:
            #The message coming from every ruter own  config is sent to the neighbor only three times (to prevent packet loss and repeated sending).
            if timecount <= 3:
                self.broadcast_self_neighbors()
             # forward message to to all neighbors except the router_from list
            for router_from, message in self.message_queue:
                self.forward_message(message, router_from)
            self.message_queue.clear()
             # broadcast "heatbeat" every min but does not forward it, which is just for dead router detection.
            self.broadcast_heartbeat()

            if self.dead_neighbors != self.prev_dead_neighbors:
                self.live_neighbors = [neighbor.id for neighbor in self.neighbors_list if neighbor.id not in self.dead_neighbors]
                self.broadcast_dead_neighbors(timecount)
            self.prev_dead_neighbors = copy.deepcopy(self.dead_neighbors)
            for neighbor in self.neighbors_list:
                self.lost_counts[neighbor.id] += 1
                if self.lost_counts[neighbor.id] > 3:
                    # mark the router as dead
                    self.dead_neighbors.add(neighbor.id)
                else:
                    # mark the router as live
                    self.dead_neighbors.discard(neighbor.id)

            dead_global_routers = self.calculate_dead_gloabal_routers()
            print(f"Router {self.router_id}, timecount {timecount}, total_topology len {len(self.local_topology)}, dead_global_routers: {dead_global_routers}")

            timecount += 1
            time.sleep(UPDATE_INTERVAL)

# ------------------------------- functions for receiving -------------------------------------------
def receive_messages(master: Router):
    recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_socket.bind(('127.0.0.1', PORT_NUM))
    while True:
        """Receive messages, update local topology, and forward them."""
        message, addr_and_port = recv_socket.recvfrom(4096)
        port_from = addr_and_port[1]
        router_from = ''
        for neighbor in master.neighbors_list:
            if port_from == neighbor.port + 1000:
                router_from = neighbor.id
                break
        data = pickle.loads(message)
        source = data['source']
        # update the lost count of the received message's source
        master.lost_counts[router_from] = 0
        if data.get('heartbeat'):
            continue
        if data.get('dead_neighbors') and not source == master.router_id:
            if not master.dead_router_table.get(source):
                master.dead_router_table[source] = (data['timestamp'], data['dead_neighbors'], data['live_neighbors'])
            elif data['timestamp'] > master.dead_router_table[source][0]:
                master.dead_router_table[source] = (data['timestamp'], data['dead_neighbors'], data['live_neighbors'])


        # Update local topology with received neighbor information
        if data.get('neighbors') and (not master.local_topology.get(source)):
            master.local_topology[source] = data['neighbors']
        # Forward the message to all neighbors except the source
        master.message_queue.append((router_from, message))
        # self.forward_message(message, source)

# ------------------------------- functions for calculation -------------------------------------------
def dijkstra(master: Router):
    start = master.router_id
    while True:
        time.sleep(ROUTE_UPDATE_INTERVAL)
        dead_routers = master.calculate_dead_gloabal_routers()
        print(f"Router {master.router_id}, dead_routers: {dead_routers}")
        graph = master.local_topology

        for dead_router in dead_routers:
            if dead_router in graph.keys():
                del graph[dead_router]

        for router_id in list(graph.keys()):
            graph[router_id] = [neighbor for neighbor in graph[router_id] if neighbor.id not in dead_routers]
        distances = {node: float('infinity') for node in graph}
        distances[start] = 0
        paths = {node: "" for node in graph}
        paths[start] = start

        unvisited = set(graph.keys())

        while unvisited:
            min_node = None
            for node in unvisited:
                if min_node is None or distances[node] < distances[min_node]:
                    min_node = node

            if min_node is None:
                break

            for neighbor in graph[min_node]:
                new_dist = distances[min_node] + neighbor.cost
                if new_dist < distances[neighbor.id]:
                    distances[neighbor.id] = new_dist
                    paths[neighbor.id] = paths[min_node] + neighbor.id

            unvisited.remove(min_node)

        print(f"I am Router {master.router_id}")
        for dest, path in paths.items():
            if dest != start:
                print(f"Least cost path to router {dest}:{path} and the cost: {distances[dest]:.1f}")

print(' ROUTER_ID: ', ROUTER_ID, ' PORT_NUM: ', PORT_NUM, ' FILENAME: ', FILENAME)
master_router = Router(ROUTER_ID, PORT_NUM, FILENAME)
receiver_thread = threading.Thread(target=receive_messages, args=(master_router,))
dijkstra_thread = threading.Thread(target=dijkstra, args=(master_router,))
receiver_thread.start()
dijkstra_thread.start()
master_router.run()


import pickle
import time
from json import JSONDecoder, JSONEncoder
import socket

from config_generator import Node, Address

class BinJSONEncoder(JSONEncoder):
    def encode(self, obj):
        obj_string = super().encode(obj)
        return obj_string.encode('utf-8')

with open('nodes.pickle', 'rb') as f:
    nodes = pickle.load(f)


decode = JSONDecoder()

def _parse_json(raw_data):
    try:
        py_json, pos = decode.raw_decode(raw_data.decode('utf-8'))
    except ValueError:
        return None, raw_data
    return py_json, raw_data[pos:]

class P2PhunRPC:
    encode = BinJSONEncoder().encode

    def __init__(self, address, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((address, port))
        self.s = s
        self.buf = b''

    def send(self, obj):
        self.s.send(self.encode(obj))

    def get_result(self):
        while True:
            self.buf += self.s.recv(1024)
            time.sleep(1)
            py_json, data_rest = _parse_json(self.buf)
            if py_json is not None:
                self.buf = data_rest
                return py_json

    def apply(self, mod, fun, args=[]): # In this very special case [] as default is okay ;)
        # a result is expected
        self.send({'mod':mod, 'fun':fun, 'args':args})
        return self.get_result()

    def create_node(self, id, port, routingtable_cfg):
        args = [{'id':id, 'port':port, 'routingtable_cfg':routingtable_cfg}]
        return self.apply('p2phun_node_sup', 'create_node', args)

    def fetch_routing_table(self, from_id):
        return self.apply('p2phun_peertable_operations', 'fetch_all', [from_id])

    def find_node(self, my_id, id2find):
        return self.apply('p2phun_swarm', 'find_node', [my_id, id2find])

    def shutdown(self):
        self.s.close()

class RoutingTableConfig:
    def __init__(self, bigbin_percent=25, nsmallbins=3, nbits=48,  bigbin_maxnodes=8, smallbin_maxnodes=3):
        self.bigbin_percent = bigbin_percent
        self.nsmallbins = nsmallbins
        self.nbits = nbits
        self.bigbin_maxnodes = bigbin_maxnodes
        self.smallbin_maxnodes = smallbin_maxnodes

    def as_dict(self):
        space_size = 2 ** self.nbits
        return {
            'space_size':space_size,
            'bigbin_spacesize':round((self.bigbin_percent/100) * space_size),
            'number_of_smallbins':self.nsmallbins,
            'smallbin_nodesize':self.smallbin_maxnodes,
            'bigbin_nodesize':self.bigbin_maxnodes}

if __name__ == '__main__':
    ADDRESS = "10.0.2.6"
    PORT = 4999
    rpc = P2PhunRPC(ADDRESS, PORT)
    swarm_ids = [node.node_num for node in nodes]
    rt = RoutingTableConfig()
    node = nodes[0]
    node_pid = rpc.create_node(node.node_num, node.address.port, rt.as_dict())
    print('Woooot:', node_pid)
    #create_node(self, id, port, routingtable_cfg):

    #result = [rpc.fetch_routing_table(peer_id) for peer_id in swarm_ids]
    #print(result)
    #node_id = nodes[0].myid_b64
    #id2find = nodes[1].myid_b64
    #rpc.find_node(node_id, id2find)

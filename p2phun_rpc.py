import time
from json import JSONDecoder, JSONEncoder
import socket
import hashlib
import base64

KEY_SIZE_BYTES = 6
KEY_SIZE_BITS = KEY_SIZE_BYTES * 8
KEY_SIZE_INT = 2 ** (KEY_SIZE_BYTES * 8) # int value of largest key as well
HOST = '127.0.0.1'

def hash_of_id(n):
    return hashlib.sha256(str(n).encode('ascii')).digest()[:KEY_SIZE_BYTES]

def to_base64(b):
    return base64.b64encode(b).decode('utf-8')


class BinJSONEncoder(JSONEncoder):
    def encode(self, obj):
        obj_string = super().encode(obj)
        return obj_string.encode('utf-8')

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

    def create_connection(self, id_int, host, port):
        args = {'nodeid':id_int, 'host':host, 'port':port}
        return self.apply('p2phun_peer_pool', 'connect', args)

    def create_node(self, id, port, routingtable_cfg, managed=True):
        if managed:
            managed = []
        else:
            managed = ['no_manager']
        args = [{'id':id, 'port':port, 'routingtable_cfg':routingtable_cfg, 'opts':managed}]
        return self.apply('p2phun_sup', 'create_node', args)

    def fetch_routing_table(self, from_id):
        return self.apply('p2phun_peertable_operations', 'fetch_all', [from_id])

    def find_node(self, my_id, id2find):
        return self.apply('p2phun_swarm', 'find_node', [my_id, id2find])

    def shutdown(self):
        self.s.close()

class RoutingTableConfig:
    def __init__(self, bigbin_percent=25, nsmallbins=3,  bigbin_maxnodes=8, smallbin_maxnodes=3):
        self.bigbin_percent = bigbin_percent
        self.nsmallbins = nsmallbins
        self.bigbin_maxnodes = bigbin_maxnodes
        self.smallbin_maxnodes = smallbin_maxnodes

    def as_dict(self):
        return {
            'space_size':KEY_SIZE_INT,
            'bigbin_spacesize':round((self.bigbin_percent/100) * KEY_SIZE_INT),
            'number_of_smallbins':self.nsmallbins,
            'smallbin_nodesize':self.smallbin_maxnodes,
            'bigbin_nodesize':self.bigbin_maxnodes}

class Node:
    def __init__(self, id_int, host, port, rt_cfg):
        self.host = host
        self.port = port
        self.id_int = id_int
        self.rt_cfg = rt_cfg
        self.peer_connections = []

    @property
    def id_hashed(self):
        return hash_of_id(self.id_int) # When we start using pub. keys this will be relevant

    @property
    def id_b64(self):
        return to_base64(self.id_int.to_bytes(KEY_SIZE_BYTES, 'big'))

def iter_nodes(number_of_nodes, rt_cfg):
    node_ids = range(number_of_nodes)
    for n in node_ids:
        yield Node(id_int=n, host=HOST, port=5000 + n, rt_cfg=rt_cfg)

if __name__ == '__main__':
    API_PORT = 4999
    number_of_nodes = 250
    rpc = P2PhunRPC(HOST, API_PORT)
    rt = RoutingTableConfig()
    nodes = list(iter_nodes(number_of_nodes, rt))
    node_pid = rpc.create_node(nodes[0].id_int, nodes[0].port, rt.as_dict())
    for node1, node2 in zip(nodes[:-1], nodes[1:]):
        print('Creating node:', node2.id_int)
        node_pid = rpc.create_node(node2.id_int, node2.port, rt.as_dict())
        result = rpc.create_connection(node1.id_int, node2.host, node2.port)
    #print(result)

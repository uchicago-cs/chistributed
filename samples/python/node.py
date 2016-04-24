import json
import sys
import signal
import zmq
import time

import colorama
colorama.init()

from zmq.eventloop import ioloop, zmqstream
ioloop.install()




class Node:
    def __init__(self, node_name, pub_endpoint, router_endpoint, peer_names):
        self.loop = ioloop.ZMQIOLoop.instance()
        self.context = zmq.Context()

        self.connected = False

        # SUB socket for receiving messages from the broker
        self.sub_sock = self.context.socket(zmq.SUB)
        self.sub_sock.connect(pub_endpoint)
        # make sure we get messages meant for us!
        self.sub_sock.set(zmq.SUBSCRIBE, node_name)
        self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
        self.sub.on_recv(self.handle)

        # REQ socket for sending messages to the broker
        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(router_endpoint)
        self.req_sock.setsockopt(zmq.IDENTITY, node_name)
        self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
        self.req.on_recv(self.handle_broker_message)

        self.name = node_name
        self.peer_names = peer_names

        self.debug = True

        self.store = {}

        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
            signal.signal(sig, self.shutdown)

    def log(self, msg):
        log_msg = ">>> %10s -- %s" % (self.name, msg)
        print colorama.Style.BRIGHT + log_msg + colorama.Style.RESET_ALL

    def log_debug(self, msg):
        if self.debug:
            log_msg = ">>> %10s -- %s" % (self.name, msg)
            print colorama.Fore.BLUE + log_msg + colorama.Style.RESET_ALL


    def start(self):
        '''
        Simple manual poller, dispatching received messages and sending those in
        the message queue whenever possible.
        '''
        self.loop.start()

    def handle_broker_message(self, msg_frames):
        '''
        Nothing important to do here yet.
        '''
        # TODO: Check whether there is an error
        pass

    def send_to_broker(self, d):
        self.req.send_json(d)
        self.log_debug("Sent: %s" % d)

    def handle(self, msg_frames):
        assert len(msg_frames) == 3
        assert msg_frames[0] == self.name
        # Second field is the empty delimiter
        msg = json.loads(msg_frames[2])
        self.log_debug("Received " + str(msg_frames))
        if msg['type'] == 'get':
            # TODO: handle errors, esp. KeyError
            k = msg['key']
            v = self.store[k]
            self.req.send_json({'type': 'log', 'debug': {'event': 'getting', 'node': self.name, 'key': k, 'value': v}})
            self.req.send_json({'type': 'getResponse', 'id': msg['id'], 'value': v})
        elif msg['type'] == 'set':
            k = msg['key']
            v = msg['value']
            #self.req.send_json({'type': 'log', 'debug': {'event': 'setting', 'node': self.name, 'key': k, 'value': v}})
            self.store[k] = v
            self.send_to_broker({'type': 'setResponse', 'id': msg['id']})
            
             
        elif msg['type'] == 'hello':
            # should be the very first message we see
            if not self.connected:
                self.connected = True
                self.send_to_broker({'type': 'helloResponse', 'source': self.name})
                self.log("Node is running")
        else:
            self.req.send_json({'type': 'log', 'debug': {'event': 'unknown', 'node': self.name}})


    def shutdown(self, sig, frame):
        self.loop.stop()
        self.sub_sock.close()
        self.req_sock.close()
        sys.exit(0)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--pub-endpoint',
            dest='pub_endpoint', type=str,
            default='tcp://127.0.0.1:23310')
    parser.add_argument('--router-endpoint',
            dest='router_endpoint', type=str,
            default='tcp://127.0.0.1:23311')
    parser.add_argument('--node-name',
            dest='node_name', type=str,
            default='test_node')
    parser.add_argument('--peer-names',
            dest='peer_names', type=str,
            default='')
    args = parser.parse_args()
    args.peer_names = args.peer_names.split(',')

    Node(args.node_name, args.pub_endpoint, args.router_endpoint, args.peer_names).start()

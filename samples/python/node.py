import json
import sys
import signal
import zmq
import time
import click
import time

import colorama
colorama.init()

from zmq.eventloop import ioloop, zmqstream
ioloop.install()




class Node:
    def __init__(self, node_name, pub_endpoint, router_endpoint, peer_names, debug):
        self.loop = ioloop.IOLoop.instance()
        self.context = zmq.Context()

        self.connected = False

        # SUB socket for receiving messages from the broker
        self.sub_sock = self.context.socket(zmq.SUB)
        self.sub_sock.connect(pub_endpoint)
        # make sure we get messages meant for us!
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, node_name)
        self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
        self.sub.on_recv(self.handle)

        # REQ socket for sending messages to the broker
        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(router_endpoint)
        self.req_sock.setsockopt_string(zmq.IDENTITY, node_name)
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
            k = msg['key']
            time.sleep(0.1)
            if k in self.store:            
                v = self.store[k]
                self.send_to_broker({'type': 'getResponse', 'id': msg['id'], 'key': k, 'value': v})
            else:
                self.send_to_broker({'type': 'getResponse', 'id': msg['id'], 'error': "No such key: %s" % k})
                
        elif msg['type'] == 'set':
            k = msg['key']
            v = msg['value']

            self.store[k] = v
            
            for p in self.peer_names:
                self.send_to_broker({'type': 'dupl', 'destination': p, 'key': k, 'value': v})
            
            self.send_to_broker({'type': 'setResponse', 'id': msg['id'], 'key': k, 'value': v})
        elif msg['type'] == 'dupl':
            k = msg['key']
            v = msg['value']

            self.store[k] = v           
        elif msg['type'] == 'hello':
            # should be the very first message we see
            if not self.connected:
                self.connected = True
                self.send_to_broker({'type': 'helloResponse', 'source': self.name})
                self.log("Node is running")
        else:
            pass
            #self.req.send_json({'type': 'log', 'debug': {'event': 'unknown', 'node': self.name}})


    def shutdown(self, sig, frame):
        self.loop.stop()
        self.sub_sock.close()
        self.req_sock.close()
        sys.exit(0)


@click.command()
@click.option('--pub-endpoint', type=str, default='tcp://127.0.0.1:23310')
@click.option('--router-endpoint', type=str, default='tcp://127.0.0.1:23311')
@click.option('--node-name', type=str)
@click.option('--peer', multiple=True)
@click.option('--debug', is_flag=True)
def run(pub_endpoint, router_endpoint, node_name, peer, debug):

    n = Node(node_name, pub_endpoint, router_endpoint, peer, debug)
    
    n.start()

if __name__ == '__main__':
    run()

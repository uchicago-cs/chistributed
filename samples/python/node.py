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

# Represent a node in our data store
class Node(object):
    def __init__(self, node_name, pub_endpoint, router_endpoint, peer_names, debug):
        self.loop = ioloop.IOLoop.instance()
        self.context = zmq.Context()

        self.connected = False

        # SUB socket for receiving messages from the broker
        self.sub_sock = self.context.socket(zmq.SUB)
        self.sub_sock.connect(pub_endpoint)
        
        # Make sure we get messages meant for us!
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, node_name)

        # Create handler for SUB socket
        self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
        self.sub.on_recv(self.handle)

        # REQ socket for sending messages to the broker
        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(router_endpoint)
        self.req_sock.setsockopt_string(zmq.IDENTITY, node_name)

        # We don't strictly need a message handler for the REQ socket,
        # but we define one in case we ever receive any errors through it.
        self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
        self.req.on_recv(self.handle_broker_message)

        self.name = node_name
        self.peer_names = peer_names

        # For now, hardcode debug to true, since the broker currently
        # doesn't send a --debug option.
        self.debug = True

        # This node's data store
        self.store = {}

        # Capture signals to ensure an orderly shutdown
        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
            signal.signal(sig, self.shutdown)


    # Logging functions
    def log(self, msg):
        log_msg = ">>> %10s -- %s" % (self.name, msg)
        print colorama.Style.BRIGHT + log_msg + colorama.Style.RESET_ALL

    def log_debug(self, msg):
        if self.debug:
            log_msg = ">>> %10s -- %s" % (self.name, msg)
            print colorama.Fore.BLUE + log_msg + colorama.Style.RESET_ALL


    # Starts the ZeroMQ loop
    def start(self):
        self.loop.start()


    # Handle replies received through the REQ socket. Typically,
    # this will just be an acknowledgement of the message sent to the
    # broker through the REQ socket, but can also be an error message.
    def handle_broker_message(self, msg_frames):
        # TODO: Check whether there is an error
        pass


    # Sends a message to the broker
    def send_to_broker(self, d):
        self.req.send_json(d)
        self.log_debug("Sent: %s" % d)


    # Handles messages received from the broker (which can originate in
    # other nodes or in the broker itself)
    def handle(self, msg_frames):

        # Unpack the message frames.
        # in the event of a mismatch, format a nice string with msg_frames in
        # the raw, for debug purposes
        assert len(msg_frames) == 3, ((
            "Multipart ZMQ message had wrong length. "
            "Full message contents:\n{}").format(msg_frames))
        assert msg_frames[0] == self.name
        # Second field is the empty delimiter
        msg = json.loads(msg_frames[2])

        self.log_debug("Received " + str(msg_frames))

        # GET handler
        if msg['type'] == 'get':
            k = msg['key']

            # Simulate that retrieving data takes time
            time.sleep(0.1)
            
            # Get the value and send a response or, if there
            # is no such key, send an error.
            if k in self.store:            
                v = self.store[k]
                self.send_to_broker({'type': 'getResponse', 'id': msg['id'], 'key': k, 'value': v})
            else:
                self.send_to_broker({'type': 'getResponse', 'id': msg['id'], 'error': "No such key: %s" % k})
                
        # SET handler
        elif msg['type'] == 'set':
            k = msg['key']
            v = msg['value']

            # Simulate that storing data takes time
            time.sleep(0.1)

            # Store the value
            self.store[k] = v
            
            # And send a 'dupl' message to the peers with a replica of the value.
            for p in self.peer_names:
                self.send_to_broker({'type': 'dupl', 'destination': p, 'key': k, 'value': v})
            
            # Finally, send response to broker
            self.send_to_broker({'type': 'setResponse', 'id': msg['id'], 'key': k, 'value': v})

        # DUPL handler
        elif msg['type'] == 'dupl':
            k = msg['key']
            v = msg['value']

            # Simulate that storing data takes time
            time.sleep(0.1)

            # Set the value in our data store
            self.store[k] = v           

        # HELLO handler
        elif msg['type'] == 'hello':
            # Only handle this message if we are not yet connected to the broker.
            # Otherwise, we should ignore any subsequent hello messages.
            if not self.connected:
                # Send helloResponse
                self.connected = True
                self.send_to_broker({'type': 'helloResponse', 'source': self.name})
                self.log("Node is running")
        else:
            self.log("Received unknown message type: %s" % msg['type'])


    # Performs an orderly shutdown
    def shutdown(self, sig, frame):
        self.loop.stop()
        self.sub_sock.close()
        self.req_sock.close()
        sys.exit(0)


# Command-line parameters
@click.command()
@click.option('--pub-endpoint', type=str, default='tcp://127.0.0.1:23310')
@click.option('--router-endpoint', type=str, default='tcp://127.0.0.1:23311')
@click.option('--node-name', type=str)
@click.option('--peer', multiple=True)
@click.option('--debug', is_flag=True)
def run(pub_endpoint, router_endpoint, node_name, peer, debug):

    # Create a node and run it
    n = Node(node_name, pub_endpoint, router_endpoint, peer, debug)
    
    n.start()

if __name__ == '__main__':
    run()

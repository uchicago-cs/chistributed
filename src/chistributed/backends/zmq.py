# Needed so we can import from the global "zmq" package
from __future__ import absolute_import

import json
import subprocess
import zmq
from zmq.eventloop import ioloop, zmqstream
import os
from chistributed.common import ChistributedException
ioloop.install()

from chistributed.core.model import SetRequestMessage, Node, CustomMessage, GetRequestMessage, Message
import chistributed.common.log as log




class ZMQMessage(dict):
    
    def __init__(self, identity, fields, raw_msg = None):
        self.identity = identity
        self.fields = fields
        self.raw_msg = raw_msg
        
        super(ZMQMessage, self).__init__(fields)
            
    def to_frames(self):
        frames = []
        frames.append(bytes(self.identity))
        frames.append("")
        frames.append(json.dumps(self))
        return frames
            
    def to_msg(self):
        return Message.from_dict(self.fields)            
            
    @classmethod
    def from_msg(cls, msg):
        if isinstance(msg, GetRequestMessage):
            fields = {"type": "get",
                      "id": msg.id,
                      "key": msg.key}
            return cls(msg.destination, fields)
        elif isinstance(msg, SetRequestMessage):
            fields = {"type": "set",
                      "id": msg.id,
                      "key": msg.key,
                      "value": msg.value}
            return cls(msg.destination, fields)
        elif isinstance(msg, CustomMessage):
            fields = {"type": msg.msg_type,
                      "destination": msg.destination}
            fields.update(msg.values)
            return cls(msg.destination, fields)      
    
    @classmethod
    def from_zmq_frames(cls, zmq_message_frames):
        assert isinstance(zmq_message_frames, list)
        assert len(zmq_message_frames) == 3
        
        fields = json.loads(zmq_message_frames[2])
        
        return cls(zmq_message_frames[0], fields, zmq_message_frames[2])
        

class ZMQBackend:
    def __init__(self, node_executable, pub_endpoint, router_endpoint, debug=False):
        self.loop = ioloop.IOLoop.instance()
        self.context = zmq.Context()
        
        self.node_executable = node_executable.split()

        # PUB socket for sending messages to nodes
        self.pub_endpoint = pub_endpoint
        self.pub_sock = self.context.socket(zmq.PUB)
        self.pub_sock.bind(pub_endpoint)
        # ZMQStream used for non-blocking operations via IOLoop
        self.pub = zmqstream.ZMQStream(self.pub_sock, self.loop)

        # ROUTER socket for receiving messages from nodes
        self.router_endpoint = router_endpoint
        self.router_sock = self.context.socket(zmq.ROUTER)
        self.router_sock.bind(router_endpoint)
        self.router = zmqstream.ZMQStream(self.router_sock, self.loop)
        self.router.on_recv(self.receive_message)

        # Map from node name to ZID
        self.node_zid = {}
        
        # And from ZID to node name
        self.zid_node = {}
        
        # Maps node names to OS PIDs for stopping
        self.node_pids = {}

        self.debug = debug
        self.devnull = None

        self.ds = None

        self.running = True

    def set_ds(self, ds):
        self.ds = ds

    def start(self):
        '''
        Start the IOLoop. Will allow messages to be send and received
        asynchronously on the pub and router ZMQStreams.
        '''
        log.info('Starting broker')
        self.loop.start()
        
    def stop(self):
        log.info('Stopping broker')
        self.running = False

        self.loop.stop()
        
        node_ids = self.node_pids.keys()
        
        for node_id in node_ids:
            self.stop_node(node_id)    
            
            
    def start_node(self, node_id, extra_params=[]):
        '''
        Start a node with given name and parameters.

        Note all output will be sent to /dev/null (or the platform's equivalent).

        Highly unsafe of course.
        '''
        args = self.node_executable[:]
        
        args += ['--node-name', node_id,
                 '--pub-endpoint', self.pub_endpoint,
                 '--router-endpoint', self.router_endpoint]
        
        args += extra_params
        
        if self.debug:
            stdout = None
            stderr = None
        else:
            if self.devnull is None:
                self.devnull = open(os.devnull, "w")
            stdout = self.devnull
            stderr = self.devnull
            
        try:
            proc = subprocess.Popen(args, stdout = stdout, stderr = stderr)
            self.node_pids[node_id] = proc
        except OSError, ose:
            raise ChistributedException("Could not start node process. Tried to run '%s'" % " ".join(args), original_exception = ose)

        # Send hello
        self.loop.add_callback(self.__hello_callback(node_id))
        
        
    def stop_node(self, node_id):
        '''
        Sends SIGTERM to the named node.

        Node implementations should catch it and shutdown because killing procs is
        risky business.
        '''
        log.info("Stopping node " + node_id)
        self.node_pids[node_id].terminate()
        del self.node_pids[node_id]
        pass        
                    

    def receive_message(self, msg_frames):
        '''
        Assembles message from raw frames and dispatch it.

        msgs should be a list of two bytes values, the first being the ZMQ ID for
        the sender, and the second being the JSON message from the nodes.
        '''
        zmq_msg = ZMQMessage.from_zmq_frames(msg_frames)

        if zmq_msg["type"] == "helloResponse":
            log.debug("RECV %s: %s" % (zmq_msg['source'], msg_frames[2]))
        else:
            log.debug("RECV %s: %s" % (self.zid_node.get(msg_frames[0], "unknown node"), msg_frames[2]))
 
        if zmq_msg["type"] == "helloResponse":        
            node_name = zmq_msg['source']
            if node_name in self.node_zid:
                err = "Duplicate hello from " + node_name
                log.debug(err)
    
            self.node_zid[node_name] = zmq_msg.identity
            self.zid_node[zmq_msg.identity] = node_name
        
            log.info(node_name + " connected")
            
            self.ds.nodes[node_name].set_state(Node.STATE_RUNNING)

            self.router.send_multipart([zmq_msg.identity,
                                        "",
                                        json.dumps({"type": "ack", "original": zmq_msg.fields})])

        elif zmq_msg["type"] == "log":
            pass
        else:
            self.router.send_multipart([zmq_msg.identity,
                                        "",
                                        json.dumps({"type": "ack", "original": zmq_msg.fields})])            
            
            msg = zmq_msg.to_msg()
            if msg is not None:
                self.ds.process_message(msg, source = self.zid_node.get(msg_frames[0]))
                
    def send_message(self, node_id, msg):
        zmq_msg = ZMQMessage.from_msg(msg)
        
        self.__send_zmq_message(zmq_msg)


    def __send_zmq_message(self, zmq_msg):        
        frames = zmq_msg.to_frames()
        
        log.debug("SEND %s: %s" % (frames[0], frames[2]))

        self.pub.send_multipart(frames)
        
        # This shouldn't be necessary but, for some reason, pyzmq stops
        # sending messages on the PUB socket after about a second or so.
        # IOLoop is somehow not receiving the WRITE events correctly.
        # Need to revisit this at some point, but this at least ensures
        # that messages get sent.
        self.pub.flush()

    def __hello_callback(self, node_id):
        zmq_msg = ZMQMessage(node_id, {'type': 'hello', 'destination': [node_id]})
        
        def callback():
            def hello_sender(tries_left):
                if node_id not in self.node_zid:
                    self.__send_zmq_message(zmq_msg)
                    tries_left -= 1
                    if tries_left > 0:
                        self.loop.add_timeout(self.loop.time() + 1, hello_sender, tries_left = tries_left)
            
                if tries_left == 0:
                    log.warning("Node %s did not respond to hello message" % node_id)
                    self.ds.nodes[node_id].set_state(Node.STATE_FAILED)
                    
            
            self.loop.add_timeout(self.loop.time() + 0.5, hello_sender, tries_left = 5)
            
        return callback
        


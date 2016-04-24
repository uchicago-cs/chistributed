# Needed so we can import from the global "zmq" package
from __future__ import absolute_import

import logging
import pprint
import json
import subprocess
import zmq
from zmq.eventloop import ioloop, zmqstream
from chistributed.core.model import SetRequestMessage, Node
ioloop.install()

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
            
    @classmethod
    def from_msg(cls, msg):
        if isinstance(msg, SetRequestMessage):
            fields = {"type": "set",
                      "id": msg.id,
                      "key": msg.key,
                      "value": msg.value}
            return cls(msg.destination, fields)
            
    
    @classmethod
    def from_zmq_frames(cls, zmq_message_frames):
        assert isinstance(zmq_message_frames, list)
        assert len(zmq_message_frames) == 3
        
        fields = json.loads(zmq_message_frames[2])
        
        return cls(zmq_message_frames[0], fields, zmq_message_frames[2])
        

class ZMQBackend:
    def __init__(self, node_executable, pub_endpoint, router_endpoint):
        self.loop = ioloop.ZMQIOLoop.instance()
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

        # "Routing" table; maps node names to ZMQ IDs.
        # (This direction should be more common than the reverse.)
        self.node_zids = dict()
        # Maps node names to OS PIDs for stopping
        self.node_pids = dict()

        self.ds = None

        # logging configuration
        # TODO: should be customizable by command line args
        logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
        self.logger = logging.getLogger('broker')

    def set_ds(self, ds):
        self.ds = ds

    def start(self):
        '''
        Start the IOLoop. Will allow messages to be send and received
        asynchronously on the pub and router ZMQStreams.
        '''
        self.log('Starting broker')
        self.loop.start()
        
    def stop(self):
        self.log('Stopping broker')
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

        # TODO: Make output redirection configurable
        #if not hasattr(self, "devnull"):
        #    self.devnull = open(os.devnull, "w")
        #proc = subprocess.Popen(args, shell=True, stdout=self.devnull, stderr=self.devnull)
        proc = subprocess.Popen(args)
        self.node_pids[node_id] = proc

        # Send hello
                    
        self.loop.add_callback(self.__hello_callback(node_id))

        
    def stop_node(self, node_id):
        '''
        Sends SIGTERM to the named node.

        Node implementations should catch it and shutdown because killing procs is
        risky business.
        '''
        self.log("stopping " + node_id)
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

        self.log("Received frames: %s" % (msg_frames,))

        if zmq_msg["type"] == "helloResponse":        
            node_name = zmq_msg['source']
            if node_name in self.node_zids:
                err = "Duplicate hello from " + node_name
                self.log(err)
    
            self.node_zids[node_name] = zmq_msg.identity
        
            self.log(node_name + " connected")
            
            self.ds.nodes[node_name].set_state(Node.STATE_RUNNING)

            self.router.send_multipart([zmq_msg.identity,
                                        "",
                                        json.dumps({"type": "ack", "original": zmq_msg.fields})])

        elif zmq_msg["type"] == "log":
            node_name = self.nodes_by_sender().get(zmq_msg.identity, 'unknown node')
            self.log('log message from {node}:\n{message}'.format(node=node_name, message=pprint.pformat(message)))
        else:
            # TODO: escalate to model
            pass
                
    def send_message(self, node_id, msg):
        zmq_msg = ZMQMessage.from_msg(msg)
        
        self.__send_zmq_message(zmq_msg)


    def __send_zmq_message(self, zmq_msg):        
        frames = zmq_msg.to_frames()
        
        self.log("Sending: %s" % (frames,))

        self.pub.send_multipart(frames)


    # =======================
    # Misc. utility functions
    # =======================

    def nodes_by_sender(self):
        '''
        Used to look up node names from the unique sender ID.
        '''
        return {v:k for k, v in self.node_zids.items()}


    def log(self, log_msg):
        '''
        Log the given line at INFO level.

        TODO: meaningful use of different levels.
        '''
        self.logger.info(log_msg)



    def __hello_callback(self, node_id):
        zmq_msg = ZMQMessage(node_id, {'type': 'hello', 'destination': [node_id]})
        
        def callback():
            def hello_sender(tries_left):
                if node_id not in self.node_zids:
                    self.__send_zmq_message(zmq_msg)
                    tries_left -= 1
                    if tries_left > 0:
                        self.loop.add_timeout(self.loop.time() + 1, hello_sender, tries_left = tries_left)
            
            self.loop.add_timeout(self.loop.time() + 0.5, hello_sender, tries_left = 5)
            
        return callback
        


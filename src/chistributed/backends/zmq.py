# Needed so we can import from the global "zmq" package
from __future__ import absolute_import

import logging
import pprint
import json
import subprocess
import zmq
import random
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

class ZMQMessage(dict):
    '''
    Wraps messages, providing convenient access to the sender and JSON message in
    both bytes and interpreted form (in the case of incoming messages), and
    convenient sending.
    '''
    def __init__(self, identity, fields, original_data = None):
        self.identity = identity
        self.original_data = original_data
        
        super(ZMQMessage, self).__init__(fields)
            
    def to_frames(self):
        frames = []
        frames.append(bytes(self.identity))
        frames.append("")
        frames.append(json.dumps(self))
        return frames
            
    @classmethod
    def from_msg(cls, msg):
        pass
    
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

        # logging configuration
        # TODO: should be customizable by command line args
        logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
        self.logger = logging.getLogger('broker')


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
        message = ZMQMessage.from_zmq_frames(msg_frames)

        # TODO: Better logging
        print "Received frames:", msg_frames

        if message["type"] == "helloResponse":        
            node_name = message['source']
            if node_name in self.node_zids:
                err = "Duplicate hello from " + node_name
                self.log(err)
    
            self.node_zids[node_name] = message.identity
    
            self.log(node_name + " connected")
        elif message["type"] == "log":
            node_name = self.nodes_by_sender().get(message.identity, 'unknown node')
            self.log('log message from {node}:\n{message}'.format(node=node_name, message=pprint.pformat(message)))
        else:
            # TODO: escalate to model
            pass
        
        
    def send_message(self, node_id, msg):
        pass


    def __send_zmq_message(self, zmq_msg):        
        frames = zmq_msg.to_frames()
        
        print "Sending ", frames

        self.pub.send_multipart(frames)

    # ================
    # Message handlers
    # ================


    def dispatch(self, message):
        '''
        Find and call the correct message handler for the message.

        Handlers always return Messages; we send them back here to locally ensure
        that all node requests get a response.

        TODO: More error handling (i.e. no message type)
        '''

        # handler dict
        if not hasattr(self, "message_handlers"):
            self.message_handlers = {
                'helloResponse': self.handle_hello_response,
                'log': self.handle_log
            }

        self.handle(message)

    def handle(self, message):
        ty = message['type']
        # Default to using the handle_unknown_type handler
        f = self.message_handlers.get(ty, self.handle_unknown_type)
        return f(message)

    def handle_unknown_type(self, message):
        '''
        Forward the message to every node listed in field 'destination', replacing
        the destination with only the recipient's name.
        '''
        should_receive = set(message['destination'])
        if message.sender:
            node_name = self.nodes_by_sender()[message.sender]
            partition = self.find_partition(node_name)
            # if we're in a partition, only send to those also in the partition
            if partition:
                should_receive = should_receive.intersection(partition)
                message['destination'] = list(should_receive)

        should_drop, should_not_drop = self.message_conditions.check_drop_conditions(message)
        should_receive = should_receive.intersection(should_not_drop)

        should_delay, delay_destinations, delayed_messages = self.message_conditions.check_delay_conditions(message)
        should_receive.difference_update(delay_destinations)

        self.message_conditions.check_after_conditions(message)

        original_value = message.get('value')
        tamper_all, tamper_destinations = False, set()
        if original_value:
            tamper_all, tamper_destinations = self.message_conditions.check_tamper_conditions(message)

        if not should_drop and not should_delay:
            for dest in should_receive:
                dest_partition = self.find_partition(dest)
                if not dest_partition or dest_partition == partition:
                    if original_value and tamper_all or dest in tamper_destinations:
                        message['value'] = random.randint(0, 1000)
                    else:
                        message['value'] = original_value
                    message['destination'] = [dest]
                    message.send(self.pub, dest)

        # Note that delayed messages are subject to partitioning once they are
        # sent, and will still cross partitions if they show up here after the
        # partition has been created
        for msg in delayed_messages:
            for dest in msg['destination']:
                msg.send(self.pub, dest)

        return Message({'type': 'okay'})

    def handle_hello_response(self, message):
        '''
        Every node must send this hello message, with its name in the source field,
        before it will receive any messages. The broker will respond with the
        identical message to indicate success.


        This is a "safe" or "special" message and should not be tampered with, used
        dishonestly, etc. That is to say, it is not part of the simulation, only
        part of the set-up.
        '''
        node_name = message['source']
        if node_name in self.node_zids:
            err = "Duplicate hello from " + node_name
            self.log(err)
            return Message({'type': 'error', 'error': err})

        self.node_zids[node_name] = message.sender

        self.log(node_name + " connected")

        return message

    def handle_log(self, message):
        '''
        Print or log to a file the given message, along with sender.
        '''
        self.log_message(message)
        return Message({'type': 'okay'})

    def make_handle_response(self, ty):
        '''
        Create a handler for an expected request
        '''
        if not hasattr(self, 'pending_requests'):
            self.pending_requests = {}

        def handle_response(message):
            '''
            Handle a pending outgoing get request.
            '''
            node_name = self.nodes_by_sender().get(message.sender, "unknown node")
            ok = Message({'type': 'okay'})
            err = Message({'type': 'error'})
            req = None
            if ty in self.pending_requests:
                req = self.pending_requests[ty]

            if self.script is None: return ok
            if ty not in self.script_conditions:
                self.log("{}: not in list of expected responses: {}".format(ty, self.script_conditions))
                return err
            if ty not in self.pending_requests:
                self.log("Expected {}, but no request found (found {})".format(ty, self.pending_requests.keys()))
                return err
            if not node_name == req['destination'][0]:
                self.log("{}: different node than expected: expected {}, expected {}".format(ty, node_name, req['destination'][0]))
                return err
            if not message['id'] == self.current_request_id:
                self.log("{}: different request ID than expected: {}, expected {}".format(ty, message['id'], req['id']))
                return err
            if 'error' in message:
                self.log("{} ({}): {} ERROR: {}".format(ty, node_name, req['key'], message['error']))
                self.script_conditions.remove(ty)
                return ok
            else:
                self.log("{} ({}): {} => {}".format(ty, node_name, req['key'], message['value']))
                self.script_conditions.remove(ty)
                return ok

        return handle_response

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
        



    def send_get(self, command):
        assert len(self.script_conditions) == 0

        if 'name' in command:
            try:
                dest = command['name']
            except KeyError as e:
                self.log("No such node " + str(e) + "will try again on next 'helloResponse'")
                self.script.insert(0, command)
                self.script_conditions.add('helloResponse')
                return
        elif len(self.node_zids) == 0:
            self.log("No nodes online, will try again on next 'helloResponse'")
            self.script.insert(0, command)
            self.script_conditions.add('helloResponse')
            return
        else:
            dest = random.choice(self.node_zids.keys())

        self.script_conditions.add('getResponse')
        self.current_request_id += 1
        self.pending_requests['getResponse'] = Message({
            'type': 'get',
            'id': self.current_request_id,
            'key': command['key'],
            'destination': [dest]
        })
        self.pending_requests['getResponse'].send(self.pub, dest)

    def send_set(self, command):
        assert len(self.script_conditions) == 0

        # TODO: deduplicate/abstract out selecting node
        if 'name' in command:
            try:
                dest = command['name']
            except KeyError as e:
                self.log("No such node " + str(e) + "will try again on next 'helloResponse'")
                self.script.insert(0, command)
                self.script_conditions.add('helloResponse')
                return
        elif len(self.node_zids) == 0:
            self.log("No nodes online, will try again on next 'helloResponse'")
            self.script.insert(0, command)
            self.script_conditions.add('helloResponse')
            return
        else:
            dest = random.choice(self.node_zids.keys())

        self.script_conditions.add('setResponse')
        self.current_request_id += 1
        self.pending_requests['setResponse'] = Message({
            'type': 'set',
            'id': self.current_request_id,
            'key': command['key'],
            'value': command['value'],
            'destination': [dest]
        })
        self.pending_requests['setResponse'].send(self.pub, dest)
        pass

    def send_json(self, command):
        self.handle(Message(command['json']))
        pass

    def split_network(self, command):
        if self.partitions.get(command['name']):
            self.log("Attempted to create duplicate partition {}".format(command['name']))
            return

        self.log("Created partition {}".format(command['name']))
        self.partitions[command['name']] = set(command['nodes'])

    def join_network(self, command):
        if not self.partitions.get(command['name']):
            self.log("Attempted to delete nonexistent partition {}".format(command['name']))

        self.log("Deleted partition {}".format(command['name']))
        del self.partitions[command['name']]
        pass



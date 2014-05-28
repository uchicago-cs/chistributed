import logging
import pprint
import json
import subprocess
import os
import zmq
import random
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

import brokerscript

class Message(dict):
  '''
  Wraps messages, providing convenient access to the sender and JSON message in
  both bytes and interpreted form (in the case of incoming messages), and
  convenient sending.
  '''
  def __init__(self, msg):
    if isinstance(msg, list):
      # ZMQ frames to be wrapped
      assert len(msg) == 3
      self.sender = msg[0]
      # middle slot is empty delimiter
      self.original = msg[2]
      super(Message, self).__init__(json.loads(self.original))
    elif isinstance(msg, dict):
      # literal dict or another Message to copy
      self.sender = None
      self.original = None
      super(Message, self).__init__(msg)

  def send(self, socket, destination):
    '''
    Given a socket and an optional destination, sends the Message JSON encoded
    '''
    assert destination is not None
    destination = bytes(destination)
    b = json.dumps(self)
    # Note the empty delimiter frame
    msg_frames = [destination, '', b]
    socket.send_multipart(msg_frames)

class Broker:
  def __init__(self, node_executable, pub_endpoint, router_endpoint, script_filename=None):
    self.loop = ioloop.ZMQIOLoop.current()
    self.context = zmq.Context()

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

    self.node_executable = node_executable

    # Load script if it exists
    self.script = None
    if script_filename is not None:
      try:
        self.script = brokerscript.parse(filename=script_filename)
      except IOError as e:
        self.log("Could not find script at specified location: " + str(e))
      except brokerscript.ParseException as e:
        self.log("Could not parse script: " + str(e))

  def start(self):
    '''
    Start the IOLoop. Will allow messages to be send and received
    asynchronously on the pub and router ZMQStreams.
    '''
    self.log('Starting broker')
    if self.script is not None:
      self.log('Running script')
      self.run_script()
    self.loop.start()

  def receive_message(self, msg_frames):
    '''
    Assembles message from raw frames and dispatch it.

    msgs should be a list of two bytes values, the first being the ZMQ ID for
    the sender, and the second being the JSON message from the nodes.
    '''
    message = Message(msg_frames)
    self.dispatch(message)

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
        'helloResponse': self.handle_hello,
        'hello': self.handle_hello, # for backwards compatibility
        'log': self.handle_log,
        'getResponse': self.make_handle_response('getResponse'),
        'setResponse': self.make_handle_response('setResponse')
      }

    try:
      resp = self.handle(message)
      resp.send(self.router, message.sender)
      self.run_script(message)
    except KeyError as e: # catchall for malformed messages
      self.log("Missing key: " + str(e))
      self.log_message(message)

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
    m = Message(message)
    for dest in message['destination']:
      m['destination'] = [dest]
      m.send(self.pub, dest)

    return Message({'type': 'okay'})

  def handle_hello(self, message):
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

    if self.script and 'hello' in self.script_conditions:
      self.script_conditions.remove('hello')

    self.log(node_name + " connected")

    return message

  def handle_log(self, message):
    '''
    Print or log to a file the given message, along with sender.
    '''
    # self.log_message(message)
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
        self.log("Did not expect {}, expected {}".format(ty, self.script_conditions))
        return err
      if ty not in self.pending_requests:
        self.log("Expected {}, but no request found (found {})".format(ty, self.pending_requests.keys()))
        return err
      if not node_name == req['destination'][0]:
        self.log("Different node than expected: {}, expected {}".format(node_name, req['destination'][0]))
        return err
      if not message['id'] == self.current_request_id:
        self.log("Different request ID than expected: {}, expected {}".format(message['id'], req['id']))
        return err
      if 'error' in message:
        self.log("{}: {} {} ERROR: {}".format(node_name, ty, req['key'], message['error']))
        self.script_conditions.remove(ty)
        return ok
      else:
        self.log("{}: {} {} => {}".format(node_name, ty, req['key'], message['value']))
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

  def log_message(self, message):
    '''
    Log the message with its sender.
    '''
    node_name = self.nodes_by_sender().get(message.sender, 'unknown node')
    self.log('log message from {node}:\n{message}'.format(node=node_name, message=pprint.pformat(message)))

  def log(self, log_msg):
    '''
    Log the given line at INFO level.

    TODO: meaningful use of different levels.
    '''
    self.logger.info(log_msg)

  # ============================
  # Simulation control/scripting
  # ============================

  def run_script(self, message=None):
    '''
    Runs as much of the script as can be until a blocking command (i.e. set,
    get, sync).

    Also sets up instance variables only used for scripting.
    '''
    if not hasattr(self, "script_handlers"):
      self.script_handlers = {
        'start': self.start_node,
        'stop': self.stop_node,
        'get': self.send_get,
        'set': self.send_set,
        'json': self.send_json
      }
      self.script_conditions = set()
      self.current_request_id = 0
      self.current_request = None

    if message:
      # generic conditions met here
      pass

    try:
      while (len(self.script_conditions) == 0) and len(self.script) > 0:
        command = self.script.pop(0)
        f = self.script_handlers[command['command']]
        f(command)
        if len(self.script) == 0:
          self.log("Script finished")
    except KeyError as e:
      self.log("Command not found: " + str(e))


  def start_node(self, command):
    '''
    Start a node with given name and parameters.

    Note all output will be sent to /dev/null (or the platform's equivalent).

    Highly unsafe of course.
    '''
    args = self.node_executable + \
           ' --node-name ' + command['name'] + \
           ' --pub-endpoint ' + self.pub_endpoint + \
           ' --router-endpoint ' + self.router_endpoint + \
           command['params']

    if not hasattr(self, "devnull"):
      self.devnull = open(os.devnull, "w")

    proc = subprocess.Popen(args, shell=True, stdout=self.devnull, stderr=self.devnull)
    self.node_pids[command['name']] = proc

    self.script_conditions.add('hello')

    self.make_hello_sender(command['name'])()

    pass

  def make_hello_sender(self, node_name):
    msg = Message({
      'type': 'hello',
      'destination': [node_name]
    })
    def hello_sender():
      if 'hello' in self.script_conditions:
        msg.send(self.pub, node_name)
        self.loop.add_timeout(self.loop.time() + 0.1, hello_sender)
      return

    return hello_sender

  def stop_node(self, command):
    '''
    Sends SIGTERM to the named node.

    Node implementations should catch it and shutdown because killing procs is
    risky business.
    '''
    self.log("stopping " + command['name'])
    self.node_pids[command['name']].terminate()
    del self.node_pids[command['name']]
    pass

  def send_get(self, command):
    assert len(self.script_conditions) == 0

    if 'name' in command:
      try:
        dest = command['name']
      except KeyError as e:
        self.log("No such node " + str(e) + "will try again on next 'hello'")
        self.script.insert(0, command)
        self.script_conditions.add('hello')
        return
    elif len(self.node_zids) == 0:
      self.log("No nodes online, will try again on next 'hello'")
      self.script.insert(0, command)
      self.script_conditions.add('hello')
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
        self.log("No such node " + str(e) + "will try again on next 'hello'")
        self.script.insert(0, command)
        self.script_conditions.add('hello')
        return
    elif len(self.node_zids) == 0:
      self.log("No nodes online, will try again on next 'hello'")
      self.script.insert(0, command)
      self.script_conditions.add('hello')
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

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  # more arguments than this will be necessary
  parser.add_argument('--node-executable', '-e',
      help='Set the name of the executable for nodes.',
      dest='node_executable', type=str,
      default='python examples/node.py')
  parser.add_argument('--pub-endpoint', '-p',
      help='Set the endpoint for the PUB socket.',
      dest='pub_endpoint', type=str,
      default='tcp://127.0.0.1:23310')
  parser.add_argument('--router-endpoint', '-r',
      help='Set the endpoint for the ROUTER socket.',
      dest='router_endpoint', type=str,
      default='tcp://127.0.0.1:23311')
  parser.add_argument('--script', '-s',
      help='Execute the given file as a broker script.',
      dest='script_filename', type=str,
      default=None)
  args = parser.parse_args()
  Broker(args.node_executable,
         args.pub_endpoint,
         args.router_endpoint,
         args.script_filename).start()


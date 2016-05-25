#  Copyright (c) 2016, The University of Chicago
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#
#  - Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
#  - Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
#  - Neither the name of The University of Chicago nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
#  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
#  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
#  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
#  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
#  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
#  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
#  POSSIBILITY OF SUCH DAMAGE.

from collections import deque
from threading import Lock, Condition

import colorama
import chistributed.common.log as log
colorama.init()

from chistributed.common import ChistributedException

class Message(object):
    
    def __init__(self, msg_type, description):
        self.msg_type = msg_type
        self.description = description

class GetRequestMessage(Message):
    def __init__(self, destination, msg_id, key):
        Message.__init__(self, "get", "GET Request")
        
        self.destination = destination
        self.id = msg_id
        self.key = key
        
class GetResponseOKMessage(Message):
    def __init__(self, msg_id, key, value):
        Message.__init__(self, "getResponse", "GET Response (OK)")
        
        self.id = msg_id
        self.key = key
        self.value = value

class GetResponseErrorMessage(Message):
    def __init__(self, msg_id, error):
        Message.__init__(self, "getResponse", "GET Response (Error)")
        
        self.id = msg_id
        self.error = error
        
class SetRequestMessage(Message):
    def __init__(self, destination, msg_id, key, value):
        Message.__init__(self, "set", "SET Request")
        
        self.destination = destination
        self.id = msg_id
        self.key = key
        self.value = value
        
class SetResponseOKMessage(Message):
    def __init__(self, msg_id, key, value):
        Message.__init__(self, "setResponse", "SET Response (OK)")
        
        self.id = msg_id
        self.key = key
        self.value = value

class SetResponseErrorMessage(Message):
    def __init__(self, msg_id, error):
        Message.__init__(self, "setResponse", "SET Response (Error)")
        
        self.id = msg_id
        self.error = error
        
class CustomMessage(Message):
    def __init__(self, msg_type, destination, values):
        Message.__init__(self, msg_type, "'{}' Message".format(msg_type))
        
        self.destination = destination
        self.source = None
        self.values = {}
        self.values.update(values)     
        
    def set_source(self, source):
        self.source = source
        

class Node(object):
    STATE_INIT = 0
    STATE_STARTING = 1
    STATE_RUNNING = 2
    STATE_STOPPED = 3
    STATE_PARTITIONED = 4
    STATE_FAILED = 5
    
    state_str = {STATE_INIT: "Initial",
                 STATE_STARTING: "Starting",
                 STATE_RUNNING: "Running",
                 STATE_STOPPED: "Stopped",
                 STATE_FAILED: "Failed"}
    
    def __init__(self, node_id):
        self.node_id = node_id
        self.state = Node.STATE_INIT
        
        self.cv_lock = Lock()
        self.cv = Condition(self.cv_lock)
        
    def wait_for_state(self, state):
        self.cv.acquire()
        while self.state not in (state, Node.STATE_FAILED):
            self.cv.wait()
        self.cv.release()
        
        if self.state != state:
            raise ChistributedException("Node entered failed state while waiting for state %s" % Node.state_str[state])
        
    def set_state(self, state):
        self.cv.acquire()
        self.state = state
        self.cv.notify_all()
        self.cv.release()        

class Partition(object):
    def __init__(self, name, nodes1, nodes2):
        self.name = name
        self.nodes1 = set(nodes1)
        self.nodes2 = set(nodes2)
        
    def are_partitioned(self, node1, node2):
        return (node1 in self.nodes1 and node2 in self.nodes2) or (node2 in self.nodes1 and node1 in self.nodes2) 

class DistributedSystem(object):
    
    def __init__(self, backend, nodes):
        self.backend = backend
        
        backend.set_ds(self)
        
        self.nodes = {n: Node(n) for n in nodes}

        self.partitions = {}
        
        # Deques are thread-safe, but we still need a lock
        # to safely iterate through the list.
        self.msg_queue = deque()
        self.msg_queue_lock = Lock()
        
        self.pending_set_requests = {}
        self.pending_get_requests = {}
        
        self.next_id = 1
        
    def start_node(self, node_id, extra_params=[]):
        if not node_id in self.nodes:
            raise ChistributedException("No such node: {}".format(node_id))

        self.nodes[node_id].set_state(Node.STATE_STARTING)
        
        self.backend.start_node(node_id, extra_params)        
        
    def send_set_msg(self, node_id, key, value):
        if not node_id in self.nodes:
            raise ChistributedException("No such node: {}".format(node_id))

        msg = SetRequestMessage(node_id, self.next_id, key, value)
        
        self.pending_set_requests[self.next_id] = msg
        
        self.next_id += 1
        
        self.backend.send_message(node_id, msg)

    def send_get_msg(self, node_id, key):
        if not node_id in self.nodes:
            raise ChistributedException("No such node: {}".format(node_id))

        msg = GetRequestMessage(node_id, self.next_id, key)
        
        self.pending_get_requests[self.next_id] = msg
        
        self.next_id += 1
        
        self.backend.send_message(node_id, msg)
        
    def process_message(self, msg, source):
        if source is not None and source not in self.nodes:
            raise ChistributedException("No such node: {}".format(source))
        
        if isinstance(msg, (GetResponseOKMessage, GetResponseErrorMessage, SetResponseOKMessage, SetResponseErrorMessage)):
            if isinstance(msg, (GetResponseOKMessage, GetResponseErrorMessage)):
                pending = self.pending_get_requests.get(msg.id, None)
                msg_type = "GET"
            elif isinstance(msg, (SetResponseOKMessage, SetResponseErrorMessage)):
                pending = self.pending_set_requests.get(msg.id, None)
                msg_type = "SET"
                
            if pending is None:
                s = colorama.Style.BRIGHT + colorama.Fore.YELLOW
                s += "WARNING: Received unexpected %s response id=%i" % (msg_type, msg.id)
                s += colorama.Style.RESET_ALL
                print s
            else:
                msg_name = "%s id=%s" % (msg_type, msg.id)
                if isinstance(msg, GetResponseErrorMessage):
                    s = colorama.Style.BRIGHT + colorama.Fore.RED
                    s += "ERROR: %s failed (k=%s): %s" % (msg_name, pending.key, msg.error)
                    s += colorama.Style.RESET_ALL
                    print s
                elif isinstance(msg, SetResponseErrorMessage):
                    s = colorama.Style.BRIGHT + colorama.Fore.RED
                    s += "ERROR: %s failed (%s=%s): %s" % (msg_name, pending.key, pending.value, msg.error)
                    s += colorama.Style.RESET_ALL
                    print s
                elif isinstance(msg, GetResponseOKMessage):
                    if pending.key != msg.key:
                        s = colorama.Style.BRIGHT + colorama.Fore.YELLOW
                        s += "WARNING: %s response has unexpected key (got %s=%s, expected %s=%s" % (msg_name, msg.key, msg.value, pending.key, msg.value)
                        s += colorama.Style.RESET_ALL
                        print s                    
                    else:
                        s = colorama.Style.BRIGHT + colorama.Fore.GREEN
                        s += "%s OK: %s = %s" % (msg_name, msg.key, msg.value)
                        s += colorama.Style.RESET_ALL
                        print s
                elif isinstance(msg, SetResponseOKMessage):
                    if pending.key != msg.key or pending.value != msg.value:
                        s = colorama.Style.BRIGHT + colorama.Fore.YELLOW
                        s += "WARNING: %s response has unexpected values (got %s=%s, expected %s=%s" % (msg_name, msg.key, msg.value, pending.key, pending.value)
                        s += colorama.Style.RESET_ALL
                        print s                    
                    else:
                        s = colorama.Style.BRIGHT + colorama.Fore.GREEN
                        s += "%s OK: %s = %s" % (msg_name, msg.key, msg.value)
                        s += colorama.Style.RESET_ALL
                        print s
                        
                if isinstance(msg, (GetResponseOKMessage, GetResponseErrorMessage)):
                    del self.pending_get_requests[msg.id]
                elif isinstance(msg, (SetResponseOKMessage, SetResponseErrorMessage)):
                    del self.pending_set_requests[msg.id]                        
                            
        elif isinstance(msg, CustomMessage):
            msg.set_source(source)            
            
            self.msg_queue_lock.acquire()
            self.msg_queue.appendleft(msg)
            self.msg_queue_lock.release()
        
            self.__process_queue()
        
    def add_partition(self, name, nodes1, nodes2 = None):
        if name in self.partitions:
            raise ChistributedException("A partition named '%s' already exists" % name)
        
        for n in nodes1:
            if n not in self.nodes:
                raise ChistributedException("No such node: %s" % n)

        if nodes2 is None:
            nodes2 = [n for n in self.nodes if n not in nodes1]
        else:
            for n in nodes2:
                if n not in self.nodes:
                    raise ChistributedException("No such node: %s" % n)
            
        p = Partition(name, [self.nodes[n] for n in nodes1], [self.nodes[n] for n in nodes2])
                
        self.partitions[name] = p
        
        
    def remove_partition(self, name, deliver):
        if name not in self.partitions:
            raise ChistributedException("No such partition: %s" % name)
        
        # Acquire lock to prevent messages being added to message queue
        # while we remove the partition
        self.msg_queue_lock.acquire()        
               
        self.__process_partitioned_messages(self.partitions[name], deliver)
        
        del self.partitions[name]        
        
        self.msg_queue_lock.release()
        
        
    def fail_node(self, node_id):
        if not node_id in self.nodes:
            raise ChistributedException("No such node: {}".format(node_id))
        
        n = self.nodes[node_id]
        
        if n.state == Node.STATE_PARTITIONED:
            raise ChistributedException("Node {} is already in a failed state".format(node_id))
            
        if n.state != Node.STATE_RUNNING:
            raise ChistributedException("Node {} cannot be failed because it is not running".format(node_id))

        self.add_partition(self.__failed_node_partition_name(node_id), [node_id])
        
        n.set_state(Node.STATE_PARTITIONED)
        
        
    def recover_node(self, node_id, deliver):
        if not node_id in self.nodes:
            raise ChistributedException("No such node: {}".format(node_id))

        n = self.nodes[node_id]
        
        if n.state != Node.STATE_PARTITIONED:
            raise ChistributedException("Node {} is not in a failed state".format(node_id))
                
        self.remove_partition(self.__failed_node_partition_name(node_id), deliver)
        
        n.set_state(Node.STATE_RUNNING)
        

    def __failed_node_partition_name(self, node_id):
        return "#fail-{}#".format(node_id)
        
        
    def __process_queue(self):
        self.msg_queue_lock.acquire()

        n = len(self.msg_queue)
        
        while n > 0:
            msg = self.msg_queue.pop()
            assert isinstance(msg, CustomMessage)
            
            src_node = self.nodes[msg.source]
            
            if msg.destination not in self.nodes:
                log.warning("Message with unknown destination {}".format(msg.destination))
                dst_node = None
            else:
                dst_node = self.nodes[msg.destination]
                            
            deliver = True
            for p in self.partitions.values():
                if p.are_partitioned(src_node, dst_node):
                    deliver = False
            
            if deliver:
                self.backend.send_message(msg.destination, msg)
            else:
                self.msg_queue.appendleft(msg)

            n -= 1

        self.msg_queue_lock.release()
        

    def __process_partitioned_messages(self, p, deliver):
        n = len(self.msg_queue)
        
        while n > 0:
            msg = self.msg_queue.pop()
            assert isinstance(msg, CustomMessage)
            
            src_node = self.nodes[msg.source]
            
            if msg.destination not in self.nodes:
                log.warning("Message with unknown destination {}".format(msg.destination))
                dst_node = None
            else:
                dst_node = self.nodes[msg.destination]
                            
            if p.are_partitioned(src_node, dst_node):
                if deliver:
                    self.backend.send_message(msg.destination, msg)
            else:
                self.msg_queue.appendleft(msg)

            n -= 1

        
        
        
        
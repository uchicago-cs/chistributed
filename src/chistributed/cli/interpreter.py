
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
#  ARISING IN ANY WAY OUT OF THE USE 

import cmd2
import time

from cmd2 import options
from optparse import make_option
from chistributed.core.model import Node, Message
from chistributed.common import ChistributedException

class Interpreter(cmd2.Cmd):
    prompt = "> "
    
    def __init__(self, ds):
        self.ds = ds
        cmd2.Cmd.__init__(self)
    
    
    @options([make_option('-n', '--node_id', type="string"),
              make_option('--no-wait', action="store_true")
             ])        
    def do_start(self, args, opts=None):
        node_id = opts.node_id
        
        if node_id not in self.ds.nodes:
            print "No such node: %s" % (node_id)
            return
        
        peers = [n for n in self.ds.nodes if n != node_id]
        
        node_opts = []
        for p in peers:
            node_opts += ["--peer", p]    
            
        node_opts += args.split()
        
        try:
            self.ds.start_node(node_id, node_opts)
            
            if not opts.no_wait:
                self.ds.nodes[node_id].wait_for_state(Node.STATE_RUNNING)
        except ChistributedException, ce:
            print "Error when starting node %s: %s" % (node_id, ce.message)
            


    @options([make_option('-n', '--node_id', type="string"),
              make_option('-k', '--key', type="string"),
              make_option('--no-wait', action="store_true")
             ])          
    def do_get(self, args, opts=None):
        node_id = opts.node_id
        
        if node_id not in self.ds.nodes:
            print "No such node: %s" % (node_id)
            return        
                
        self.ds.send_get_msg(node_id, opts.key)
        
        
    @options([make_option('-n', '--node_id', type="string"),
              make_option('-k', '--key', type="string"),
              make_option('-v', '--value', type="string"),
              make_option('--no-wait', action="store_true")
             ])          
    def do_set(self, args, opts=None):
        node_id = opts.node_id
        
        if node_id not in self.ds.nodes:
            print "No such node: %s" % (node_id)
            return        
                
        self.ds.send_set_msg(node_id, opts.key, opts.value)        
        
        
    @options([make_option('-t', '--time', type="float")
             ])
    def do_wait(self, args, opts=None):
        time.sleep(opts.time)


    @options([make_option('-n', '--node_id', type="string")
             ])          
    def do_fail_node(self, args, opts=None):
        node_id = opts.node_id
        
        if node_id not in self.ds.nodes:
            print "No such node: %s" % (node_id)
            return        
                
        self.ds.fail_node(node_id)         

        
    @options([make_option('-n', '--node_id', type="string"),
              make_option('-d', '--deliver', action="store_true")
             ])          
    def do_recover_node(self, args, opts=None):
        node_id = opts.node_id
        
        if node_id not in self.ds.nodes:
            print "No such node: %s" % (node_id)
            return        
                
        self.ds.recover_node(node_id, opts.deliver)
        
        
    @options([make_option('-n', '--name', type="string"),
              make_option('-p', '--partition', type="string"),
              make_option('-2', '--partition2', type="string", default="")
             ])          
    def do_create_partition(self, args, opts=None):
        name = opts.name
        
        if name in self.ds.partitions:
            print "There is already a partition with this name: %s" % (name)
            return        
            
        nodes1 = opts.partition.split(",")
        for n in nodes1:
            if n not in self.ds.nodes:
                print "No such node: %s" % (n)
                return        
        
        if len(opts.partition2) == 0:
            nodes2 = None
        else:
            nodes2 = opts.partition2.split(",")
            for n in nodes2:
                if n not in self.ds.nodes:
                    print "No such node: %s" % (n)
                    return            
                
        self.ds.add_partition(name, nodes1, nodes2)    
        
        
    @options([make_option('-n', '--name', type="string"),
              make_option('-d', '--deliver', action="store_true")
             ])          
    def do_remove_partition(self, args, opts=None):
        name = opts.name
        
        if not name in self.ds.partitions:
            print "There is no partition with this name: %s" % (name)
            return             
                
        self.ds.remove_partition(name, opts.deliver)        
        
    
    def do_send_msg(self, s):
        msg = Message.from_json(s)
        self.ds.backend.send_message(msg.destination, msg)

                
        
    @options([make_option('-v', '--verbose', action="store_true")
             ])  
    def do_show_queue(self, args, opts=None):
        self.ds.msg_queue_lock.acquire()
        
        if len(self.ds.msg_queue) == 0:
            print "No messages in message queue."
        else:
            for msg in self.ds.msg_queue:
                print "{} -> {}: {}".format(msg.source, msg.destination, msg.msg_type)
                if opts.verbose:
                    print msg.values
                    print

        self.ds.msg_queue_lock.release()
        
        
    @options([make_option('-f', '--include-failed-nodes', action="store_true")])  
    def do_show_partitions(self, args, opts=None):
        for p in self.ds.partitions.values():
            if p.name.startswith("#fail-") and not opts.include_failed_nodes:
                continue
            
            print "Partition '%s'" % p.name
            print "  Nodes 1: %s" % ", ".join([n.node_id for n in p.nodes1])    
            print "  Nodes 2: %s" % ", ".join([n.node_id for n in p.nodes2])    
                 
        
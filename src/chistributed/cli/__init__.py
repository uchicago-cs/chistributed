
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


import click
import signal
import threading
import sys
import os.path

from chistributed import RELEASE
import chistributed.common.log as log
from chistributed.common import CHISTRIBUTED_FAIL, CHISTRIBUTED_SUCCESS,\
    ChistributedException
from chistributed.common.config import Config
from chistributed.backends.zmq import ZMQBackend
from chistributed.cli.interpreter import Interpreter
from chistributed.core.model import DistributedSystem, Node
import traceback

@click.command(name="chistributed")
@click.option('--config', '-c', type=str, multiple=True)
@click.option('--config-file', type=str)
@click.option('--pub-port', type=int, default=23310)
@click.option('--router-port', type=int, default=23311)
@click.option('--verbose', '-v', is_flag=True)
@click.option('--debug', is_flag=True)
@click.option('--show-node-output', is_flag=True)
@click.option('--run', type=str)
@click.version_option(version=RELEASE)
def chistributed_cmd(config, config_file, pub_port, router_port, verbose, debug, show_node_output, run):
    log.init_logging(verbose, debug)

    try:
        config_overrides = {}
        for c in config:
            if c.count("=") != 1 or c[0] == "=" or c[-1] == "=":
                raise click.BadParameter("Invalid configuration parameter: {}".format(c))
            else:
                k, v = c.split("=")
                config_overrides[k] = v
    
        if config_file is None:
            config_file = "chistributed.conf"
                    
        if not os.path.exists(config_file):
            raise click.BadParameter("Configuration file (%s) does not exist." % config_file)
    
        config_obj = Config.get_config(config_file, config_overrides)
    
        pub_endpoint = 'tcp://127.0.0.1:%i' % pub_port
        router_endpoint = 'tcp://127.0.0.1:%i' % router_port
    
        backend = ZMQBackend(config_obj.get_node_executable(), pub_endpoint, router_endpoint, debug = debug, show_node_output = show_node_output)
        
        ds = DistributedSystem(backend, config_obj.get_nodes())
    
        interpreter = Interpreter(ds)
        
        # Run broker in separate thread
        def backend_thread():
            backend.start()                
    
        t = threading.Thread(target=backend_thread)
        t.daemon = True
        t.start()    
    
        def signal_handler(signal, frame):
            print('SIGINT received')
            backend.stop()
            
            t.join()
    
            interpreter.do_quit(None)
            
            sys.exit(1)
                    
        signal.signal(signal.SIGINT, signal_handler)
        
        try:            
            if backend.running:
                if run is not None:
                    stop = interpreter.do_load(run)
                else:
                    stop = False
            
                if not stop:
                    # Call _cmdloop instead of cmdloop to prevent cmd2 from
                    # trying to parse command-line arguments
                    interpreter._cmdloop()
        except Exception as e:
            print("ERROR: Unexpected exception %s" % (e))
            if debug:
                print(traceback.format_exc())
            interpreter.do_quit(None)
        
        backend.stop()
        
        t.join()
    except ChistributedException as ce:
        print("ERROR: %s" % ce.message)
        if debug:
            ce.print_exception()
        sys.exit(-1)
    except Exception as e:
        print("ERROR: Unexpected exception %s" % (e))
        sys.exit(-1)

    return CHISTRIBUTED_SUCCESS

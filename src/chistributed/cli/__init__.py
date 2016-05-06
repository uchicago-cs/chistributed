
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

from chistributed import RELEASE
import chistributed.common.log as log
from chistributed.common import CHISTRIBUTED_FAIL, CHISTRIBUTED_SUCCESS
from chistributed.common.config import Config
from chistributed.backends.zmq import ZMQBackend
from chistributed.cli.interpreter import Interpreter
from chistributed.core.model import DistributedSystem, Node
import traceback

@click.command(name="chistributed")
@click.option('--config', '-c', type=str, multiple=True)
@click.option('--config-file', type=click.File('r'))
@click.option('--verbose', '-v', is_flag=True)
@click.option('--debug', is_flag=True)
@click.argument('script_file', type=str, default=None)
@click.version_option(version=RELEASE)
def chistributed_cmd(config_file, config, verbose, debug, script_file):
    log.init_logging(verbose, debug)

    config_overrides = {}
    for c in config:
        if c.count("=") != 1 or c[0] == "=" or c[-1] == "=":
            raise click.BadParameter("Invalid configuration parameter: {}".format(c))
        else:
            k, v = c.split("=")
            config_overrides[k] = v

    config_obj = Config.get_config(config_file, config_overrides)

    backend = ZMQBackend(config_obj.get_node_executable(), 'tcp://127.0.0.1:23310', 'tcp://127.0.0.1:23311', debug = debug)
    
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
            if script_file is not None:
                interpreter.do_load(script_file)            
            
            # Call _cmdloop instead of cmdloop to prevent cmd2 from
            # trying to parse command-line arguments
            interpreter._cmdloop()
    except Exception, e:
        print "ERROR: Unexpected exception %s" % (e)
        if debug:
            print traceback.format_exc()
        interpreter.do_quit(None)
    
    backend.stop()
    
    t.join()

    return CHISTRIBUTED_SUCCESS

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
from chistributed import RELEASE
import chistributed.common.log as log
from chistributed.common import CHISTRIBUTED_FAIL, CHISTRIBUTED_SUCCESS
from chistributed.common.config import Config
from chistributed.backends.zmq import ZMQBackend
from chistributed.cli.interpreter import Interpreter
import threading
from chistributed.core.model import DistributedSystem

@click.command(name="chistributed")
@click.option('--config', '-c', type=str, multiple=True)
@click.option('--config-file', type=click.File('r'))
@click.option('--verbose', '-v', is_flag=True)
@click.option('--debug', is_flag=True)
@click.version_option(version=RELEASE)
def chistributed_cmd(config_file, config, verbose, debug):
    global VERBOSE, DEBUG
    
    VERBOSE = verbose
    DEBUG = debug
    
    log.init_logging(verbose, debug)

    config_overrides = {}
    for c in config:
        if c.count("=") != 1 or c[0] == "=" or c[-1] == "=":
            raise click.BadParameter("Invalid configuration parameter: {}".format(c))
        else:
            k, v = c.split("=")
            config_overrides[k] = v

    config_obj = Config.get_config(config_file, config_overrides)

    backend = ZMQBackend(config_obj.get_node_executable(), 'tcp://127.0.0.1:23310', 'tcp://127.0.0.1:23311')
    
    # Run broker in separate thread
    def backend_thread():
        backend.start()
        print "Backend thread exiting"

    t = threading.Thread(target=backend_thread)
    t.daemon = True
    t.start()    
    
    ds = DistributedSystem(backend, config_obj.get_nodes())

    ds.start_node("node-1")
    ds.start_node("node-2")
    #ds.start_node("node-3")
    
    #ds.send_set_msg("node-1", "A", 42)
    
    interpreter = Interpreter()
    
    interpreter.cmdloop()
    
    backend.stop()
    
    t.join()

    return CHISTRIBUTED_SUCCESS
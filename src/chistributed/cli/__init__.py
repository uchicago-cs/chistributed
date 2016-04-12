
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


@click.option('--config', '-c', type=str, multiple=True)
@click.option('--config-dir', type=str, default=None)
@click.option('--work-dir', type=str, default=None)
@click.option('--verbose', '-v', is_flag=True)
@click.option('--debug', is_flag=True)
@click.version_option(version=RELEASE)
@click.pass_context
def chisubmit_cmd(ctx, config, config_dir, work_dir, verbose, debug):
    global VERBOSE, DEBUG
    
    VERBOSE = verbose
    DEBUG = debug
    
    if config_dir is None and work_dir is not None:
        print "You cannot specify --work-dir without --config-dir"
        ctx.exit(CHISTRIBUTED_FAIL)

    if config_dir is not None and work_dir is None:
        print "You cannot specify --config-dir without --work-dir"
        ctx.exit(CHISTRIBUTED_FAIL)
    
    log.init_logging(verbose, debug)

    config_overrides = {}
    for c in config:
        if c.count("=") != 1 or c[0] == "=" or c[-1] == "=":
            raise click.BadParameter("Invalid configuration parameter: {}".format(c))
        else:
            k, v = c.split("=")
            config_overrides[k] = v

    ctx.obj = {}

    ctx.obj["config_overrides"] = config_overrides
    ctx.obj["config_dir"] = config_dir
    ctx.obj["work_dir"] = work_dir
    ctx.obj["verbose"] = verbose
    ctx.obj["debug"] = debug

    return CHISTRIBUTED_SUCCESS
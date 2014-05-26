
chistributed: A Distributed Systems Simulator


SOFTWARE PREREQUISITES
----------------------

- Python 2.7.x
- ZeroMQ 4.0.4 (http://zeromq.org/area:download)
- pyzmq (https://pypi.python.org/pypi/pyzmq)
- tornado 3.2.1 (https://pypi.python.org/pypi/tornado)

Note: we recommend you install ZeroMQ from source (since up-to-date binary packages
are not available for most systems). It has no dependencies, so it should be
straightforward to install just by downloading the ZeroMQ tarball, and then running:

   ./configure
   make
   make install

We also recommend you install pyzmq and tornado usign "pip":

    pip install pyzmq
    pip install tornado

If the "pip" command is not available in your system, please see
http://pip.readthedocs.org/en/latest/installing.html

Please note that installing pyzmq will compile native extensions using the current
version of ZeroMQ installed on your system. Make sure you install pyzmq *after*
you've installed the 4.0.4 version of ZeroMQ (in case your system has an older
version of ZeroMQ installed).


RUNNING THE BROKER
------------------

The broker takes two parameters:

  -e / --node-executable: The executable that will be run as a node.
                          If using a scripting language, then specify
                          the full invocation necessary to run your
                          code (e.g., "python examples/node.py")

  -s / --script: chidistributed script to run

To run the simple example:

python broker.py -e "python examples/node.py" -s simple.chi

This sets the main node executable to running "python examples/node.py" and 
then runs the script contained in "simple.chi"


IMPLEMENTING A NODE
-------------------

Please refer to the final project documentation for details on how to
implement a chistributed node. However, please note that you must provide
the "--peer-names" parameters *explicitly* in the chistributed script;
it will not be generated automatically by the broker (this actually gives
you more freedom in deciding what information to reveal about peers
when starting a node, as well as allowing you to start additional nodes
later on).


AUTHORS
-------
Michael Victor Zink
Borja Sotomayor


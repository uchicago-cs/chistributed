chistributed: A Distributed Systems Simulator
=============================================

Installing
----------

Note: These instructions have only been tested on UChicago CS machines. They
should work in other UNIX environments, including Macs, but your mileage
may vary.

First, clone chistributed into any directory you want:

    git clone https://github.com/uchicago-cs/chistributed.git

Inside the chistributed directory, run the following:

    pip install --user -e .

Finally, change your $PATH (you may want to include this in your `.bashrc`
file to make sure it gets set correctly every time you log in:

    export PATH=~/.local/bin:$PATH

You can verify whether chistributed is correctly installed by running
the following:

    chistributed --version

This should print out a version number.

Take into account that, if we push changes to the chistributed repository, you
will be able to start using those changes just by pulling them to your local
chistributed repository. You will not need to repeat the installation instructions
(unless we explicitly tell you to, e.g., if we add an extra software dependency).


Running the sample data store
-----------------------------

We provide a sample data store in `samples/python/` that uses a very rudimentary
replication strategy: if a node receives a "set" request, it will duplicate the
value to all the other nodes, but will not check whether the value has been correctly
duplicated. A "get" request will return the latest value stored in that node (if any).

To test this data store, simply run chistributed from the `samples/python/` directory:

    chistributed

This will start the chistributed shell:

    >

This data store is defined to use just three nodes, `node-1`, `node-2`, and `node-3`. You
can start the nodes by entering the following commands into the chistributed shell:

    start -n node-1
    start -n node-2
    start -n node-3

Note: If the node starts up correctly, no messages will be printed. You will simply return
to the shell's prompt.

Next, try doing a set (this command will set key `A` to `42`):

    set -n node-1 -k A -v 42

If the operation is succesful, you will see this:

    SET id=1 OK: A = 42

Note: The output of the operation may overlap with the shell. If so, just press enter to get a fresh prompt.

Next, you can verify that the set was correctly done in `node-1`:

    get -n node-1 -k A

But also propagated to the other nodes:

    get -n node-2 -k A
    get -n node-3 -k A

All of these should print the following:

    GET id=ID OK: A = 42

Where `ID` will be the identifier of the request, as described in the chistributed documentation.

However, if you try to get a key that doesn't exist:

    get -n node-1 -k foobar

You should get an error:

    ERROR: GET id=4 failed (k=foobar): No such key: foobar

You can also run chistributed with options `--verbose` or `--debug`
to get more information on what chistributed is doing. In particular, the `--debug` option will
allow you to see the output from the node (which is typically not shown) as well as all the
messages being sent between the message broker and the nodes. We suggest you run through the
above examples with the `--debug` flag to get a better sense for all the messages that are
being sent.

You can also tell chistributed to run a series of commands at startup. For example, the sample
data store includes a `start-nodes.chi` file with the `start` commands shown earlier. To run
these commands on startup, just run chistributed like this:

    chistributed --run start-nodes.chi

Implementing a node
-------------------

To implement a node, you first create an empty directory to contain a configuration file and your chistributed scripts. This directory does not necessarily have to include your source code.

Next, create a file called `chistributed.conf` with the following contents:

    node-executable: NODE_EXECUTABLE
    nodes: NODES

Where:

- `NODE_EXECUTABLE` should be the command that chistributed should run your node executable. For example, if you implemented the node in Python, this could be `python node.py`.
If you implemented it in a compiled language, it could be `./node` (assuming the `node` executable is in
the same directory as `chistributed.conf`).
- `NODES` is a space-separated list of nodes in your data store. Note that these are not automatically
  started when running chistributed.

When implementing your node executable, you must write it to accept at least the following command-line parameters. All of these are supplied by the broker when running your node:

- `--pub-endpoint`: URI of the broker's ZeroMQ PUB socket.
- `--router-endpoint`: URI of the broker's ZeroMQ ROUTER socket.
- `--node-name`: The node's name.
- `--peer`: This option can appear multiple times, and is used to specify all the other nodes
  in the data store. Note that the broker will provide the names of all the other nodes, regardless
  of whether they have started or not.

For example, in the sample data store discussed above, when running this command in the shell:

    start -n node-1

This resulted in the broker running the following:

    python node.py --pub-endpoint tcp://127.0.0.1:23310 --router-endpoint tcp://127.0.0.1:23311 \
                   --node-name node-1 --peer node-2 --peer node-3

Note: you can define your node executable to accept additional parameters. The above are simply the
bare minimum you need for your node to be able to work with the chistributed broker.

When your node starts, you must do the following:

- Create a ZeroMQ SUB socket connected to the address specified in the `--pub-endpoint` parameter. This
  socket must be subscribed (by setting the ZeroMQ SUBSCRIBE option) to the name specified in the
  `--node-name` parameter.
- Create a ZeroMQ REQ socket connected to the address specified in the `--router-endpoint` parameter.

The exact code to do this will vary from one language to another. Please consult the documentation
for the appropriate ZeroMQ language bindings.

Next, you should define a message handler for the messages that arrive through the SUB socket.
This handler should, at the very least, respond to a `hello` message with a `helloResponse`.




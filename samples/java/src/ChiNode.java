import java.util.List;

import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

/**
 * Chistributed Node class. Implementation of an abstract node to encapsulate
 * all communication with the ZeroMQ based Chistributed system.
 */
public abstract class ChiNode {
	private final ZLoop loop;
	private final ZContext context;
	private final ZMQ.Socket subSock;
	private final ZMQ.Socket reqSock;
	private final boolean debug;
	private boolean connected;

	protected final String name;
	protected final List<String> peerNames;

	private static final String ANSI_RESET = "\u001B[0m";
	private static final String ANSI_BRIGHT = "\u001B[1m";
	private static final String ANSI_BLUE = "\u001B[34m";

	/**
	 * Handles messages received from the broker (which can originate in other
	 * nodes or in the broker itself)
	 */
	private class IncomingMessageHandler implements ZLoop.IZLoopHandler {
		@Override
		public int handle(ZLoop loop, ZMQ.PollItem item, Object arg_) {
			ChiNode arg = (ChiNode) arg_;

			// Unpack the message frames.
			ZMsg msgFrames = ZMsg.recvMsg(item.getSocket());

			assert (msgFrames.size() == 3);
			String msgDestination = msgFrames.popString();
			assert (msgDestination.equals(arg.name));

			// Second field is the empty delimiter
			msgFrames.pop();

			String json = msgFrames.popString();
			arg.logDebug("Received " + json);
			Message msg = Message.fromJson(json);
			handleMessage(msg);

			return 0;
		}
	}

	/**
	 * Handles replies received through the REQ socket. Typically, this will
	 * just be an acknowledgment of the message sent to the broker through the
	 * REQ socket, but can also be an error message.
	 */
	private class OutgoingMessageHandler implements ZLoop.IZLoopHandler {
		@Override
		public int handle(ZLoop loop, ZMQ.PollItem item, Object arg_) {
			// TODO: Check whether there is an error
			return 0;
		}
	}

	/**
	 * Handles timeouts so that nodes can guess when other nodes have failed
	 */
	private class TimeoutHandler implements ZLoop.IZLoopHandler {
		@Override
		public int handle(ZLoop loop, ZMQ.PollItem item, Object arg_) {
			handleTimeout();
			return 0;
		}
	}

	public ChiNode(CommandLineArgs args) {
		loop = new ZLoop();
		context = new ZContext();

		connected = false;

		// SUB socket for receiving messages from the broker
		subSock = context.createSocket(ZMQ.SUB);
		subSock.connect(args.pubEndpoint);

		// Make sure we get messages meant for us!
		subSock.subscribe(args.nodeName.getBytes());

		// Create handler for SUB socket
		ZMQ.PollItem sub = new ZMQ.PollItem(subSock, ZMQ.Poller.POLLIN);
		IncomingMessageHandler incomingMessageHandler = new IncomingMessageHandler();
		loop.addPoller(sub, incomingMessageHandler, this);

		// REQ socket for sending messages to the broker
		reqSock = context.createSocket(ZMQ.REQ);
		reqSock.connect(args.routerEndpoint);
		reqSock.setIdentity(args.nodeName.getBytes());

		// We don't strictly need a message handler for the REQ socket,
		// but we define one in case we ever receive any errors through it.
		ZMQ.PollItem req = new ZMQ.PollItem(reqSock, ZMQ.Poller.POLLOUT
				| ZMQ.Poller.POLLERR);
		OutgoingMessageHandler outgoingMessageHandler = new OutgoingMessageHandler();
		loop.addPoller(req, outgoingMessageHandler, this);

		name = args.nodeName;
		peerNames = args.peerNames;

		debug = args.debug;

		// Capture signals to ensure an orderly shutdown
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				loop.destroy();
				subSock.close();
				reqSock.close();
			}
		});
	}

	/**
	 * This function will be called once for every message that this node
	 * receives. It should process the message and send any responses back
	 * through the network.
	 */
	public abstract void handleMessage(Message msg);

	/**
	 * This function should perform any logic to handle a timeout event.
	 */
	public abstract void handleTimeout();

	/**
	 * Connects a node to the broker. Will only have an effect if the node is
	 * not yet connected to the broker.
	 */
	public void connectToBroker() {
		if (!connected) {
			connected = true;

			Message helloResponse = new Message("helloResponse");
			helloResponse.source = name;
			sendToBroker(helloResponse);
			log("Node is running");
		}
	}

	/**
	 * Returns true if this node is already connected to the broker.
	 */
	public boolean isConnectedToBroker() {
		return connected;
	}

	// Timeout functions
	public void setTimeout(int delay) {
		loop.addTimer(delay, 1 /* times */, new TimeoutHandler(), this);
	}

	public void resetTimeout(int delay) {
		loop.removeTimer(this);
		loop.addTimer(delay, 1 /* times */, new TimeoutHandler(), this);
	}

	// Logging functions
	public void log(String msg) {
		String logMsg = String.format(">>> %10s -- %s", this.name, msg);
		System.err.println(ANSI_BRIGHT + logMsg + ANSI_RESET);
	}

	public void logDebug(String msg) {
		if (this.debug) {
			String logMsg = String.format(">>> %10s -- %s", this.name, msg);
			System.err.println(ANSI_BLUE + logMsg + ANSI_RESET);
		}
	}

	/**
	 * Sends a message to the broker.
	 */
	public void sendToBroker(Message msg) {
		String json = msg.toJson();
		reqSock.send(json);
		logDebug("Sent: " + json);

		/*
		 * !!! Note that the ZeroMQ API guarantees that a message sent is
		 * actually received, unsegmented, by the peer for ZMQ_REQ sockets.
		 * However, it requires that a response be received before another
		 * message can be sent on that socket. As such, every message you send
		 * on the ZMQ_REQ socket will receive a response from the broker
		 * acknowledging the message (or, in some cases, indicating that an
		 * error happened).
		 */
		reqSock.recvStr();
	}

	/**
	 * Starts the ZeroMQ loop
	 */
	public void start() {
		this.loop.start();
	}
}
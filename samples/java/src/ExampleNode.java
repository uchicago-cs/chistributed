import java.util.HashMap;

import com.beust.jcommander.JCommander;

public class ExampleNode extends ChiNode {
	private HashMap<String, String> store;

	public ExampleNode(CommandLineArgs args) {
		super(args);

		// This node's data store
		store = new HashMap<>();
	}

	@Override
	public void handleMessage(Message msg) {
		if (msg.type.equals("get")) {
			handleGetMessage(msg);
		} else if (msg.type.equals("set")) {
			handleSetMessage(msg);
		} else if (msg.type.equals("dupl")) {
			handleDuplMessage(msg);
		} else if (msg.type.equals("hello")) {
			handleHelloMessage(msg);
		} else {
			log("Received unknown message type: " + msg.type);
		}
	}

	/**
	 * GET handler
	 */
	private void handleGetMessage(Message getMessage) {
		String key = getMessage.key;

		// Simulate that retrieving data takes time
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}

		// Get the value and send a response or, if there is no such key, send
		// an error.
		Message getResponse = new Message("getResponse");
		getResponse.id = getMessage.id;
		if (store.containsKey(key)) {
			String value = store.get(key);
			getResponse.key = key;
			getResponse.value = value;
		} else {
			String error = "No such key: " + key;
			getResponse.error = error;
		}
		sendToBroker(getResponse);
	}

	/**
	 * SET handler
	 */
	private void handleSetMessage(Message setMessage) {
		String key = setMessage.key;
		String value = setMessage.value;

		// Simulate that storing data takes time
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}

		// Store the value
		store.put(key, value);

		// And send a 'dupl' message to the peers with a replica of the value.
		for (String peer : peerNames) {
			Message duplMessage = new Message("dupl");
			duplMessage.destination = peer;
			duplMessage.key = key;
			duplMessage.value = value;
			sendToBroker(duplMessage);
		}

		// Finally, send response to broker
		Message setResponse = new Message("setResponse");
		setResponse.id = setMessage.id;
		setResponse.key = key;
		setResponse.value = value;
		sendToBroker(setResponse);
	}

	/**
	 * DUPL handler
	 */
	private void handleDuplMessage(Message duplMessage) {
		String key = duplMessage.key;
		String value = duplMessage.value;

		// Simulate that storing data takes time
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}

		// Set the value in our data store
		store.put(key, value);
	}

	/**
	 * HELLO handler
	 */
	private void handleHelloMessage(Message helloMessage) {
		connectToBroker();
	}

	/**
	 * This node currently does not use timeouts.
	 */
	@Override
	public void handleTimeout() {
	}

	public static void main(String[] args) {
		// Parse command line arguments
		CommandLineArgs commandLineArgs = new CommandLineArgs();
		new JCommander(commandLineArgs, args);

		// Create a node and run it
		ExampleNode node = new ExampleNode(commandLineArgs);
		node.start();
	}

}

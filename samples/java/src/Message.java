import java.util.List;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

/**
 * Represents a message that can be passed between two nodes in the Chistributed
 * system.
 */
public class Message {
	public final String type;
	// This field must be a list of strings for all of the builtin messages, but
	// a single string for all custom messages, so it's currently just set to be
	// an Object.
	public Object destination;
	public String source;
	public Integer id;
	public String key;
	public String value;
	public String error;

	// JSON serializer and deserializer.
	private static final Gson GSON = new Gson();

	public Message(String type) {
		this.type = type;
	}

	public String toJson() {
		return GSON.toJson(this);
	}

	public static Message fromJson(String json) {
		return GSON.fromJson(json, Message.class);
	}
}

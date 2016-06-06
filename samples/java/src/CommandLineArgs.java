import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;

/**
 * Command-line parameters
 */
public class CommandLineArgs {
	@Parameter(names = "--debug", description = "Debug mode")
	public boolean debug;

	@Parameter(names = "--pub-endpoint")
	public String pubEndpoint;

	@Parameter(names = "--router-endpoint")
	public String routerEndpoint;

	@Parameter(names = "--node-name")
	public String nodeName;

	@Parameter(names = "--peer")
	public List<String> peerNames = new ArrayList<>();
}
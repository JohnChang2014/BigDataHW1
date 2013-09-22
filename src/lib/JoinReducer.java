package lib;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      
	// Variables to aid the join process
	private String address, coordinates;

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		while (values.hasNext()) {
			String currValue = values.next().toString();
			String valueSplitted[] = currValue.split("~");

			if (valueSplitted[0].equals("ad")) {
				address = key + ", "+ valueSplitted[1].trim();
			} else if (valueSplitted[0].equals("co")) {
				coordinates = ", " + valueSplitted[1].trim();
			}
		}
		// pump final output to file
		if (address != null && coordinates != null) output.collect(new Text(address), new Text(coordinates));
		address = null;
		coordinates = null;
    }
}
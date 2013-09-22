package lib;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AddressMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private String address_key, address, fileTag="ad~";
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        
        URL oracle = new URL(line);
		BufferedReader in = new BufferedReader(new InputStreamReader(oracle.openStream()));

		String inputLine;
		String[] tmp;
		int n = 0;
        
		while ((inputLine = in.readLine()) != null) {
			n++;
			if (n == 1) continue;
			tmp = inputLine.split(",", 2);
			address_key = tmp[0].trim();
			address = tmp[1].trim();
			
			if (address_key != null && address_key.length() > 0) output.collect(new Text(address_key), new Text(fileTag + address));
		}
		in.close(); 
	}
}
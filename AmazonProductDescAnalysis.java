import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/***
 * This Map-Reduce code will go through every Amazon product in rfox12:products
 * It will then output data on the top-level JSON keys
 */
public class AmazonProductDescAnalysis extends Configured implements Tool {

	// Just used for logging
	protected static final Logger LOG = LoggerFactory.getLogger(AmazonProductDescAnalysis.class);

	// This is the execution entry point for Java programs
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(HBaseConfiguration.create(), new AmazonProductDescAnalysis(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length > 3) {
			System.err.println("Not more than 3 arguments (hdfs output path), got: " + args.length);
			return -1;
		}

		// Now we create and configure a map-reduce "job"
		Job job = Job.getInstance(getConf(), "AmazonProductDescAnalysis");
		job.setJarByClass(AmazonProductDescAnalysis.class);
		for (int i = 0; i < args.length; i += 1) {
			  if ("-skip".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
				// this demonstrates logging
				LOG.info("Added file to the distributed cache: " + args[i]);
			  }
			}
		// By default we are going to can every row in the table
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		// This helper will configure how table data feeds into the "map" method
		TableMapReduceUtil.initTableMapperJob("rfox12:products_10000", // input HBase table name
				scan, // Scan instance to control CF and attribute selection
				MapReduceMapper.class, // Mapper class
				Text.class, // Mapper output key
				IntWritable.class, // Mapper output value
				job, // This job
				true // Add dependency jars (keep this to true)
		);

		// Specifies the reducer class to used to execute the "reduce" method after
		// "map"
		job.setReducerClass(MapReduceReducer.class);

		// For file output (text -> number)
		FileOutputFormat.setOutputPath(job, new Path(args[0])); // The first argument must be an output path
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// What for the job to complete and exit with appropriate exit code
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MapReduceMapper extends TableMapper<Text, IntWritable> {

		private static final Logger LOG = LoggerFactory.getLogger(MapReduceMapper.class);

		// Here are some static (hard coded) variables
		private static final byte[] CF_NAME = Bytes.toBytes("cf"); // the "column family" name
		private static final byte[] QUALIFIER = Bytes.toBytes("product_data"); // the column name
		private final static IntWritable one = new IntWritable(1); // a representation of "1" which we use frequently

		private Counter rowsProcessed; // This will count number of products processed
		private JsonParser parser; // This gson parser will help us parse JSON
		private String input;
		private Set<String> patternsToSkip = new HashSet<String>();
		private boolean caseSensitive = false;
		// This setup method is called once before the task is started
		@Override
		protected void setup(Context context) throws IOException,
        InterruptedException{
			parser = new JsonParser();
			rowsProcessed = context.getCounter("AmazonProductDescAnalysis", "Rows Processed");
			if (context.getInputSplit() instanceof FileSplit) {
			  this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
			  this.input = context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
		    this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
		    if (config.getBoolean("wordcount.skip.patterns", false)) {
		  	  URI[] localPaths = context.getCacheFiles();
			parseSkipFile(localPaths[0]);
		    }
		}
		private void parseSkipFile(URI patternsURI) {
		  LOG.info("Added file to the distributed cache: " + patternsURI);
		  try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
			String pattern;
			while ((pattern = fis.readLine()) != null) {
			  patternsToSkip.add(pattern);
			}
			LOG.warn("Patterns skipping size:"+ patternsToSkip.size());
			LOG.warn("Patterns skipping:"+ patternsToSkip.toString());
			
		  } catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '"
				+ patternsURI + "' : " + StringUtils.stringifyException(ioe));
		  }
		}

		// This "map" method is called with every row scanned.
		@Override
		public void map(ImmutableBytesWritable rowKey, Result value, Context context)
				throws InterruptedException, IOException {
			try {
				// Here we get the json data (stored as a string) from the appropriate column
				String jsonString = new String(value.getValue(CF_NAME, QUALIFIER));

				// Now we parse the string into a JsonElement so we can dig into it
				JsonElement jsonTree = parser.parse(jsonString);

				JsonObject jsonObject = jsonTree.getAsJsonObject();

				String description = jsonObject.get("description").getAsString();
				//System.out.println("Product description before cleaning: " + description);

				if (description.startsWith("<")) {
					String pattern = ".*alt=\"([^\"]*)\".*";
					Pattern p = Pattern.compile(pattern);
					Matcher m = p.matcher(description);
					while (m.find()) {
						description = m.group(1);
					}
				}
				description = description.replaceAll("\\<.*?\\>", "").replaceAll("\\d\\.", "")
						.replaceAll("[^a-zA-Z ]", " ").replaceAll("(?U)\\s+", " ");
				// .replaceAll("[\\\\[\\\\](){}^=]", "").replaceAll("\\s*,\\s*", " ");
				// .replaceAll("\\d\\.", "").replaceAll("( =^ ^=)", "").replaceAll("(=^ ^=)",
				// "");

				//System.out.println("Product description after cleaning:" + description);

				ArrayList<String> allWords = Stream.of(description.toLowerCase().split("\\s+")).map(String::trim)
						.collect(Collectors.toCollection(ArrayList<String>::new));

				//System.out.println(
				//		"Product description words count before removing duplicates and stopwords: " + allWords.size());

				allWords = Stream.of(description.toLowerCase().split("\\s+")).map(String::trim)
						.filter(item -> !item.isEmpty()).distinct()
						.collect(Collectors.toCollection(ArrayList<String>::new));

				//System.out.println(
				//		"Product description words coung after removing duplicates and before removing stopwords: "
				//				+ allWords.size());
				/*URL path = AmazonProductDescAnalysis.class.getClass().getResource("/stopwords.txt");

				List<String> stopwords = Files.readAllLines(Paths.get(path.getPath().toString().substring(1)));

				allWords.removeAll(stopwords);

				System.out.println(
						"Product description words count after removing duplicates and stopwords: " + allWords.size());
				*/
				Iterator itr = allWords.iterator();
				while (itr.hasNext()) {
					String str=itr.next().toString();
					if (patternsToSkip.contains(str)) {
						LOG.warn("Skipping value: "+str);
					continue;
					}
					else{
					context.write(new Text(str), one);
					}
				}


				// Here we increment a counter that we can read when the job is done
				rowsProcessed.increment(1);
			} catch (Exception e) {
				LOG.error("Error in MAP process: " + e.getMessage(), e);
			}
		}
	}

	// Reducer to simply sum up the values with the same key (text)
	// The reducer will run until all values that have the same key are combined
	public static class MapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}

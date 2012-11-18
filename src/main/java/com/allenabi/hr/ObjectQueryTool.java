package com.allenabi.hr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * This MapReduce job will count the total number of NASDAQ or NYSE records stored within the
 * files of the given input directories. It will also skip the CSV header.
 * <p/>
 * It's meant to be an explicit example to show all the moving parts of a MapReduce job. Much of
 * the code is just copied from the models and mapper package.
 */
public class ObjectQueryTool extends Configured implements Tool {

    public static class ObjectFieldQueryMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final String inputString = value.toString();

            final Map map = mapper.readValue(inputString, Map.class);
            final String objectName = (String) map.get("name");
            final List<Map> fields = (List) map.get("fields");
            final StringBuilder sb = new StringBuilder();
            for (final Map field : fields) {
                final Object fieldName = field.get("name");
                if (sb.length() == 0) {
                    sb.append("SELECT ").append(objectName).append(".").append(fieldName);
                } else {
                    sb.append(", ").append(objectName).append(".").append(fieldName);
                }
            }
            sb.append(" FROM ").append(objectName).append(" LIMIT 7");
            context.write(new Text(objectName), new Text(sb.toString()));
        }

        private final ObjectMapper mapper = new ObjectMapper();
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 2) {
            System.err.println("Usage: " + getClass().getName() + " <input> <output>");
            System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        final Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(ObjectFieldQueryMapper.class);
//        job.setReducerClass(RecordCounterReducer.class);

        // The Nasdaq/NYSE data dumps comes in as a CSV file (text input), so we configure
        // the job to use this format.
        job.setInputFormatClass(TextInputFormat.class);

        // This is what the Mapper will be outputting to the Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // This is what the Reducer will be outputting
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Setting the input folder of the job
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Preparing the output folder by first deleting it if it exists
        final Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new ObjectQueryTool(), args);
        System.exit(result);
    }
}

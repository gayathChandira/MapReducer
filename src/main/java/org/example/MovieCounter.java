package org.example;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovieCounter {

    private static final Logger LOG = LoggerFactory.getLogger(MovieFilterMapper.class);
    public static class MovieFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text("filtered_movies");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Skip header line if present
            if (key.get() == 0)
                return;

            // Parse CSV line using Apache Commons CSV
            try (CSVParser csvParser = CSVFormat.DEFAULT.parse(new StringReader(value.toString()))) {
                for (CSVRecord record : csvParser) {
                    try {
                        // Check if the record has the expected number of columns
                        double popularity = Double.parseDouble(record.get(5));
                        double voteAverage = Double.parseDouble(record.get(7));
                        double voteCount = Double.parseDouble(record.get(8));

                        LOG.info("Popularity: "+ popularity+ " voteAverage: "+voteAverage+" voteCOunt: "+voteCount);
                        if (popularity > 500.0 && voteAverage > 8.0 && voteCount > 1000.0) {
                            context.write(outputKey, value);
                        }

                    } catch (Exception e) {
                        // Log or handle other exceptions within the loop
                        LOG.info("Error in loop: " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                // Log or handle the IOException during parsing
                LOG.info("Error parsing CSV record: " + e.getMessage());
            }
        }

    }

    public static class MovieCountReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;

            // Count the number of movies
            for (Text value : values) {
                count++;
            }
            LOG.info("+++++ Count : " + count);
            outputValue.set(Integer.toString(count));
            context.write(key, outputValue);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MovieFilter");

        job.setJarByClass(MovieCounter.class);
        job.setMapperClass(MovieFilterMapper.class);
        job.setReducerClass(MovieCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

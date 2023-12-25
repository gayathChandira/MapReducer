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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;

public class YearCounter {

    private static final Logger LOG = LoggerFactory.getLogger(MovieCounter.MovieFilterMapper.class);
    public static class MovieYearMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text outputKey = new Text();
        private final LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Skip header line if present
            if (key.get() == 0 ) {
                return;
            }

            try (CSVParser csvParser = CSVFormat.DEFAULT.parse(new StringReader(value.toString()))) {
                for (CSVRecord record : csvParser) {
                    try {
                        // Check if the record has the expected number of columns
                        String releaseDate = record.get(6);
                        String releaseYear = releaseDate.length() >= 4 ? releaseDate.substring(0, 4) : "Unknown";
                        outputKey.set(releaseYear);
                        context.write(outputKey, one);
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

    public static class MovieYearReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MovieYearCounter");

        job.setJarByClass(YearCounter.class);
        job.setMapperClass(MovieYearMapper.class);
        job.setReducerClass(MovieYearReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

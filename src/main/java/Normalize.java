import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: movieA:movieB \t relation
            //collect the relationship list for movieA
            String[] movie_relation = value.toString().trim().split("\t");
            String[] movies = movie_relation[0].split(":");
            String movieA = movies[0];
            String movieB = movies[1];
            String relation = movie_relation[1];

            //output:
            //key:movieA
            //value: movieB=relation
            context.write(new Text(movieA), new Text(movieB+"="+relation));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input:
            //key = movieA,
            //value: <movieB=relation, movieC=relation...>
            //normalize each unit of co-occurrence matrix
            // movieB=relation/sum
            int sum = 0;
            Map<String, Integer> movieRelations = new HashMap<String, Integer>();
            for (Text value:values){
                String[] data= value.toString().split("=");
                String movieB = data[0];
                int relation = Integer.parseInt(data[1]);
                sum += relation;
                movieRelations.put(movieB, relation);
            }
            // outputKey: movieB
            // outputValue: movieA=relation/sum
            for (Map.Entry<String, Integer> entry:movieRelations.entrySet()){
                String outputKey = entry.getKey();
                double normalized = (double)entry.getValue()/(double)sum;
                String outputValue = key.toString()+"="+normalized;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

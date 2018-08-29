import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class NullValueHandler {
    public static class NullMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            // value: user,movie,rating
            String[] user_move_rating = values.toString().split(",");
            if (user_move_rating.length<3){
                return;
            }
            double rating = Double.parseDouble(user_move_rating[2]);
            context.write(new Text(user_move_rating[0]), new DoubleWritable(rating));
        }
    }

    public static class NullReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        private int numOfMovies;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            numOfMovies = conf.getInt("movies", 1);
        }
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            //key: user
            //value: rating
            double sum=0.0;

            while (values.iterator().hasNext()){
                sum += values.iterator().next().get();
            }
            double average = sum/numOfMovies;
            context.write(key, new DoubleWritable(average));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.setInt("movies", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf);
        job.setMapperClass(NullMapper.class);
        job.setReducerClass(NullReducer.class);

        job.setJarByClass(NullValueHandler.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

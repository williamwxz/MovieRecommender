import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = userid \t movie1: rating, movie2: rating...
			String[] user_movieRating = value.toString().split("\t");
			if (user_movieRating.length<2){
			    return;
            }
            //output:
            //key: movieA:movieB
            //value: 1
            String[] movie_ratings = user_movieRating[1].split(",");
			for (int i=0; i<movie_ratings.length; i++){
                String movieA  = movie_ratings[i].split(":")[0];
			    for (int j=0; j<movie_ratings.length; j++){
			        String movieB = movie_ratings[j].split(":")[0];
			        String outputKey = movieA+":"+movieB;
			        context.write(new Text(outputKey), new IntWritable(1));
                }
            }
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key movie1:movie2
            //value = iterable<1, 1, 1>
            int sum = 0;
            while (values.iterator().hasNext()){
                sum += values.iterator().next().get();
            }
            context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}

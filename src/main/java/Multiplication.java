import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			String[] movieB_movieA_relation = value.toString().split("\t");
			//pass data to reducer
            context.write(new Text(movieB_movieA_relation[0]), new Text(movieB_movieA_relation[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: user,movie,rating
            String[] user_movie_rating = value.toString().split(",");
            context.write(new Text(user_movie_rating[1]), new Text(user_movie_rating[0]+":"+user_movie_rating[2]));
		}
    }

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication
            //values: relationship matrix or rating matrix
            //relationship matrix * rating matrix
            //output value = rating9 * relation1
            //output key = user4:movie1
            Map<String, Double> relations = new HashMap<String, Double>();
            Map<String, Double> ratings = new HashMap<String, Double>();
            for(Text value:values){
                String data = value.toString();
                if (data.contains("=")){
                    //from co occurence matrix
                    String[] movie_relation = data.split("=");
                    Double relation_val = Double.parseDouble(movie_relation[1]);
                    relations.put(movie_relation[0], relation_val);
                }else{
                    String[] user_ratings = data.split(":");
                    Double rating = Double.parseDouble(user_ratings[1]);
                    ratings.put(user_ratings[0], rating);
                }
            }
            for (Map.Entry<String, Double> entry:relations.entrySet()){
                String movieA = entry.getKey();
                double relation = entry.getValue();
                for (Map.Entry<String, Double> element:ratings.entrySet()){
                    String user = element.getKey();
                    double rating = element.getValue();

                    String outputKey = user+":"+movieA;
                    double outputValue = relation*rating;
                    context.write(new Text(outputKey), new DoubleWritable(outputValue));
                }
            }
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}

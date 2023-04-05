package weather.project;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WeatherData {

    public static class MaxTemperatureMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private static final float HOT_THRESHOLD = 40.0f;
        private static final float COLD_THRESHOLD = 10.0f;

        private Text date = new Text();
        private FloatWritable maxTemperature = new FloatWritable();
        private FloatWritable minTemperature = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            date.set(line.substring(6, 14)); // date

            float tempMax = Float.parseFloat(line.substring(39, 45).trim()); // MaxTemperature
            float tempMin = Float.parseFloat(line.substring(47, 53).trim()); // MinTemperature

            maxTemperature.set(tempMax);
            minTemperature.set(tempMin);

            if (tempMax > HOT_THRESHOLD) {
                // Hot day
                context.write(new Text(date + " Hot Day "), maxTemperature);
            }

            if (tempMin < COLD_THRESHOLD) {
                // Cold day
                context.write(new Text(date + " Cold Day "), minTemperature);
            }
        }
    }

    public static class MaxTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float maxTemperature = Float.MIN_VALUE;

            for (FloatWritable value : values) {
                float temperature = value.get();
                if (temperature > maxTemperature) {
                    maxTemperature = temperature;
                }
            }

            context.write(key, new FloatWritable(maxTemperature));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather-data");
        job.setJarByClass(WeatherData.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
		// set input
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
		// set output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		// add inputpath and outputpath
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
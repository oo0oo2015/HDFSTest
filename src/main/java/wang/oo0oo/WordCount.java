package wang.oo0oo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class WordCount {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //对数据进行打散
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //输入数据 hello world love work
            String line = value.toString();
            //对数据切分
            String[] words = line.split(" ");
            //写出<hello, 1>
            for (String w : words) {
                //写出reducer端
                context.write(new Text(w), new IntWritable(1));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //记录出现的次数
            int sum = 0;
            //累加求和输出
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    static class WordCountDriver {
        static void main() {
            System.out.println("====WordCount测试示例程序====");
            //设置root权限
            System.setProperty("HADOOP_USER_NAME", "root");
            //创建job任务
            Configuration conf = new Configuration();
            Job job = null;
            try {
                job = Job.getInstance(conf);

                //指定jar包位置
                job.setJarByClass(WordCountDriver.class);
                //关联使用的Mapper类
                job.setMapperClass(WordCountMapper.class);
                //关联使用的Reducer类
                job.setReducerClass(WordCountReducer.class);
                //设置Mapper阶段输出的数据类型
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
                //设置Reducer阶段输出的数据类型
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                //设置数据输入路径和文件名
                FileInputFormat.setInputPaths(job, new Path("/bbdir/bbout.txt"));
                //设置数据输出路径
                FileOutputFormat.setOutputPath(job, new Path("/root/out"));
                //提交任务
                boolean rs = job.waitForCompletion(true);
                //退出
                System.exit(rs ? 0 : 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

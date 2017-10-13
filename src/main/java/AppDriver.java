import combiner.BidCombiner;
import customtype.CustomKey;
import customtype.GroupComparator;
import mapper.MapSideJoinMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import partitioner.CustomPartitioner;
import reducer.BidReducer;


public class AppDriver extends Configured implements Tool {

    public int run (String[]args) throws Exception {

        if (args.length != 3) {
            System.err.printf("Usage: %s <input1> <input2> <outputfolder>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance();
        job.setJarByClass(AppDriver.class);
        job.setJobName("Bid price count per city");

        Configuration conf = new Configuration();
        job.addCacheFile(new Path(args[0]).toUri());
        //DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        FileSystem fs= FileSystem.get(conf);
        FileInputFormat.setInputDirRecursive(job,true);
        FileStatus[] status_list = fs.listStatus(new Path(args[1]));
        if(status_list != null){
            for(FileStatus status : status_list){
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(CustomPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setMapperClass(MapSideJoinMapper.class);
        job.setCombinerClass(BidCombiner.class);
        job.setReducerClass(BidReducer.class);

        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(6);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new AppDriver(), args);
        System.exit(res);
    }
}

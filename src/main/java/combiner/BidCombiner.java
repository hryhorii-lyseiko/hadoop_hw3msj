package combiner;

import customtype.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BidCombiner extends Reducer<CustomKey,IntWritable,CustomKey,IntWritable> {

    @Override
    protected void reduce(CustomKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer count = 0;

        for(IntWritable value: values) {
                count += value.get();
                        }

        context.write(key,new IntWritable(count));
    }
}

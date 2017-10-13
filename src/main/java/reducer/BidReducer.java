package reducer;

import customtype.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class BidReducer extends Reducer<CustomKey,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(CustomKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer count=0;

        for(IntWritable value: values) {
            count += value.get();
        }

        context.write(new Text("OS Name = " + key.getOSType()  + "\tCity Name = " +key.getCityName() + "\tAmount event per city = "),new IntWritable(count));
    }

}

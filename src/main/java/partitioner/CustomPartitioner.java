package partitioner;

import customtype.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomKey, IntWritable> {

    @Override
    public int getPartition(CustomKey key, IntWritable value, int numPartitions) {

        String OSType = key.getOSType().toString().toLowerCase();
        if(OSType == null){
            return (key.getCityName().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
        if(OSType.contains("linux")) {
            return 0;
        }
        if(OSType.contains("windows xp")){
            return 1;
        }
        if(OSType.contains("android")){
            return 2;
        }
        if(OSType.contains("mac os x")){
            return 3;
        }
        if(OSType.contains("ios")){
            return 4;
        }

        return 5;
    }
}
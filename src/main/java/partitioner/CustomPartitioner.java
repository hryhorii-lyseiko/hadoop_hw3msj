package partitioner;

import customtype.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomKey, IntWritable> {

    @Override
    public int getPartition(CustomKey key, IntWritable value, int numPartitions) {
        int res = 0;
        String OSType = key.getOSType().toString().toLowerCase();
        if(OSType == null){
            return (key.getCityName().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }else if(OSType.contains("linux") || OSType.contains("ubuntu")) {
            res = 0;
        }else if(OSType.contains("windows")){
            res = 1;
        }else if(OSType.contains("android")){
            res = 2;
        }else if(OSType.contains("mac os x")){
            res = 3;
        }else if(OSType.contains("ios")){
            res = 4;
        }else res = 5;

        return res;
    }
}
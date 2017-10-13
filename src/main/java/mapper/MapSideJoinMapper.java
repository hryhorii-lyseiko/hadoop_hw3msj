package mapper;

import customtype.CustomKey;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, CustomKey, IntWritable> {

    private  HashMap<String, String> CustIdOrderMap ;
   // private BufferedReader brReader;
    private String os_type = "";
    private String CityName = "";
    private FileSystem fs;

    enum MYCOUNTER {
        RECORD_COUNT
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        fs = FileSystem.get(context.getConfiguration());
        CustIdOrderMap = new HashMap<>();
        URI[] cacheFiles = context.getCacheFiles();
        if(cacheFiles != null) {
            for(URI uri : cacheFiles) {
                setupOrderHashMap(new Path(uri.getPath()));
            }
        }
         }
    private void setupOrderHashMap(Path filePath) throws IOException {
        FSDataInputStream is = fs.open(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String idAndCity;
        while((idAndCity = bufferedReader.readLine()) != null) {
            String[] sections = idAndCity.split("\t");
            if(sections.length == 2){
                CustIdOrderMap.put(sections[0], sections[1]);
            }
        }
        is.close();

    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
        String[] rows = value.toString().split("\n");
        for(String sections : rows){

            int bid = 0;
            if (sections.toString().length() > 0) {
                context.getCounter("if", "first").increment(1);
                String custDataArr[] = sections.toString().split("\t");


                CityName = CustIdOrderMap.get(custDataArr[7]);
                bid = Integer.parseInt(custDataArr[19]);
                UserAgent userAgent = new UserAgent(custDataArr[4]);
                os_type = userAgent.getOperatingSystem().getName();
                context.getCounter("size", String.valueOf(CustIdOrderMap.size())).increment(1);


            }
            if (bid >= 250 && CityName != null && os_type != null) {
                context.getCounter("if", "second").increment(1);
                context.write(new CustomKey(new Text(CityName), new Text( os_type)), new IntWritable(1));
            }
        }


    }
}

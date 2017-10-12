package mapper;

import customtype.CustomKey;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, CustomKey, IntWritable> {

    private static HashMap<String, String> CustIdOrderMap = new HashMap<>();
    private BufferedReader brReader;
    private String os_type = "";
    private String CityName = "";

    enum MYCOUNTER {
        RECORD_COUNT
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                for(Path stopWordFile : stopWordsFiles) {
                    setupOrderHashMap(stopWordFile);
                }
            }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private void setupOrderHashMap(Path filePath)
            throws IOException {

        String strLineRead = "";

        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));

            while ((strLineRead = brReader.readLine()) != null) {
                String custIdCityArr[] = strLineRead.toString().split("\t");
                CustIdOrderMap.put(custIdCityArr[0].trim(), custIdCityArr[1].trim());
            }

        } catch (IOException e) {

            e.printStackTrace();
        }finally {
            if (brReader != null) {
                brReader.close();

            }

        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
        int bid = 0;
        if (value.toString().length() > 0) {
            String custDataArr[] = value.toString().split("\t");

            try {
                CityName = CustIdOrderMap.get(custDataArr[7].toString());
                bid = Integer.parseInt(custDataArr[19].toString());
                UserAgent userAgent = new UserAgent(custDataArr[4].toString());
                os_type = userAgent.getOperatingSystem().getName().toString();
            } finally {
                CityName = ((CityName.equals(null) || CityName
                        .equals("")) ? "NOT-FOUND" : CityName);
            }



        }
        if (bid >= 250) {
            context.write(new CustomKey(new Text(CityName), new Text( os_type)), new IntWritable(1));
        }
        CityName = "";
    }
}

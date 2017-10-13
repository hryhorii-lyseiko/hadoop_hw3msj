import combiner.BidCombiner;
import customtype.CustomKey;
import customtype.GroupComparator;
import mapper.MapSideJoinMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.BidReducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class MRTest {

    MapDriver<LongWritable, Text, CustomKey, IntWritable> mapDriver;
    ReduceDriver<CustomKey,IntWritable,Text,IntWritable> reduceDriver;
    ReduceDriver<CustomKey,IntWritable,CustomKey,IntWritable> combineDriver;
    MapReduceDriver<LongWritable, Text, CustomKey,IntWritable,Text,IntWritable> mapReduceDriver;


    @Before
    public void setUp() throws URISyntaxException {

        MapSideJoinMapper mapper = new MapSideJoinMapper();
        GroupComparator comparator = new GroupComparator();
        BidCombiner combiner = new BidCombiner();
        BidReducer reducer = new BidReducer();

        String mockedPathToDistributedCache = "src/main/resources/city";

        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.withCacheFile(new URI(mockedPathToDistributedCache));

        combineDriver = ReduceDriver.newReduceDriver(combiner);

        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setKeyGroupingComparator(comparator);
        mapReduceDriver.setCombiner(combiner);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.withCacheFile(new URI(mockedPathToDistributedCache));

    }


    @Test
    public void dataMapperTest1() throws IOException {
        Text text = new Text("c7636522cc1433b8929b705a80564a11\t20130312180203005\t" +
                "1\t18239e0c09c4d6481af855fa1328fe53\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t" +
                "221.1.19.*\t146\t149\t1\t5F97t5E0BTK7XhNrUMpENpn\t88855d41f70e2b51e52becb5d8305367\tnull\tmm_10027070_118039_10308280\t160\t600\t2\t1\t0\t" +
                "36391fa23e0928a93cd51ea8af344b82\t300\t116\tdf6f61b2409f4e2f16b6873a7eb50444\n");
        mapDriver.withInput(new LongWritable(1), text);
        mapDriver.withOutput(new CustomKey(new Text("zibo"), new Text("Windows XP")), new IntWritable(1));
        mapDriver.runTest();
    }
    @Test
    public void dataMapperTest2() throws IOException {
        Text text = new Text("e3100b0c47fa4a4d460ae9f3a3efd138\t20130312180209350\t1\tae4438961ad8f9dee2037e17ee0c9b91\t" +
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/536.26.17 (KHTML, like Gecko) Version/6.0.2 Safari/536.26.17\t" +
                "114.238.96.*\t80\t88\t1\te1F0t19ogqNhjqKbuKz\td638f96b03a2707f0dedae9fea1154c7\tnull\tmm_10032051_2374052_9577342\t950\t90\t2\t1\t0\t" +
                "8f86b069dbb8898e2beca5ec3030e147\t300\t101\tdf6f61b2409f4e2f16b6873a7eb50444\n");
        mapDriver.withInput(new LongWritable(1), text);
        mapDriver.withOutput(new CustomKey(new Text("huaian"), new Text("Mac OS X")), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void dataMapperTest3() throws IOException {
        Text text = new Text("4575dbdaf80140728e8a8076cfaf7980\t20130312001410528\t1\t1f97526717d8317178244bab0bec852b\t" +
                "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X; zh-cn) AppleWebKit/534.46.0 (KHTML, like Gecko) CriOS/21.0.1180.82 Mobile/10A403 Safari/7534.48.3\t" +
                "155.69.2.*\t0\t0\t3\tersbQv1RdoTy1m58uG\t6b15273f500688b75e47e6c2c8079c93\tnull\tSports_F_Rectangle\t300\t250\t0\t0\t50\tf206493d1a82b7d977075b20b7afd5f4\t" +
                "300\t120\tdf6f61b2409f4e2f16b6873a7eb50444");
        mapDriver.withInput(new LongWritable(1), text);
        mapDriver.withOutput(new CustomKey(new Text("unknown"), new Text("iOS")), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void dataMapperTest4() throws IOException {
        Text text = new Text("9c89ab5dbf644af0a9d2985a3f705210\t20130312001425679\t1\t135475deea00eeefaaf3b57650ca5407\t" +
                "Mozilla/5.0 (Linux; U; Android 2.3.5; zh-cn; GT-I9100 Build/GINGERBREAD) AppleWebKit/533.1 (KHTML, like Gecko) FlyFlow/2.3 Version/4.0 Mobile Safari/533.1 baidubrowser/042_1.41.3.2_diordna_008_084/gnusmas_01_5.3.2_0019I-TG/7400005o/764E6BC3FDFCF5389A5E5D43DAE85BBD%7C378163640074753/1\t" +
                "112.96.131.*\t216\t222\t3\tersbQv1RdoTy1m58uG\t13d975bcb425ee8b0a8726f43f3ec0ad\tnull\tSports_F_Rectangle\t300\t250\t0\t0\t50\tf206493d1a82b7d977075b20b7afd5f4\t300\t120\t" +
                "df6f61b2409f4e2f16b6873a7eb50444\n");
        mapDriver.withInput(new LongWritable(1), text);
        mapDriver.withOutput(new CustomKey(new Text("foshan"), new Text("Android")), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void dataMapperTest5() throws IOException {
        Text text = new Text("4152f15b1c90adbe6d4a3a034cad317f\t20130312001703951\t1\tef14e2fe1ec3a643f62cc71096538222\t" +
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.123 Safari/537.22\t1.83.68.*\t" +
                "333\t334\t3\tDD1SqS9rg5scFsf\ta228a40b44610e06f6c761afb4ef8a0b\tnull\tAuto_Width2\t960\t90\t0\t0\t20\t475d63a4a414634cb5b95dc826a6258f\t" +
                "300\t20\t9f4e2f16b6873a7eb504df6f61b24044\n");
        mapDriver.withInput(new LongWritable(1), text);
        mapDriver.withOutput(new CustomKey(new Text("xian"), new Text("Linux")), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void dataCombinerTest() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(4));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        combineDriver.withInput(new CustomKey(new Text("lasa"), new Text("Windows")), values);
        combineDriver.withOutput(new CustomKey(new Text("lasa"),new Text("Windows")), new IntWritable(8));
        combineDriver.runTest();
    }

    @Test
    public void dataReducerTest() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new CustomKey(new Text("lasa"), new Text("Windows")), values);
        reduceDriver.withOutput(new Text("OS Name = Windows\tCity Name = lasa\tAmount event per city = "), new IntWritable(5));
        reduceDriver.runTest();
    }

    @Test
    public void generalMRTest() throws IOException {
        Text testData = new Text("4152f15b1c90adbe6d4a3a034cad317f\t20130312001703951\t1\tef14e2fe1ec3a643f62cc71096538222\t" +
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.123 Safari/537.22\t1.83.68.*\t" +
                "333\t334\t3\tDD1SqS9rg5scFsf\ta228a40b44610e06f6c761afb4ef8a0b\tnull\tAuto_Width2\t960\t90\t0\t0\t20\t475d63a4a414634cb5b95dc826a6258f\t" +
                "300\t20\t9f4e2f16b6873a7eb504df6f61b24044\n");
        mapReduceDriver.withInput(new LongWritable(2), testData);
        mapReduceDriver.withOutput(new Text("OS Name = Linux\tCity Name = xian\tAmount event per city = "), new IntWritable(1));

        mapReduceDriver.runTest();
    }
}

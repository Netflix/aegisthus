package com.netflix.aegisthus.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.netflix.aegisthus.mapred.reduce.CassReducer;

public class CassReducerTest {
    private Text t(String text) {
        return new Text(text);
    }

    @DataProvider
    public Object[][] values() {
        return new Object[][] {

                {	"{\"uid\":",
                        new String[] {	"{\"uid\": {\"deletedAt\": -9223372036854775808, \"columns\": [[\"CELL\",\"00000002\",1312400480243000], [\"ENABLED\",\"59\",1312400420243004]]}}",
                                "{\"uid\": {\"deletedAt\": 10, \"columns\": [[\"CELL\",\"00000000\",1312400475129000], [\"ENABLED\",\"59\",1312400475129004]]}}",
                                "{\"uid\": {\"deletedAt\": 0, \"columns\": [[\"CELL\",\"00000000\",1312400473649000], [\"\\\\N\",\"59\",1312400533649004]]}}" },
                        "{\"uid\": {\"deletedAt\": 10, \"columns\": [[\"CELL\",\"00000002\",1312400480243000], [\"ENABLED\",\"59\",1312400475129004], [\"\\\\N\",\"59\",1312400533649004]]}}" },
                {	"{\"uid\":",
                        new String[] {	"{\"uid\": {\"deletedAt\": -9223372036854775808, \"columns\": [[\"cell\",\"00000002\",1]]}}",
                                "{\"uid\": {\"deletedAt\": 10, \"columns\": []}}" },
                        "{\"uid\": {\"deletedAt\": 10, \"columns\": []}}" }

        };

    }

    @SuppressWarnings({ "unchecked" })
    @Test(dataProvider = "values")
    public void reduce(String keyString, String[] lists, String result) throws IOException, InterruptedException {
        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, Text, Text, Text>.Context context = (Reducer.Context) support.createMock(Context.class);
        Text key = t(keyString);

        context.write(EasyMock.eq(key), EasyMock.eq(new Text(result)));

        List<Text> values = new ArrayList<Text>();
        for (String value : lists) {
            values.add(t(value));

        }
        CassReducer reducer = new CassReducer();
        support.replayAll();
        reducer.reduce(key, values, context);
        support.verifyAll();
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void reducekeys() throws IOException, InterruptedException {
        Text key1 = new Text("{\"uid1\": {\"test1\":");
        Text key2 = new Text("{\"uid2\": {\"test2\":");
        Text value1 = new Text("{\"uid1\": {\"test1\": {\"deletedAt\": 1, \"columns\": [[\"cell1\",\"00000001\",1312400480243001]]}, \"deletedAt\": 1}}");
        Text value2 = new Text("{\"uid2\": {\"test2\": {\"deletedAt\": 2, \"columns\": [[\"cell2\",\"00000002\",1312400480243002]]}, \"deletedAt\": 2}}");

        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, Text, Text, Text>.Context context = (Reducer.Context) support.createMock(Context.class);

        context.write(EasyMock.eq(key1), EasyMock.eq(new Text(value1.toString())));
        context.write(EasyMock.eq(key2), EasyMock.eq(new Text(value2.toString())));

        List<Text> values1 = new ArrayList<Text>();
        values1.add(value1);
        List<Text> values2 = new ArrayList<Text>();
        values2.add(value2);
        CassReducer reduce = new CassReducer();
        support.replayAll();
        reduce.reduce(key1, values1, context);
        reduce.reduce(key2, values2, context);

        support.verifyAll();
    }

    private static class Data implements Iterable<Text>, Iterator<Text> {
        static int count = 0;

        @Override
        public boolean hasNext() {
            return count < 2;
        }

        @Override
        public Text next() {
            if (count++ == 0) {
                return new Text("{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"cell1\",\"00000001\",1312400480243001]]}}");
            } else {
                return new Text("{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"cell2\",\"00000001\",1312400480243001]]}}");
            }
        }

        @Override
        public void remove() {
            throw new UnknownError();
        }

        @Override
        public Iterator<Text> iterator() {
            return this;
        }
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void reference() throws IOException, InterruptedException {
        Text key1 = new Text("{\"uid1\"");

        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, Text, Text, Text>.Context context = (Reducer.Context) support.createMock(Context.class);

        context
                .write(	EasyMock.eq(key1),
                        EasyMock
                                .eq(new Text("{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"cell1\",\"00000001\",1312400480243001], [\"cell2\",\"00000001\",1312400480243001]]}}")));

        CassReducer reduce = new CassReducer();
        support.replayAll();
        reduce.reduce(key1, new Data(), context);

        support.verifyAll();
    }

    /**
     * We must escape \ all the way through reducers, but due to the way that we
     * process sstables this only affects column names for now
     */
    @SuppressWarnings({ "unchecked" })
    @Test
    public void escapecharacters() throws IOException, InterruptedException {
        Text key1 = new Text("key1");
        Text value1 = new Text("{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"\\\\N\",\"\",1312400480243002], [\"cell2\",\"00000001\",1312400480243002]]}}");
        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, Text, Text, Text>.Context context = (Reducer.Context) support.createMock(Context.class);

        context.write(EasyMock.eq(key1), EasyMock.eq(value1));

        List<Text> values1 = new ArrayList<Text>();
        values1.add(value1);
        values1
                .add(new Text("{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"\\\\N\",\"\",1312400480243002], [\"cell2\",\"\\\\N\",1312400480243001]]}}"));
        CassReducer reduce = new CassReducer();
        support.replayAll();
        reduce.reduce(key1, values1, context);

        support.verifyAll();
    }
}

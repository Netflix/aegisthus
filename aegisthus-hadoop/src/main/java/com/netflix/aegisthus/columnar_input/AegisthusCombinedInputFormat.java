package com.netflix.aegisthus.columnar_input;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.aegisthus.columnar_input.splits.AegCombinedSplit;
import com.netflix.aegisthus.columnar_input.splits.AegSplit;

/**
 * This class takes the splits created by the AegisthusInputFormat and combines
 * small SSTables into single splits.
 */
public class AegisthusCombinedInputFormat extends AegisthusInputFormat {
    private static final Log LOG = LogFactory.getLog(AegisthusCombinedInputFormat.class);
    private static final int MAX_SPLIT_SIZE = 104857600;

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);
        List<InputSplit> combinedSplits = Lists.newArrayList();
        Map<String, AegCombinedSplit> map = Maps.newHashMap();
        int cnt = 0;
        int cntAdded = 0;

        top:
        for (InputSplit split : splits) {
            AegSplit aegSplit = (AegSplit) split;
            switch (aegSplit.getType()) {
            case sstable:
                cnt++;
                if (aegSplit.getLength() >= MAX_SPLIT_SIZE) {
                    cntAdded++;
                    combinedSplits.add(aegSplit);
                    continue;
                }
                try {
                    String lastLocation = null;
                    for (String location : aegSplit.getLocations()) {
                        lastLocation = location;
                        if (map.containsKey(location)) {
                            AegCombinedSplit temp = map.get(location);
                            cntAdded++;
                            temp.getSplits().add(aegSplit);
                            if (temp.getLength() >= MAX_SPLIT_SIZE) {
                                combinedSplits.add(temp);
                                map.remove(location);
                            }
                            continue top;
                        }
                    }
                    cntAdded++;
                    AegCombinedSplit temp = new AegCombinedSplit(aegSplit);
                    map.put(lastLocation, temp);

                } catch (InterruptedException e) {
                    throw new IOException(e);
                }

                break;
            default:
                combinedSplits.add(aegSplit);
            }
        }
        for (AegCombinedSplit split : map.values()) {
            combinedSplits.add(split);
        }

        LOG.info(String.format("sstable AegSplits: %d", cnt));
        LOG.info(String.format("sstables Added AegSplits: %d", cntAdded));
        LOG.info(String.format("other AegSplits: %d", splits.size() - cnt));
        LOG.info(String.format("AegCombinedSplits: %d", combinedSplits.size()));
        return combinedSplits;
    }

}

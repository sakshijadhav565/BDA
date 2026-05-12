import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Integer> map = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for(IntWritable val : values) {
            sum += val.get();
        }

        map.put(key.toString(), sum);
    }

    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        List<Map.Entry<String, Integer>> list =
                new ArrayList<>(map.entrySet());

        // Descending order = MOST frequent
        list.sort((a,b) -> b.getValue() - a.getValue());

        int k = 3;

        for(int i=0; i<k && i<list.size(); i++) {

            context.write(
                    new Text(list.get(i).getKey()),
                    new IntWritable(list.get(i).getValue())
            );
        }
    }
}

// list.sort((a,b) -> a.getValue() - b.getValue()); for K least frequent
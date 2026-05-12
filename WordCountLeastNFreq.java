import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountLeastNFreq {

    // =========================
    // MAPPER
    // =========================

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one =
                new IntWritable(1);

        private Text word = new Text();

        public void map(Object key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr =
                    new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {

                word.set(
                        itr.nextToken().toLowerCase()
                );

                context.write(word, one);
            }
        }
    }

    // =========================
    // REDUCER
    // =========================

    public static class IntSumReducer
            extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        // Store all word frequencies
        private Map<String, Integer> wordMap =
                new HashMap<>();

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {

                sum += val.get();
            }

            // Store word and frequency
            wordMap.put(key.toString(), sum);
        }

        // =========================
        // PRINT LEAST N WORDS
        // =========================

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // CHANGE N HERE
            int N = 5;

            // Convert map to list
            List<Map.Entry<String, Integer>> list =
                    new ArrayList<>(wordMap.entrySet());

            // Sort ascending by frequency
            Collections.sort(
                    list,
                    (a, b) -> a.getValue() - b.getValue()
            );

            context.write(
                    new Text("Least " + N + " Frequent Words"),
                    new IntWritable(0)
            );

            // Print Least N Words
            for (int i = 0;
                 i < N && i < list.size();
                 i++) {

                Map.Entry<String, Integer> entry =
                        list.get(i);

                context.write(
                        new Text(
                                (i + 1) + ". " +
                                        entry.getKey()
                        ),
                        new IntWritable(
                                entry.getValue()
                        )
                );
            }
        }
    }

    // =========================
    // DRIVER CODE
    // =========================

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Least N Frequent Words"
        );

        job.setJarByClass(
                WordCountLeastNFreq.class
        );

        job.setMapperClass(
                TokenizerMapper.class
        );

        job.setReducerClass(
                IntSumReducer.class
        );
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(
                IntWritable.class
        );

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}

/*# 1. Open VS Code terminal

cd Desktop

mkdir BDA

cd BDA


# 2. Check Java

java -version

echo $JAVA_HOME


# 3. If Java 8 not set

nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh


# 4. Check Hadoop

hadoop version


# 5. Start Hadoop

start-dfs.sh

start-yarn.sh


# 6. Check services

jps


# 7. If Hadoop not working

stop-all.sh

start-all.sh


# If still issue

hdfs namenode -format

start-all.sh


# Leave safemode

hdfs dfsadmin -safemode leave


# 8. Create Java file

nano WordCountLeastNFreq.java


# 9. Create input file

nano words.txt


# 10. Set classpath

export HADOOP_CLASSPATH=$(hadoop classpath)


# 11. Create HDFS folder

hdfs dfs -mkdir /exp4


# 12. Upload file to HDFS

hdfs dfs -put words.txt /exp4


# 13. Verify file

hdfs dfs -ls /exp4


# 14. Remove old classes

rm -rf classes

mkdir classes


# 15. Compile program

javac -classpath $(hadoop classpath) \
-d classes WordCountLeastNFreq.java


# 16. Check class files

ls classes


# 17. Create jar

jar -cvf exp4.jar -C classes/ .


# 18. Check jar

ls


# 19. Remove old output

hdfs dfs -rm -r /output/leastfreq


# 20. Run program

hadoop jar exp4.jar \
WordCountLeastNFreq \
/exp4/words.txt \
/output/leastfreq


# 21. View output

hdfs dfs -cat /output/leastfreq/part-r-00000


# 22. Check output files

hdfs dfs -ls /output/leastfreq


# 23. Stop Hadoop

stop-dfs.sh

stop-yarn.sh
*/
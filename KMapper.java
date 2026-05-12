import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words = value.toString().split(" ");

        for(String w : words) {
            word.set(w);
            context.write(word, one);
        }
    }
}

// # ================= PRE-REQUISITES =================

// # 1. Check Java Version

// java -version

// # Java 8 should come


// # 2. Check JAVA_HOME

// echo $JAVA_HOME

// # Java 8 path should come


// # 3. If Java 8 is not set

// nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh

// # Set JAVA_HOME to Java 8 path


// # 4. Check Hadoop Version

// hadoop -version


// # ================= START HADOOP =================

// # 5. Start DFS and YARN

// start-dfs.sh
// start-yarn.sh


// # 6. Check Hadoop Processes

// jps

// # Should see:
// # NameNode
// # DataNode
// # SecondaryNameNode
// # ResourceManager
// # NodeManager


// # 7. If Hadoop Not Working

// stop-all.sh
// start-all.sh

// # If still issue:

// hdfs namenode -format

// # Leave safemode

// hdfs dfsadmin -safemode leave


// # ================= CREATE INPUT FILE =================

// # 8. Create Input File

// nano input.txt


// # ================= CREATE JAVA FILES =================

// # 9. Create Java Files

// nano KMapper.java
// nano KReducer.java
// nano KDriver.java


// # ================= SET HADOOP CLASSPATH =================

// # 10. Set Classpath

// export HADOOP_CLASSPATH=$(hadoop classpath)


// # ================= COMPILE JAVA FILES =================

// # 11. Compile Files

// hadoop com.sun.tools.javac.Main KMapper.java KReducer.java KDriver.java


// # ================= CREATE JAR FILE =================

// # 12. Create Jar

// jar cf kfreq.jar *.class


// # ================= HDFS COMMANDS =================

// # 13. Create Input Folder in HDFS

// hdfs dfs -mkdir /input


// # 14. Upload File to HDFS

// hdfs dfs -put input.txt /input


// # 15. Check File in HDFS

// hdfs dfs -ls /input


// # ================= RUN PROGRAM =================

// # 16. Delete Old Output Folder

// hdfs dfs -rm -r /output


// # 17. Run Hadoop Program

// hadoop jar kfreq.jar KDriver /input /output


// # ================= VIEW OUTPUT =================

// # 18. View Output

// hdfs dfs -cat /output/part-r-00000


// # ================= STOP HADOOP =================

// # 19. Stop Hadoop

// stop-yarn.sh
// stop-dfs.sh
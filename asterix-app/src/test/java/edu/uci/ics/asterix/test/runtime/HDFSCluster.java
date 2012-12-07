package edu.uci.ics.asterix.test.runtime;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;

@SuppressWarnings("deprecation")
public class HDFSCluster {

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final int nameNodePort = 31888;
    private static final String DATA_PATH = "data/hdfs";
    private static final String HDFS_PATH = "/asterix";
    private static final HDFSCluster INSTANCE = new HDFSCluster();

    private MiniDFSCluster dfsCluster;
    private int numDataNodes = 2;
    private JobConf conf = new JobConf();
    private FileSystem dfs;

    public static HDFSCluster getInstance() {
        return INSTANCE;
    }

    private HDFSCluster() {

    }

    public void setup() throws Exception {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        cleanupLocal();
        dfsCluster = new MiniDFSCluster(nameNodePort, conf, numDataNodes, true, true, StartupOption.REGULAR, null);
        dfs = FileSystem.get(conf);
        loadData();
    }

    private void loadData() throws IOException {
        Path destDir = new Path(HDFS_PATH);
        File srcDir = new File(DATA_PATH);
        File[] listOfFiles = srcDir.listFiles();
        for (File srcFile : listOfFiles) {
            Path path = new Path(srcFile.getAbsolutePath());
            dfs.copyFromLocalFile(path, destDir);
        }
    }

    private void cleanupLocal() throws IOException {
        // cleanup artifacts created on the local file system
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
    }

    public void cleanup() throws Exception {
        dfsCluster.shutdown();
        cleanupLocal();
    }

    public static void main(String[] args) throws Exception {
        HDFSCluster cluster = new HDFSCluster();
        cluster.setup();
        JobConf conf = configureJobConf();
        FileSystem fs = FileSystem.get(conf);
        InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, 0);
        for(InputSplit split : inputSplits){
            System.out.println("split :" + split);
        }
        //   cluster.cleanup();
    }

    private static JobConf configureJobConf() throws Exception {
        JobConf conf = new JobConf();
        String hdfsUrl = "hdfs://127.0.0.1:31888";
        String hdfsPath = "/asterix/extrasmalltweets.txt";
        conf.set("fs.default.name", hdfsUrl);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(HDFSAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", hdfsPath);
        conf.set("mapred.input.format.class", "org.apache.hadoop.mapred.TextInputFormat");
        return conf;
    }

}

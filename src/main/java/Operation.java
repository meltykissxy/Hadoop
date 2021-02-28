import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Operation {
    private FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://39.103.160.70:8020");
        Configuration configuration = new Configuration();
        String user = "atguigu";
        FileSystem fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    //最基础的部分。user是hdfs识别你身份的
    @Test
    public void basic() throws IOException {
        fs.mkdirs(new Path("/beijing/xichengqu"));
    }

    @Test
    public void play0() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://39.103.160.70:8020");
        Configuration configuration = new Configuration();
        String user = "atguigu";
        FileSystem fs = FileSystem.get(uri, configuration, user);
        fs.copyFromLocalFile(new Path("/Users/apple/Downloads/Apple/Beauty/love.sql"), new Path("/beijing/xichengqu"));
        fs.close();
    }
}

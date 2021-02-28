import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;

public class WordProducer {

    private static final String[] words = new String[]{"January", "February", "March", "April",
            "May", "June", "July", "August", "September", "October", "November", "December"};

    public static void main(String[] args) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9820"), new Configuration(), "atguigu");
        BufferedOutputStream fos = new BufferedOutputStream(fileSystem.create(new Path("/word.txt")));
        for (int i = 0; i < 100000000; i++) {
            fos.write((words[(int) (Math.random() * words.length)] + " ").getBytes());
            if ((i+1) % 10 == 0) {
                fos.write('\n');
            }
        }

        fos.close();
    }
}

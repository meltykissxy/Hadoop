import com.apple.wordcount.WcDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Driver {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WcDriver(), args);
    }
}

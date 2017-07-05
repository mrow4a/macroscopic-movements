package movements;

import com.bluebreezecf.tools.sparkjobserver.api.*;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.net.URLConnection;
import java.net.URL;

import static spark.Spark.*;

public class Main {
    private static final String defaultSparkJobHost = "localhost";
    private static final String defaultSparkJobPort = "8090";
    private static String endpoint = String.format("http://%s:%s/", defaultSparkJobHost, defaultSparkJobPort);

    public static void main(String[] args) {

        try {
            ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(endpoint);

            //POST /jars/<appName>
            InputStream jarFileStream = ClassLoader.getSystemResourceAsStream("wordcount.jar");

            String appName = "spark-test";
            boolean isUploaded = client.uploadSparkJobJar(jarFileStream, appName);
            System.out.println("Uploaded: " + isUploaded);

            //POST /contexts/<name>--Create context with parameters
            Map<String, String> params = new HashMap<String, String>();
            params.put(ISparkJobServerClientConstants.PARAM_MEM_PER_NODE, "512m");
            params.put(ISparkJobServerClientConstants.PARAM_NUM_CPU_CORES, "10");
            client.createContext("ctxTest", params);

        } catch (SparkJobServerClientException e1) {
            e1.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        get("/", (req, res) -> {
            ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(endpoint);
            Map<String, String> params = new HashMap<String, String>();
            params.put(ISparkJobServerClientConstants.PARAM_MEM_PER_NODE, "512m");
            params.put(ISparkJobServerClientConstants.PARAM_NUM_CPU_CORES, "10");
            //Start a spark job synchronously and wait until the result
            params.put(ISparkJobServerClientConstants.PARAM_APP_NAME, "spark-test");
            params.put(ISparkJobServerClientConstants.PARAM_CLASS_PATH, "spark.jobserver.WordCountExample");
            params.put(ISparkJobServerClientConstants.PARAM_CONTEXT, "ctxTest");
            params.put(ISparkJobServerClientConstants.PARAM_SYNC, "true");

            File inputData = new File(ClassLoader.getSystemResource("wordcount.json").toURI());
            SparkJobResult result = client.startJob(inputData, params);
            return result.toString();
        });
    }
}

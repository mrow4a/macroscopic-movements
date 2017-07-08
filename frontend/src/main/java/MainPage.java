import com.bluebreezecf.tools.sparkjobserver.api.*;
import spark.*;
import spark.template.velocity.*;

import java.io.InputStream;
import java.util.*;
import static spark.Spark.*;
import java.io.File;

/**
 * This class uses the ICRoute interface to create void routes.
 * The response for an ICRoute is rendered in an after-filter.
 */
public class MainPage {
    private static final String defaultSparkJobHost = "localhost";
    private static final String defaultSparkJobPort = "8090";
    private static final String appName = "movements";
    private static final String contextName = "movements";
    private static String endpoint = String.format("http://%s:%s/", defaultSparkJobHost, defaultSparkJobPort);

    public static void main(String[] args) {

        exception(Exception.class, (e, req, res) -> e.printStackTrace()); // print all exceptions
        staticFiles.location("/public");
        port(9999);

        get("/",                        (req, res)      -> renderMap(req));

        get("/api/check_file", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                String filePath = map.get("file").value().replaceAll("\\s+","");
                String sparkAddress = map.get("spark").value().replaceAll("\\s+","");
                File file = new File(filePath);
                if(file.exists() && !file.isDirectory()) {
                    ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(sparkAddress);
                    List<String> contexts = client.getContexts();

                    return "OK";
                }
                return "Error: File does not exists";
            }
            catch (Exception e){
                return "Error: " + e.getMessage();
            }
        });
        get("/api/get_hotspots", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                String filePath = map.get("file").value().replaceAll("\\s+","");
                String sparkAddress = map.get("spark").value().replaceAll("\\s+","");
                File file = new File(filePath);
                if(file.exists() && !file.isDirectory()) {
                    try {
                        ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(sparkAddress);

                        //POST /jars/<appName>
                        InputStream jarFileStream = ClassLoader.getSystemResourceAsStream("stopdetection.jar");

                        String appName = "spark-test";
                        boolean isUploaded = client.uploadSparkJobJar(jarFileStream, appName);
                        System.out.println("Uploaded: " + isUploaded);

                        //POST /contexts/<name>--Create context with parameters
                        Map<String, String> params = new HashMap<String, String>();
                        params.put(ISparkJobServerClientConstants.PARAM_MEM_PER_NODE, "512m");
                        params.put(ISparkJobServerClientConstants.PARAM_NUM_CPU_CORES, "10");
                        client.createContext("ctxTest", params);
                        params.put(ISparkJobServerClientConstants.PARAM_APP_NAME, "spark-test");
                        params.put(ISparkJobServerClientConstants.PARAM_CLASS_PATH, "spark.jobserver.stopdetection.StopDetectionJob");
                        params.put(ISparkJobServerClientConstants.PARAM_CONTEXT, "ctxTest");
                        params.put(ISparkJobServerClientConstants.PARAM_SYNC, "true");

                        String inputData = "input.path=" + filePath;
                        SparkJobResult result = client.startJob(inputData, params);
                        return result.toString();
                    } catch (SparkJobServerClientException e1) {
                        e1.printStackTrace();
                        return "Error: SparkJobServerClientException";
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "Error: Exception during spark execution";
                    }
                } else {
                    return "Error: File does not exists";
                }
            }
            catch (Exception e){
                return "Error: " + e.getMessage();
            }
        });
//        after((req, res) -> {
//            if (res.body() == null) { // if we didn't try to return a rendered response
//                res.body(renderMap(req));
//            }
//        });

    }

    private static String renderMap(Request req) {
        Map<String, Object> model = new HashMap<>();
        return renderTemplate("velocity/index.vm", model);
    }

    private static String renderTemplate(String template, Map model) {
        return new VelocityTemplateEngine().render(new ModelAndView(model, template));
    }

    @FunctionalInterface
    private interface ICRoute extends Route {
        default Object handle(Request request, Response response) throws Exception {
            handle(request);
            return "";
        }
        void handle(Request request) throws Exception;
    }

}

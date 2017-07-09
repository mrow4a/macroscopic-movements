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
    private static final String appName = "movements";
    private static final String contextName = "movements";
    private static final String heatDetectionJobPath = "spark.jobserver.movements.HeatSpotsDetectionJob";

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
                    if (!client.hasApp(appName)) {
                        //POST /jars/<appName>
                        InputStream jarFileStream = ClassLoader.getSystemResourceAsStream("movements.jar");
                        if (!client.uploadSparkJobJar(jarFileStream, appName)) {
                            return "Error: Problem uploading job JAR file";
                        }
                    }

                    if (!client.hasContext(contextName)) {
                        //CREATE CONTEXT
                        Map<String, String> params = new HashMap<String, String>();
                        params.put(ISparkJobServerClientConstants.PARAM_MEM_PER_NODE, "5000m");
                        params.put(ISparkJobServerClientConstants.PARAM_NUM_CPU_CORES, "10");
                        if (!client.createContext(contextName, params)) {
                            return "Error: Problem creating context";
                        }
                    }
                    return "Spark job server ready";
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

                        //POST /contexts/<name>--Create context with parameters
                        Map<String, String> params = new HashMap<String, String>();
                        params.put(ISparkJobServerClientConstants.PARAM_APP_NAME, appName);
                        params.put(ISparkJobServerClientConstants.PARAM_CLASS_PATH, heatDetectionJobPath);
                        params.put(ISparkJobServerClientConstants.PARAM_CONTEXT, contextName);
                        params.put(ISparkJobServerClientConstants.PARAM_SYNC, "true");
                        params.put(ISparkJobServerClientConstants.PARAM_TIMEOUT, "1000000");

                        String jobServFileInputPath = client.uploadInputFile(filePath, appName);
                        String inputData = "input.path=" + jobServFileInputPath;
                        // return inputData;
                        SparkJobResult result = client.startJob(inputData, params);
                        return result.toString();
                    } catch (SparkJobServerClientException e) {
                        e.printStackTrace();
                        return "Error: SparkJobServerClientException - " + e.toString();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "Error: Exception during spark execution - " + e.toString();
                    }
                } else {
                    return "Error: File does not exists";
                }
            }
            catch (Exception e){
                return "Error: " + e.getMessage();
            }
        });
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

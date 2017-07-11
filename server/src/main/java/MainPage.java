import com.spotify.docker.client.exceptions.DockerException;
import spark.*;
import spark.template.velocity.*;

import java.util.*;
import static spark.Spark.*;
import java.io.File;
import java.nio.file.Paths;
import movements.docker.DockerHandler;

/**
 * This class uses the ICRoute interface to create void routes.
 * The response for an ICRoute is rendered in an after-filter.
 */
public class MainPage {

    public static void main(String[] args) {

        exception(Exception.class, (e, req, res) -> e.printStackTrace()); // print all exceptions
        staticFiles.location("/public");
        port(9999);

        get("/",                        (req, res)      -> renderMap(req));

        get("/api/check_file", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                String filePath = map.get("file").value().replaceAll("\\s+","");
                File file = new File(filePath);
                if(file.exists() && !file.isDirectory()) {
                    try {
                        // Try to obtain master container
                        DockerHandler.getMasterContainer();
                    } catch (DockerException e) {
                        //DockerHandler.initStack();
                        return "Docker spark cluster not running";
                    }

                    return "Docker spark cluster spawned";
                }
                return "Error: File input does not exists";
            }
            catch (Exception e){
                return "Error: [consider removing spark-master docker and refresh] - " + e.getMessage();
            }
        });

        get("/api/init_spark", (req, res) -> {
            try {
                // Try to obtain master container
                DockerHandler.getMasterContainer();
            } catch (DockerException e) {
                DockerHandler.initStack();
            }
            return "Docker spark cluster spawned";
        });

        get("/api/stop_spark", (req, res) -> {

            try {
                // Try to remove stack
                DockerHandler.removeStack();
            } catch (DockerException e) {
                return "Docker spark cluster cannot be removed";
            }
            return "Docker spark cluster removed";
        });

        get("/api/extract", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                String filePath = map.get("file").value().replaceAll("\\s+","");
                try {
                    DockerHandler.copyToTmp(filePath);
                    return "Prepared and extracted";
                } catch (Exception e) {
                    e.printStackTrace();
                    return "Error: Exception during spark execution - " + e.toString();
                }
            }
            catch (Exception e){
                return "Error: " + e.getMessage();
            }
        });

        get("/api/tl_hotspots", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                String filePath = map.get("file").value().replaceAll("\\s+","");
                String sparkAddress = map.get("spark").value().replaceAll("\\s+","");
                try {
                    // Get only filename from the path
                    String filename = Paths.get(filePath).getFileName().toString();

                    // Run job and return its exit code
                    return DockerHandler.runSparkJob(sparkAddress, filename, "movements.jobs.ClusterStopsJob");
                } catch (Exception e) {
                    e.printStackTrace();
                    return "Error: Exception during spark execution - " + e.toString();
                }
            }
            catch (Exception e){
                return "Error: " + e.getMessage();
            }
        });

        get("/api/get_hotspots", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                try {
                    return DockerHandler.runReadFileJob();
                } catch (Exception e) {
                    e.printStackTrace();
                    return "Error: Exception during spark execution - " + e.toString();
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

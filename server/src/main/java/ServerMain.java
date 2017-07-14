import com.spotify.docker.client.exceptions.DockerException;
import spark.*;
import spark.template.velocity.*;

import java.util.*;
import static spark.Spark.*;
import java.io.File;
import java.nio.file.Paths;
import movements.docker.*;
import api.ApiHandler;

/**
 * This class uses the ICRoute interface to create void routes.
 * The response for an ICRoute is rendered in an after-filter.
 */
public class ServerMain {

    public static void main(String[] args) {

        exception(Exception.class, (e, req, res) -> e.printStackTrace()); // print all exceptions
        staticFiles.location("/public");
        port(80);

        get("/",                        (req, res)      -> renderMap(req));

        get("/api/check_file", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            String endpoint = map.get("endpoint").value().replaceAll("\\s+","");
            String filePath = map.get("file").value().replaceAll("\\s+","");
            String sparkMaster = map.get("spark").value().replaceAll("\\s+","");
            return ApiHandler.check_file(endpoint, filePath, sparkMaster);
        });

        get("/api/get_hotspots", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            String endpoint = map.get("endpoint").value().replaceAll("\\s+","");
            String filePath = map.get("file").value().replaceAll("\\s+","");
            String sparkMaster = map.get("spark").value().replaceAll("\\s+","");
            return ApiHandler.get_hotspots(endpoint, filePath, sparkMaster);
        });

    /*
     * The below methods were used to handle Docker clients and are now depreciated
     */

//        get("/api/check_file", (req, res) -> {
//            return ApiHandler.check_file();
//        });
//
//        get("/api/init_spark", (req, res) -> {
//            return ApiHandler.init_spark();
//        });
//
//        get("/api/stop_spark", (req, res) -> {
//            return ApiHandler.stop_spark();
//        });
//
//        get("/api/extract", (req, res) -> {
//            return ApiHandler.extract();
//        });
//
//        get("/api/tl_hotspots", (req, res) -> {
//            return ApiHandler.tl_hotspots();
//        });
//
//        get("/api/get_hotspots", (req, res) -> {
//            return ApiHandler.get_hotspots();
//        });
    }


    private static String renderMap(Request req) {
        Map<String, Object> model = new HashMap<>();
        return renderTemplate("index.vm", model);
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

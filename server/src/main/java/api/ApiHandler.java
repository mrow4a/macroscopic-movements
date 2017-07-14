package api;

public final class ApiHandler {

    public ApiHandler()  {} // private constructor


    public static String check_file(String endpoint, String filePath, String sparkMaster) {
        try {
            return SparkClient.validateFile(endpoint, filePath, sparkMaster);
        }
        catch (Exception e){
            return "Error: "+e;
        }
    }

    public static String get_hotspots(String endpoint, String filePath, String sparkMaster) {
        try {
            return SparkClient.get_hotspots(endpoint, filePath, sparkMaster);
        }
        catch (Exception e){
            return "Error: "+e;
        }
    }

    /*
     * The below methods were used to handle Docker clients and are now depreciated
     */

//    public static String check_file() {
//        QueryParamsMap map = req.queryMap();
//        try {
//            String filePath = map.get("file").value().replaceAll("\\s+","");
//            File file = new File(filePath);
//            if(file.exists() && !file.isDirectory()) {
//                try {
//                    // Try to obtain master container
//                    DockerHandler.getMasterContainer();
//                } catch (DockerException e) {
//                    //DockerHandler.initStack();
//                    return "Docker spark cluster not running";
//                }
//
//                return "Docker spark cluster spawned";
//            }
//            return "Error: File input does not exists";
//        }
//        catch (Exception e){
//            return "Error: [consider removing spark-master docker and refresh] - " + e.getMessage();
//        }
//    }
//
//    public static String init_spark() {
//        try {
//            // Try to obtain master container
//            DockerHandler.getMasterContainer();
//        } catch (DockerException e) {
//            DockerHandler.initStack();
//        }
//        return "Docker spark cluster spawned";
//    }
//
//    public static String stop_spark() {
//            try {
//                // Try to remove stack
//                DockerHandler.removeStack();
//            } catch (DockerException e) {
//                return "Docker spark cluster cannot be removed";
//            }
//            return "Docker spark cluster removed";
//    }
//
//    public static String extract() {
//        QueryParamsMap map = req.queryMap();
//        try {
//            String filePath = map.get("file").value().replaceAll("\\s+","");
//            try {
//                DockerHandler.copyToTmp(filePath);
//                return "Prepared and extracted";
//            } catch (Exception e) {
//                e.printStackTrace();
//                return "Error: Exception during spark execution - " + e.toString();
//            }
//        }
//        catch (Exception e){
//            return "Error: " + e.getMessage();
//        }
//    }
//
//    public static String tl_hotspots() {
//        QueryParamsMap map = req.queryMap();
//        try {
//            String filePath = map.get("file").value().replaceAll("\\s+","");
//            String sparkAddress = map.get("spark").value().replaceAll("\\s+","");
//            try {
//                // Get only filename from the path
//                String filename = Paths.get(filePath).getFileName().toString();
//
//                // Run job and return its exit code
//                return DockerHandler.runSparkJob(sparkAddress, filename, "movements.jobs.ClusterStopsJob");
//            } catch (Exception e) {
//                e.printStackTrace();
//                return "Error: Exception during spark execution - " + e.toString();
//            }
//        }
//        catch (Exception e){
//            return "Error: " + e.getMessage();
//        }
//    }
//
//    public static String get_hotspots() {
//        QueryParamsMap map = req.queryMap();
//        try {
//            try {
//                return DockerHandler.runReadFileJob();
//            } catch (Exception e) {
//                e.printStackTrace();
//                return "Error: Exception during spark execution - " + e.toString();
//            }
//        }
//        catch (Exception e){
//            return "Error: " + e.getMessage();
//        }
//    }

}
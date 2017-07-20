package api;

import org.apache.spark.launcher.SparkLauncher;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import utils.InputStreamReaderRunnable;

public final class SparkClient {

    private static final String inputLocation = "/data/input";
    private static final String outputLocation = "/data/output";
    private static final String jarLocation = "/jobs.jar";
    private static final String appName = "MOVEMENTS";

    public SparkClient()  {} // private constructor

    public static String validateFile(String endpoint, String filePath, String sparkMaster) throws Exception {
        Process spark = new SparkLauncher()
                .setAppResource(jarLocation)
                .setMainClass("movements.jobs.ValidateFileJob")
                .setMaster(sparkMaster)
                .addAppArgs(endpoint)
                .addAppArgs(filePath)
                .addAppArgs(sparkMaster)
                .launch();

        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        spark.waitFor();

        if (spark.exitValue() == 0) {
            return inputStreamReaderRunnable.getOutput();
        }
        throw new Exception(errorStreamReaderRunnable.getOutput());
    }

    public static String get_hotspots(String endpoint, String filePath, String sparkMaster) throws Exception {
        Process spark = new SparkLauncher()
                .setAppResource(jarLocation)
                .setMainClass("movements.jobs.ClusterStopsJob")
                .setMaster(sparkMaster)
                .addAppArgs(endpoint)
                .addAppArgs(filePath)
                .addAppArgs(sparkMaster)
                .launch();

        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        spark.waitFor();

        if (spark.exitValue() == 0) {
            String output = inputStreamReaderRunnable.getOutput();
            JSONArray array = new JSONArray();
            String[] lines = output.split("\n");
            for (String line : lines) {
                JSONObject item = new JSONObject();
                String[] parts = line.split("\\|");
                System.out.println(String.join(",", parts));
//              |-- ClusterID: integer (nullable = true)
//              |-- avg(Latitude): double (nullable = true)
//              |-- avg(Longitude): double (nullable = true)
//              |-- avg(Duration): double (nullable = true)
//              |-- PageRank: double (nullable = true)
//              |-- NeighborsIN: list() (nullable = true)
//              |-- NeighborsOUT: list() (nullable = true)
//              |-- Outdegrees: integer (nullable = true)
//              |-- Indegrees: integer (nullable = true)
                if (parts.length == 9) {
                    item.put("id", parts[0]);
                    item.put("lat", parts[1]);
                    item.put("long", parts[2]);
                    item.put("duration", parts[3]);
                    item.put("pagerank", parts[4]);

                    JSONArray neighborsin = JSONArray
                            .fromObject(
                                    parts[5].replace("List(", "[").replace(")","]")
                            );
                    JSONArray neighborsout = JSONArray
                            .fromObject(
                                    parts[6].replace("List(", "[").replace(")","]")
                            );
                    item.put("neighborsin", neighborsin);
                    item.put("neighborsout", neighborsout);

                    item.put("outdegrees", parts[7]);
                    item.put("indegrees", parts[8]);
                    array.add(item);
                } else {
                    System.out.println("Error, wrong number of parameters");
                }
            }
            return array.toString();
        }
        throw new Exception(errorStreamReaderRunnable.getOutput());
    }


    /*
     * The below methods were used to handle Docker clients and are now depreciated
     */

//    public static boolean initStack() throws DockerException, InterruptedException, DockerCertificateException {
//        // Master not found, need to initialize whole stack
//        removeStack();
//
//        createDataStore();
//
//        String masterId = createMaster();
//
//        for(int x = 0; x < 2; x = x + 1) {
//            createSlave(nameSlave+x);
//        }
//        return true;
//    }
//
//    public static boolean removeStack() throws DockerException, InterruptedException, DockerCertificateException{
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        final List<Container> containers = docker.listContainers(
//                DockerClient.ListContainersParam.allContainers());
//        for (Container container : containers) {
//            if(container.names().toString().contains(nameDataStore)
//                    || container.names().toString().contains(nameMaster)
//                    || container.names().toString().contains(nameSlave)) {
//                docker.removeContainer(container.id(),
//                        DockerClient.RemoveContainerParam.forceKill());
//            }
//        }
//        return true;
//    }
//
//    public static boolean copyToTmp(String filePath) throws DockerException, InterruptedException, IOException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        String id = getMasterContainer().id();
//        runCleaningJob();
//
//        Path filePathObject = Paths.get(filePath).getParent();
//        docker.copyToContainer(filePathObject, id, tmpLocation);
//
//        return true;
//    }
//
//    public static String createSlave(String nameSlave)
//            throws DockerException, InterruptedException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        final HostConfig hostConfigSlave= HostConfig.builder()
//                .volumesFrom("datastore")
//                .links("master:master")
//                .build();
//        final ContainerConfig configSlave = ContainerConfig.builder()
//                .image("brunocf/spark-slave")
//                .hostConfig(hostConfigSlave)
//                .build();
//        final ContainerCreation creationSlave = docker.createContainer(configSlave, nameSlave);
//        String id = creationSlave.id();
//        docker.startContainer(id);
//        return id;
//    }
//
//    public static String createDataStore()
//            throws DockerException, InterruptedException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        final ContainerConfig configDataStore = ContainerConfig.builder()
//                .image("brunocf/spark-datastore")
//                .volumes("/data")
//                .build();
//        final ContainerCreation creationDataStore = docker.createContainer(configDataStore, nameDataStore);
//        final String id = creationDataStore.id();
//        return id;
//    }
//
//    public static Container getMasterContainer() throws DockerException, InterruptedException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        final List<Container> containers = docker.listContainers(DockerClient.ListContainersParam.allContainers());
//        for (Container container : containers) {
//            if(container.names().toString().contains(nameMaster)
//                    && container.status().contains("Up")) {
//                return container;
//            }
//        }
//        throw new DockerException("Master Spark Node not running");
//    }
//
//    public static String createMaster()
//            throws DockerException, InterruptedException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        final String[] ports = {"8080", "7077"};
//        final Map<String, List<PortBinding>> portBindings = new HashMap<>();
//        for (String port : ports) {
//            List<PortBinding> hostPorts = new ArrayList<>();
//            hostPorts.add(PortBinding.of("0.0.0.0", port));
//            portBindings.put(port, hostPorts);
//        }
//        final HostConfig hostConfigMaster = HostConfig.builder()
//                .portBindings(portBindings)
//                .volumesFrom("datastore")
//                .build();
//        final ContainerConfig configMaster = ContainerConfig.builder()
//                .image("brunocf/spark-master")
//                .hostConfig(hostConfigMaster)
//                .exposedPorts(ports)
//                .build();
//        final ContainerCreation creationMaster = docker.createContainer(configMaster, nameMaster);
//        final String idMaster = creationMaster.id();
//        docker.startContainer(idMaster);
//        return idMaster;
//    }
//
//    /*
//     * This method should implement cleaning of the ETL db and tmp folders
//     */
//    public static int runSparkJob(String sparkAddress, String filename, String classpath) throws DockerException, InterruptedException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        String cmd = "spark-submit" +
//                " --master " + sparkAddress +
//                " --class " + classpath +
//                " --executor-memory 1G" +
//                " --total-executor-cores 8" +
//                " " + jarLocation +
//                " " + tmpLocation + "/" + filename + " " + databaseLocation;
//
//        // Create container with exposed ports
//        final HostConfig hostConfig= HostConfig.builder()
//                .volumesFrom("datastore")
//                .links("master:master")
//                .build();
//        final ContainerConfig containerConfig = ContainerConfig.builder()
//                .hostConfig(hostConfig)
//                .image("movements/jobs")
//                .cmd("sh", "-c", cmd)
//                .build();
//
//        final String id = docker.createContainer(containerConfig).id();
//        docker.startContainer(id);
//        int exit = -1;
//        try {
//            exit = docker.waitContainer(id).statusCode();
//        } catch (Exception e) {
//        }
//        docker.removeContainer(id);
//        return exit;
//    }
//
//
//    /*
//     * This method should implement cleaning of the ETL db and tmp folders
//     */
//    public static String runReadFileJob() throws DockerException, InterruptedException, IOException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        JSONArray array = new JSONArray();
//
//        // Create container with exposed ports
//        final String command = "FILES="+databaseLocation+"/*;for f in $FILES; do cat $f; done";
//        final HostConfig hostConfig= HostConfig.builder()
//                .volumesFrom("datastore")
//                .build();
//        final ContainerConfig containerConfig = ContainerConfig.builder()
//                .hostConfig(hostConfig)
//                .image("movements/jobs")
//                .cmd("sh", "-c", command)
//                .build();
//
//        final String id = docker.createContainer(containerConfig).id();
//        docker.startContainer(id);
//        try {
//            if(docker.waitContainer(id).statusCode() == 0) {
//                final String log;
//                try (LogStream logs = docker.logs(id, LogsParam.stdout())) {
//                    String[] lines = logs.readFully().split("\n");
//                    for (String line : lines) {
//                        JSONObject item = new JSONObject();
//                        String[] parts = line.split(",");
//                        if (parts.length == 3) {
//                            item.put("lat", parts[0]);
//                            item.put("long", parts[1]);
//                            item.put("id", parts[2]);
//                            array.add(item);
//                        }
//                    }
//                }
//            }
//        } catch (Exception e) {
//        }
//
//        docker. removeContainer(id, DockerClient.RemoveContainerParam.forceKill());
//        return array.toString();
//    }
//
//    /*
//     * This method should implement cleaning of the ETL db and tmp folders
//     */
//    public static boolean runCleaningJob() throws DockerException, InterruptedException, DockerCertificateException {
//        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
//        // Create container with exposed ports
//        final String command = "rm -rf " + databaseLocation +
//                " && rm -rf " + tmpLocation + " && mkdir -p " + tmpLocation;
//        final HostConfig hostConfig= HostConfig.builder()
//                .volumesFrom("datastore")
//                .build();
//        final ContainerConfig containerConfig = ContainerConfig.builder()
//                .hostConfig(hostConfig)
//                .image("brunocf/spark-submit")
//                .cmd("sh", "-c", command)
//                .build();
//
//        final String id = docker.createContainer(containerConfig).id();
//        docker.startContainer(id);
//        try {
//            docker.waitContainer(id);
//        } catch (Exception e) {
//        }
//        docker.removeContainer(id, DockerClient.RemoveContainerParam.forceKill());
//        return true;
//    }
}
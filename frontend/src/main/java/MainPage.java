import com.bluebreezecf.tools.sparkjobserver.api.*;
import com.spotify.docker.client.exceptions.DockerException;
import com.sun.org.apache.xpath.internal.operations.Bool;
import spark.*;
import spark.template.velocity.*;

import java.io.IOException;
import java.util.*;
import static spark.Spark.*;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.DockerClient.LogsParam;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * This class uses the ICRoute interface to create void routes.
 * The response for an ICRoute is rendered in an after-filter.
 */
public class MainPage {
    private static final String appName = "movements";
    private static final String contextName = "movements";
    private static final String databaseLocation = "/data/db";
    private static final String tmpLocation = "/data/tmp";
    private static final String nameMaster = "master";
    private static final String nameDataStore = "datastore";
    private static final String nameSlave = "slave";

    public static void main(String[] args) {

        exception(Exception.class, (e, req, res) -> e.printStackTrace()); // print all exceptions
        staticFiles.location("/public");
        port(9999);

        get("/",                        (req, res)      -> renderMap(req));

        get("/api/check_file", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                String filePath = map.get("file").value().replaceAll("\\s+","");
                String jarPath = map.get("jar").value().replaceAll("\\s+","");
                File file = new File(filePath);
                File jar = new File(jarPath);
                if(file.exists() && !file.isDirectory() && jar.exists() && !jar.isDirectory()) {
                    final DockerClient docker = DefaultDockerClient.fromEnv().build();

                    try {
                        // Try to obtain master container
                        getMasterContainer(nameMaster, docker);
                    } catch (DockerException e) {
                        // Master not found, need to initialize whole stack
                        final List<Container> containers = docker.listContainers(DockerClient.ListContainersParam.allContainers());
                        for (Container container : containers) {
                            if(container.names().toString().contains(nameDataStore)
                                    || container.names().toString().contains(nameMaster)
                                    || container.names().toString().contains(nameSlave)) {
                                docker.removeContainer(container.id(), DockerClient.RemoveContainerParam.forceKill());
                            }
                        }

                        createDataStore(nameDataStore, docker);

                        String masterId = createMaster(nameMaster, docker);

                        for(int x = 0; x < 2; x = x + 1) {
                            createSlave(nameSlave+x, docker);
                        }
                    }

                    return "Docker spark cluster spawned";
                }
                return "Error: File input or JAR does not exists";
            }
            catch (Exception e){
                return "Error: [consider removing spark-master docker and refresh] - " + e.getMessage();
            }
        });
        get("/api/extract", (req, res) -> {
            QueryParamsMap map = req.queryMap();
            try {
                String filePath = map.get("file").value().replaceAll("\\s+","");
                String jarPath = map.get("jar").value().replaceAll("\\s+","");
                try {
                    final DockerClient docker = DefaultDockerClient.fromEnv().build();
                    String id = getMasterContainer(nameMaster, docker).id();
                    runCleaningJob(docker);

                    Path jarPathObject = Paths.get(jarPath).getParent();
                    docker.copyToContainer(jarPathObject, id, tmpLocation);

                    Path filePathObject = Paths.get(filePath).getParent();
                    docker.copyToContainer(filePathObject, id, tmpLocation);
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
                String jarPath = map.get("jar").value().replaceAll("\\s+","");
                String sparkAddress = map.get("spark").value().replaceAll("\\s+","");
                try {
                    final DockerClient docker = DefaultDockerClient.fromEnv().build();

                    String jarName = Paths.get(jarPath).getFileName().toString();
                    String filename = Paths.get(filePath).getFileName().toString();
                    String cmd = "spark-submit" +
                            " --master " + sparkAddress +
                            " --class movements.jobs.ClusterStopsJob" +
                            " --executor-memory 1G" +
                            " --total-executor-cores 8" +
                            " " + tmpLocation + "/" + jarName +
                            " " + tmpLocation + "/" + filename + " " + databaseLocation;

                    // Run job and return its exit code
                    return runSparkJob(cmd, docker);
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
                    final DockerClient docker = DefaultDockerClient.fromEnv().build();
                    String id = getMasterContainer(nameMaster, docker).id();
                    return runReadFileJob(docker);
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

    private static String createSlave(String nameSlave, DockerClient docker)
            throws DockerException, InterruptedException {
        final HostConfig hostConfigSlave= HostConfig.builder()
                .volumesFrom("datastore")
                .links("master:master")
                .build();
        final ContainerConfig configSlave = ContainerConfig.builder()
                .image("brunocf/spark-slave")
                .hostConfig(hostConfigSlave)
                .build();
        final ContainerCreation creationSlave = docker.createContainer(configSlave, nameSlave);
        String id = creationSlave.id();
        docker.startContainer(id);
        return id;
    }

    /*
     * This method should implement cleaning of the ETL db and tmp folders
     */
    static int runSparkJob(String cmd, DockerClient docker) throws DockerException, InterruptedException {
        // Create container with exposed ports
        final HostConfig hostConfig= HostConfig.builder()
                .volumesFrom("datastore")
                .links("master:master")
                .build();
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image("brunocf/spark-submit")
                .cmd("sh", "-c", cmd)
                .build();

        final String id = docker.createContainer(containerConfig).id();
        docker.startContainer(id);
        int exit = -1;
        try {
            exit = docker.waitContainer(id).statusCode();
        } catch (Exception e) {
        }
        docker.removeContainer(id);
        return exit;
    }

    /*
     * This method should implement cleaning of the ETL db and tmp folders
     */
    static String runReadFileJob(DockerClient docker) throws DockerException, InterruptedException, IOException {
        JSONArray array = new JSONArray();

        // Create container with exposed ports
        final String command = "FILES="+databaseLocation+"/*;for f in $FILES; do cat $f; done";
        final HostConfig hostConfig= HostConfig.builder()
                .volumesFrom("datastore")
                .build();
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image("brunocf/spark-submit")
                .cmd("sh", "-c", command)
                .build();

        final String id = docker.createContainer(containerConfig).id();
        docker.startContainer(id);
        try {
            if(docker.waitContainer(id).statusCode() == 0) {
                final String log;
                try (LogStream logs = docker.logs(id, LogsParam.stdout())) {
                    String[] lines = logs.readFully().split("\n");
                    for (String line : lines) {
                        JSONObject item = new JSONObject();
                        String[] parts = line.split(",");
                        if (parts.length == 3) {
                            item.put("lat", parts[0]);
                            item.put("long", parts[1]);
                            item.put("id", parts[2]);
                            array.add(item);
                        }
                    }
                }
            }
        } catch (Exception e) {
        }

        docker. removeContainer(id, DockerClient.RemoveContainerParam.forceKill());
        return array.toString();
    }

    /*
     * This method should implement cleaning of the ETL db and tmp folders
     */
    static boolean runCleaningJob(DockerClient docker) throws DockerException, InterruptedException {
        // Create container with exposed ports
        final String command = "rm -rf " + databaseLocation +
                " && rm -rf " + tmpLocation + " && mkdir -p " + tmpLocation;
        final HostConfig hostConfig= HostConfig.builder()
                .volumesFrom("datastore")
                .build();
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image("brunocf/spark-submit")
                .cmd("sh", "-c", command)
                .build();

        final String id = docker.createContainer(containerConfig).id();
        docker.startContainer(id);
        try {
            docker.waitContainer(id);
        } catch (Exception e) {
        }
        docker.removeContainer(id, DockerClient.RemoveContainerParam.forceKill());
        return true;
    }

    static String createDataStore(String nameDataStore, DockerClient docker)
            throws DockerException, InterruptedException {
        final ContainerConfig configDataStore = ContainerConfig.builder()
                .image("brunocf/spark-datastore")
                .volumes("/data")
                .build();
        final ContainerCreation creationDataStore = docker.createContainer(configDataStore, nameDataStore);
        final String id = creationDataStore.id();
        return id;
    }

    private static Container getMasterContainer(String nameMaster, DockerClient docker) throws DockerException, InterruptedException {
        final List<Container> containers = docker.listContainers(DockerClient.ListContainersParam.allContainers());
        for (Container container : containers) {
            if(container.names().toString().contains(nameMaster)
                    && container.status().contains("Up")) {
                return container;
            }
        }
        throw new DockerException("Master Spark Node not running");
    }

    private static String createMaster(String nameMaster, DockerClient docker)
            throws DockerException, InterruptedException {
        final String[] ports = {"8080", "7077"};
        final Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of("0.0.0.0", port));
            portBindings.put(port, hostPorts);
        }
        final HostConfig hostConfigMaster = HostConfig.builder()
                .portBindings(portBindings)
                .volumesFrom("datastore")
                .build();
        final ContainerConfig configMaster = ContainerConfig.builder()
                .image("brunocf/spark-master")
                .hostConfig(hostConfigMaster)
                .exposedPorts(ports)
                .build();
        final ContainerCreation creationMaster = docker.createContainer(configMaster, nameMaster);
        final String idMaster = creationMaster.id();
        docker.startContainer(idMaster);
        return idMaster;
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

package movements.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.DockerClient.LogsParam;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DockerHandler {

    private static final String databaseLocation = "/data/db";
    private static final String jarLocation = "/jobs.jar";
    private static final String tmpLocation = "/data/tmp";
    private static final String nameMaster = "master";
    private static final String nameDataStore = "datastore";
    private static final String nameSlave = "slave";

    public DockerHandler()  {} // private constructor

    public static boolean initStack() throws DockerException, InterruptedException, DockerCertificateException {
        // Master not found, need to initialize whole stack
        removeStack();

        createDataStore();

        String masterId = createMaster();

        for(int x = 0; x < 2; x = x + 1) {
            createSlave(nameSlave+x);
        }
        return true;
    }

    public static boolean removeStack() throws DockerException, InterruptedException, DockerCertificateException{
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
        final List<Container> containers = docker.listContainers(
                DockerClient.ListContainersParam.allContainers());
        for (Container container : containers) {
            if(container.names().toString().contains(nameDataStore)
                    || container.names().toString().contains(nameMaster)
                    || container.names().toString().contains(nameSlave)) {
                docker.removeContainer(container.id(),
                        DockerClient.RemoveContainerParam.forceKill());
            }
        }
        return true;
    }

    public static boolean copyToTmp(String filePath) throws DockerException, InterruptedException, IOException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
        String id = getMasterContainer().id();
        runCleaningJob();

        Path filePathObject = Paths.get(filePath).getParent();
        docker.copyToContainer(filePathObject, id, tmpLocation);

        return true;
    }

    public static String createSlave(String nameSlave)
            throws DockerException, InterruptedException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
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

    public static String createDataStore()
            throws DockerException, InterruptedException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
        final ContainerConfig configDataStore = ContainerConfig.builder()
                .image("brunocf/spark-datastore")
                .volumes("/data")
                .build();
        final ContainerCreation creationDataStore = docker.createContainer(configDataStore, nameDataStore);
        final String id = creationDataStore.id();
        return id;
    }

    public static Container getMasterContainer() throws DockerException, InterruptedException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
        final List<Container> containers = docker.listContainers(DockerClient.ListContainersParam.allContainers());
        for (Container container : containers) {
            if(container.names().toString().contains(nameMaster)
                    && container.status().contains("Up")) {
                return container;
            }
        }
        throw new DockerException("Master Spark Node not running");
    }

    public static String createMaster()
            throws DockerException, InterruptedException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
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

    /*
     * This method should implement cleaning of the ETL db and tmp folders
     */
    public static int runSparkJob(String sparkAddress, String filename, String classpath) throws DockerException, InterruptedException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
        String cmd = "spark-submit" +
                " --master " + sparkAddress +
                " --class " + classpath +
                " --executor-memory 1G" +
                " --total-executor-cores 8" +
                " " + jarLocation +
                " " + tmpLocation + "/" + filename + " " + databaseLocation;

        // Create container with exposed ports
        final HostConfig hostConfig= HostConfig.builder()
                .volumesFrom("datastore")
                .links("master:master")
                .build();
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image("movements/jobs")
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
    public static String runReadFileJob() throws DockerException, InterruptedException, IOException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
        JSONArray array = new JSONArray();

        // Create container with exposed ports
        final String command = "FILES="+databaseLocation+"/*;for f in $FILES; do cat $f; done";
        final HostConfig hostConfig= HostConfig.builder()
                .volumesFrom("datastore")
                .build();
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image("movements/jobs")
                .cmd("sh", "-c", command)
                .build();

        final String id = docker.createContainer(containerConfig).id();
        docker.startContainer(id);
        try {
            if(docker.waitContainer(id).statusCode() == 0) {
                final String log;
                try (LogStream logs = docker.logs(id, DockerClient.LogsParam.stdout())) {
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
    public static boolean runCleaningJob() throws DockerException, InterruptedException, DockerCertificateException {
        final DockerClient docker  = DefaultDockerClient.fromEnv().build();
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
}
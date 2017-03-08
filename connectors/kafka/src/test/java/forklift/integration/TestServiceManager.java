package forklift.integration;

import forklift.Forklift;
import forklift.connectors.KafkaConnector;
import org.apache.commons.io.FileUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Contains all of the necessary connection management code that tests
 * need in order to run against the activemq broker and forklift. This
 * manager assumes that only a single broker is needed for testing.
 *
 * @author mconroy
 * @author afrieze
 */
public class TestServiceManager {
    private static ExecutorService executor = Executors.newFixedThreadPool(3);
    private static final Object lock = new Object();
    private static Integer count = 0;
    private static ZookeeperService zookeeper;
    private static KafkaService kafka;
    private static SchemaRegistryService schemaRegistry;
    private static KafkaConnector connector;
    private static Forklift forklift;

    public static void start() {
        synchronized (lock) {
            if (forklift != null && forklift.isRunning())
                return;

            try {
                int zookeeperPort = 42181;
                int kafkaPort = 49092;
                int schemaPort = 48081;
                zookeeper = new ZookeeperService(zookeeperPort);
                kafka = new KafkaService(zookeeperPort, kafkaPort);
                schemaRegistry = new SchemaRegistryService(zookeeperPort, schemaPort);
                executor.execute(zookeeper);
                Thread.sleep(1000);
                executor.execute(kafka);
                Thread.sleep(1000);
                executor.execute(schemaRegistry);
                Thread.sleep(1000);

                // Verify that we can get a kafka connection to the broker.
                String kafkaUri = "localhost:" + kafkaPort;
                String schemaUrl = "http://localhost:" + schemaPort;
                connector = new KafkaConnector(kafkaUri, schemaUrl, "testGroup");
                connector.start();

                forklift = new Forklift();
                forklift.start(connector);

                count++;
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public static void stop() {
        synchronized (lock) {
            // Don't shutdown the connector if there are still classes using it.
            count--;
            if (count > 0)
                return;

            // Kill the broker and cleanup the testing data.
            try {
                forklift.shutdown();
                schemaRegistry.stop();
                Thread.sleep(500);
                kafka.stop();
                Thread.sleep(500);
                zookeeper.stop();
                Thread.sleep(500);
                FileUtils.deleteDirectory(kafka.getDataDirectoryFile());
                FileUtils.deleteDirectory(zookeeper.getDataDirectoryFile());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public static KafkaConnector getConnector() {
        synchronized (lock) {
            return connector;
        }
    }

    public static Forklift getForklift() {
        synchronized (lock) {
            return forklift;
        }
    }
}

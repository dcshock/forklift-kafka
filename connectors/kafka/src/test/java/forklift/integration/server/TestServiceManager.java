package forklift.integration.server;

import forklift.Forklift;
import forklift.connectors.KafkaConnector;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger log = LoggerFactory.getLogger(TestServiceManager.class);
    private static ExecutorService executor = Executors.newFixedThreadPool(3);
    private static final Object lock = new Object();
    private static Integer count = 0;
    private static ZookeeperService zookeeper;
    private static KafkaService kafka;
    private static SchemaRegistryService schemaRegistry;
    private static KafkaConnector connector;
    private static Forklift forklift;
    private static TelnetClient telnet;

    public static void start() {
        synchronized (lock) {
            if (forklift != null && forklift.isRunning())
                return;

            try {
                int zookeeperPort = 42181;
                int kafkaPort = 49092;
                int schemaPort = 58081;
                zookeeper = new ZookeeperService(zookeeperPort);
                kafka = new KafkaService(zookeeperPort, kafkaPort);
                schemaRegistry = new SchemaRegistryService(zookeeperPort, schemaPort);
                executor.execute(zookeeper);
                TestServiceManager.waitService("127.0.0.1", zookeeperPort);
                executor.execute(kafka);
                TestServiceManager.waitService("127.0.0.1", kafkaPort);
                executor.execute(schemaRegistry);
                TestServiceManager.waitService("127.0.0.1", schemaPort);

                // Verify that we can get a kafka connection to the broker.
                String kafkaUri = "127.0.0.1:49092";
                String schemaUrl = "http://127.0.0.1:58081";
                connector = new KafkaConnector(kafkaUri, schemaUrl, "testGroup");

                forklift = new Forklift();
                forklift.start(connector);

                count++;
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    private static void waitService(String host, int port) throws InterruptedException {
        telnet = new TelnetClient();
        int triesLeft = 100;
        while (triesLeft > 0)
            try {
                telnet.connect(host, port);
                triesLeft = 0;
            } catch (Exception e) {
                Thread.sleep(50);
                triesLeft--;
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
                kafka.stop();
                zookeeper.stop();
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {

                try {
                    FileUtils.deleteDirectory(kafka.getDataDirectoryFile());
                    FileUtils.deleteDirectory(zookeeper.getDataDirectoryFile());
                } catch (Exception e) {

                }
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

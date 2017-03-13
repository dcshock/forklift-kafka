package forklift;

import com.google.common.base.Preconditions;
import consul.Consul;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.KafkaConnector;
import forklift.consumer.ConsumerDeploymentEvents;
import forklift.consumer.LifeCycleMonitors;
import forklift.decorators.CoreService;
import forklift.decorators.Queue;
import forklift.decorators.Service;
import forklift.decorators.Topic;
import forklift.decorators.Topics;
import forklift.deployment.ClassDeployment;
import forklift.deployment.Deployment;
import forklift.stats.StatsCollector;
import org.apache.activemq.broker.BrokerService;
import org.apache.http.annotation.ThreadSafe;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Start Forklift as a server.  A running forklift server is responsible for
 * <pre>
 *     1.  Watching a directory for new deployments
 *     2.  Watching a directory for new properties
 *     3.  Accepting new deployments at runtime via the {@link #registerDeployment(Class[])} method
 *     4.  Optionally running a Broker
 *     5.  Connections to the Broker
 *     6.  Starting and managing the Deployment lifecycles
 *     7.  Retry Strategy
 *     8.  Replay Strategy
 * </pre>
 *
 * @author zdavep
 */
@ThreadSafe
public final class ForkliftServer {

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private volatile ServerState state = ServerState.LATENT;

    // Logging
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ForkliftServer.class);

    // Consumer deployment interval
    private static int SLEEP_INTERVAL = 10000; // 10 seconds

    private final ForkliftOpts opts;
    private Forklift forklift;

    public ForkliftServer(ForkliftOpts options) {
        this.opts = options;
    }

    private ConsumerDeploymentEvents deploymentEvents;
    private Registrar<Deployment> classDeployments = new Registrar<>();
    private CountDownLatch runningLatch = new CountDownLatch(1);

    /**
     * Attempts to start the forklift server.  This call is blocking and will not return until either the server starts successfully or the waitTime reaches 0.
     * A response of false does not mean the server will stop its attempt to startup.  This method may only be called once.
     *
     * @param waitTime the maximum time to wait
     * @param timeUnit the time unit of the waitTime
     * @return the {@link ServerState state} of the server at the time this method returns
     * @throws InterruptedException  if the current thread is interrupted while waiting for the server to start
     * @throws IllegalStateException if this method has already been called
     */
    public ServerState startServer(long waitTime, TimeUnit timeUnit) throws InterruptedException {
        synchronized (this) {
            Preconditions.checkState(state == ServerState.LATENT);
            state = ServerState.STARTING;
            log.info("Forklift server starting");
        }
        executor.execute(this::launch);
        try {
            this.runningLatch.await(waitTime, timeUnit);
            return state;
        } catch (InterruptedException e) {
            log.error("Launch Interrupted", e);
            throw e;
        }
    }

    /**
     * Stops the ForkliftServer.  This call is blocking and will not return until either the server is shutdown or the waitTime reaches 0.
     *
     * @param waitTime the maximum time to wait
     * @param timeUnit the time unit of the waitTime
     * @return the {@link ServerState state} of the server at the time this method returns
     * @throws InterruptedException if the current thread is interrupted before the waitTime has ellapsed
     */
    public ServerState stopServer(long waitTime, TimeUnit timeUnit) throws InterruptedException {
        shutdown();
        executor.shutdownNow();
        executor.awaitTermination(waitTime, timeUnit);
        return state;
    }

    /**
     * @return the current {@link ServerState state} of the server
     */
    public ServerState getServerState() {
        return state;
    }

    /**
     * Registers a collection of classes as new deployment.  Classes are scanned for the {@link Queue}, {@link Topic},
     * {@link Topics} {@link Service}, {@link CoreService} annotations.
     *
     * @param deploymentClasses the classes which make up the deployment
     */
    public synchronized void registerDeployment(Class<?>... deploymentClasses) {
        Preconditions.checkState(state == ServerState.RUNNING);
        Deployment deployment = new ClassDeployment(deploymentClasses);
        if (!classDeployments.isRegistered(deployment)) {
            classDeployments.register(deployment);
            deploymentEvents.onDeploy(deployment);
        }
    }

    /**
     * Launch a Forklift server instance.
     */
    private void launch() {
        if (setupBrokerAndForklift()) {
            deploymentEvents = new ConsumerDeploymentEvents(forklift);
            if (setupLifeCycleMonitors(forklift)) {
                try {
                    runEventLoop();
                } catch (InterruptedException ignored) {
                }
            }
        }
        if (state != ServerState.STOPPED) {
            state = ServerState.ERROR;
            log.info("Forklift server Error state");
        }
    }

    private boolean setupBrokerAndForklift() {
        try {
            forklift = new Forklift();
            final ForkliftConnectorI connector = startAndConnectToBroker();
            forklift.start(connector);
        } catch (Exception e) {
            log.error("Unable to startup broker and forklift", e);
        }
        return forklift.isRunning();
    }

    private void runEventLoop() throws InterruptedException {
        state = ServerState.RUNNING;
        log.info("Forklift server Running");
        runningLatch.countDown();
        while (state == ServerState.RUNNING) {
            log.debug("Scanning for new deployments...");
            synchronized (this) {
                this.wait(SLEEP_INTERVAL);
            }
        }
    }

    private void shutdown() {
        synchronized (this) {
            if (state != ServerState.RUNNING || state != ServerState.ERROR) {
                return;
            }
            state = ServerState.STOPPING;
            log.info("Forklift server Stopping");
            classDeployments.getAll().forEach(deploy -> deploymentEvents.onUndeploy(deploy));
            forklift.shutdown();

            if (broker != null) {
                try {
                    broker.stop();
                } catch (Exception ignored) {
                }
            }
            state = ServerState.STOPPED;
            log.info("Forklift server Stopped");

            this.notifyAll();
        }
    }

    private boolean setupLifeCycleMonitors(Forklift forklift) {
        LifeCycleMonitors.register(StatsCollector.class);
        boolean setup = true;
        return setup;
    }

    private ForkliftConnectorI startAndConnectToBroker() throws Exception {
        String brokerUrl = opts.getBrokerUrl();
        ForkliftConnectorI connector = null;
        if (brokerUrl.startsWith("consul.") && brokerUrl.length() > "consul.".length()) {

            final Consul c = new Consul("http://" + opts.getConsulHost(), 8500);
            // Build the connection string.
            final String serviceName = brokerUrl.split("\\.")[1];

            if ("kafka".equalsIgnoreCase(serviceName)) {
                String schemaRegistry = brokerUrl.split("\\.")[2];
                brokerUrl = c.catalog().service(serviceName).getProviders().stream()
                             .filter(srvc -> !srvc.isCritical())
                             .map(srvc -> srvc.getAddress() + ":" + srvc.getPort())
                             .collect(Collectors.joining(","));
                String schemaRegistries = c.catalog().service(schemaRegistry).getProviders().stream()
                                           .filter(srvc -> !srvc.isCritical())
                                           .map(srvc -> "http://" + srvc.getAddress() + ":" + srvc.getPort())
                                           .collect(Collectors.joining(","));

                String applicationName = this.opts.getApplicationName() == null ? "forklift" : this.opts.getApplicationName();
                connector = new KafkaConnector(brokerUrl, schemaRegistries, applicationName);

            }
        }
        log.info("Connected to broker on " + brokerUrl);
        return connector;
    }
}

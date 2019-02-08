package org.monitoring.oozie.zookeeper;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.monitoring.oozie.kafka.event.MonitoringEvent;
import org.monitoring.oozie.kafka.producer.KafkaEventProducer;

import com.salesforce.kafka.test.junit4.SharedZookeeperTestResource;

public class FlowConfigTest {
	@ClassRule
	public static final SharedZookeeperTestResource ZOOKEEPER = new SharedZookeeperTestResource();
	
	private static final String TOPIC_NAME = "tpc-bsy";
	private static final String APP_MON_KRUX = "krux";
	private static final String TRIGRAM_DGL = "dgl";
	private static final String IRT_KRUX = "A0384";
	private static final String ZK_PATH_PREFIX = "/project/bddf/apps/bsy/monitoring";
	private static final String JOB_NAME = "dmp_bad_krux";
	
	private FlowConfig flowRouter;
	
	@BeforeClass
	public static void classSetup() throws IOException, InterruptedException, KeeperException {
		createConfigInZooKeeper();
	}
	
	@Before
	public void setup() throws IOException, InterruptedException {
		flowRouter = new FlowConfig(ZOOKEEPER.getZookeeperConnectString(), ZK_PATH_PREFIX);
	}

	@Test
	public void getEventConfiguration_chould_return_properties_with_topic_name() {
		// GIVEN
		MonitoringEvent event = new MonitoringEvent();
		event.setJobName(JOB_NAME);
		// WHEN
		Optional<Properties> config = flowRouter.getEventConfiguration(event.getJobName());
		// THEN
		assertThat(config.get()).containsEntry(KafkaEventProducer.KAFKA_TOPIC_CONF, TOPIC_NAME);
	}

	@Test
	public void getEventConfiguration_chould_return_empty_when_project_is_not_registred() {
		// GIVEN
		MonitoringEvent event = new MonitoringEvent();
		event.setJobName("no_job_name");
		// WHEN
		Optional<Properties> config = flowRouter.getEventConfiguration(event.getJobName());
		// THEN
		assertThat(config).isEmpty();
	}
	
	private static byte[] createConfig() throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Properties props = new Properties();
		props.setProperty(KafkaEventProducer.KAFKA_TOPIC_CONF, TOPIC_NAME);
		props.setProperty(KafkaEventProducer.KAFKA_CONFIG_PREFIX + "security.protocol", "PLAINTEXT");
		props.setProperty(KafkaEventProducer.KAFKA_CONFIG_PREFIX + "client.id", "CLIENT1");
		
		props.setProperty(KafkaEventProducer.EVENT_CONFIG_PREFIX + "app_mon", APP_MON_KRUX);
		props.setProperty(KafkaEventProducer.EVENT_CONFIG_PREFIX + "trigram", TRIGRAM_DGL);
		props.setProperty(KafkaEventProducer.EVENT_CONFIG_PREFIX + "irt", IRT_KRUX);
		
		props.store(outputStream, null);
		byte[] data = outputStream.toByteArray();
		return data;
	}
	
	private static ZooKeeper getZooKeeper() throws IOException, InterruptedException {
		CountDownLatch countDownLatch = new CountDownLatch(1);
		ZooKeeper zookeeper = new ZooKeeper(ZOOKEEPER.getZookeeperConnectString(), 5 * 1000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if(event.getState() == KeeperState.SyncConnected) {
					countDownLatch.countDown();
				}
			}
		});
		countDownLatch.await();
		return zookeeper;
	}

	private static void createConfigInZooKeeper() throws IOException, InterruptedException, KeeperException {
		ZooKeeper zookeeper = getZooKeeper();
		byte[] data = createConfig();
		StringBuilder pathBuilder = new StringBuilder("/project");
		Op op1 = Op.create(pathBuilder.toString(), StringUtils.EMPTY.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		pathBuilder.append("/bddf");
		Op op2 = Op.create(pathBuilder.toString(), StringUtils.EMPTY.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		pathBuilder.append("/apps");
		Op op3 = Op.create(pathBuilder.toString(), StringUtils.EMPTY.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		pathBuilder.append("/bsy");
		Op op4 = Op.create(pathBuilder.toString(), StringUtils.EMPTY.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		pathBuilder.append("/monitoring");
		Op op5 = Op.create(pathBuilder.toString(), StringUtils.EMPTY.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		pathBuilder.append("/" + JOB_NAME);
		Op op6 = Op.create(pathBuilder.toString(), data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zookeeper.multi(Arrays.asList(op1, op2, op3, op4, op5, op6));
	}
}

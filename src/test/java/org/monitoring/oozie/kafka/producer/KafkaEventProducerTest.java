package org.monitoring.oozie.kafka.producer;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import com.google.gson.Gson;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.junit4.SharedZookeeperTestResource;

public class KafkaEventProducerTest {
	@ClassRule
	public static final SharedKafkaTestResource KAFKA = new SharedKafkaTestResource();
	@ClassRule
	public static final SharedZookeeperTestResource ZOOKEEPER = new SharedZookeeperTestResource();
	
	private static final String TOPIC_NAME = "tpc-bsy";
	private static final String TOPIC_KEY = "topic";
	private static final Gson MAPPER = new Gson();
	
	KafkaEventProducer producer;
	
	@BeforeClass
	public static void classSetup() throws IOException, InterruptedException, KeeperException {
		createConfigInZooKeeper();
	}
	
	@Before
	public void setup() {
		KAFKA.getKafkaTestUtils().createTopic(TOPIC_NAME, 1, (short) 1);
		producer = new KafkaEventProducer(KAFKA.getKafkaConnectString(), ZOOKEEPER.getZookeeperConnectString());
	}
	
	@Test
	public void sendEvent_shoud_send_an_event_into_kafka() {
		// GIVEN
		MonitoringEvent event = new MonitoringEvent();
		event.setEntity("bddf");
		event.setTrigram("bsy");
		// WHEN
		producer.sendEvent(event);
		// THEN
		assertThat(firstJsonRecord()).isEqualTo(MAPPER.toJson(event));
	}

	private String firstJsonRecord() {
		return KAFKA.getKafkaTestUtils().consumeAllRecordsFromTopic(TOPIC_NAME, StringDeserializer.class, StringDeserializer.class)
			.stream()
			.findFirst()
			.map(record -> record.value()).orElse(StringUtils.EMPTY);
	}
	
	private static byte[] createConfig() throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Properties props = new Properties();
		props.setProperty(TOPIC_KEY, TOPIC_NAME);
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
		Op op5 = Op.create(pathBuilder.toString(), data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zookeeper.multi(Arrays.asList(op1, op2, op3, op4, op5));
	}
}

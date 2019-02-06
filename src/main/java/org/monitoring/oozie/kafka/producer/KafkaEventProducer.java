package org.monitoring.oozie.kafka.producer;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.oozie.util.XLog;
import org.monitoring.oozie.kafka.event.MonitoringEvent;
import org.monitoring.oozie.zookeeper.FlowRouter;

import com.google.gson.Gson;

public class KafkaEventProducer {
	private static final XLog LOGGER = XLog.getLog(KafkaEventProducer.class);
	private static final Gson MAPPER = new Gson();
	
	public static final String KAFKA_CONFIG_PREFIX = "job.listener.kafka.config.";

	private KafkaProducer<String, String> producer;
	private FlowRouter flowRouter;

	public KafkaEventProducer(String bootstrapServer, String zookeeperServer) {
		initKafkaProducer(bootstrapServer);
		initRouter(zookeeperServer);
	}

	private void initRouter(String zookeeperServer) {
		flowRouter = new FlowRouter(zookeeperServer);
	}

	private void initKafkaProducer(String bootstrapServer) {
		LOGGER.info("Init kafka producer");
		Optional<Properties> props = Optional.ofNullable(bootstrapServer)
			.map(bs -> {
				Properties properties = new Properties();
				properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
				properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				return properties;
			});
		producer = props.map(p -> new KafkaProducer<String, String>(p)).orElse(null);
		LOGGER.info("Kafka event producer created");
	}

	public void sendEvent(MonitoringEvent event) {
		final String jsonEvent = MAPPER.toJson(event);
		
		flowRouter.getEventTopic(event)
			.map(topic -> new ProducerRecord<String, String>(topic, jsonEvent))
			.flatMap(this::produceToKafka)
			.flatMap(this::getMetadata)
			.ifPresent(meta -> {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Event sent to : " + meta.toString());
				}
			});
	}
	
	private Optional<Future<RecordMetadata>> produceToKafka(ProducerRecord<String, String> record){
		return Optional.ofNullable(producer)
			.map(p -> p.send(record));
	}
	
	private Optional<RecordMetadata> getMetadata(Future<RecordMetadata> future){
		try {
			return Optional.of(future.get());
		} catch (InterruptedException | ExecutionException e) {
			LOGGER.error(e);
			return Optional.empty();
		}
	}
}

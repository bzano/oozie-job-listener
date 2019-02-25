package org.monitoring.oozie.kafka.producer;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.oozie.util.XLog;
import org.monitoring.oozie.kafka.event.MonitoringEvent;
import org.monitoring.oozie.zookeeper.FlowConfig;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class KafkaEventProducer {
	private static final XLog LOGGER = XLog.getLog(KafkaEventProducer.class);
	private static final Gson MAPPER = new Gson();
	private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	public static final String KAFKA_CONFIG_PREFIX = "job.listener.kafka.config.";
	public static final String EVENT_CONFIG_PREFIX = "job.listener.event.config.";
	public static final String KAFKA_TOPIC_CONF = "job.listener.kafka.topic";

	private static final Cache<String, KafkaProducer<String, String>> CACHE;

	static {
		CACHE = CacheBuilder.newBuilder().expireAfterWrite(15, TimeUnit.MINUTES).removalListener(
				(RemovalListener<String, KafkaProducer<String, String>>) notification -> notification.getValue().close())
				.maximumSize(100)
				.build();
	}

	private FlowConfig flowConfig;
	private String bootstrapServers;

	public KafkaEventProducer(String bootstrapServers, String zookeeperServer, String zkPathPrefix) {
		this.bootstrapServers = bootstrapServers;
		initFlowConfig(zookeeperServer, zkPathPrefix);
	}

	private void initFlowConfig(String zookeeperServer, String zkPathPrefix) {
		flowConfig = new FlowConfig(zookeeperServer, zkPathPrefix);
	}

	private synchronized Optional<KafkaProducer<String, String>> getKafkaProducer(String jobName) {
		KafkaProducer<String, String> producer = CACHE.getIfPresent(jobName);
		return Optional.ofNullable(Optional.ofNullable(producer).orElseGet(() -> {
			LOGGER.info("Init kafka producer");
			Optional<Properties> props = Optional.ofNullable(bootstrapServers).map(bs -> {
				Properties properties = createJobKafkaProducerProps(jobName).orElseGet(Properties::new);
				properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
				properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
				return properties;
			});
			return props.map(p -> {
				KafkaProducer<String, String> newProducer = new KafkaProducer<String, String>(p);
				CACHE.put(jobName, newProducer);
				return newProducer;
			}).orElse(null);
		}));
	}

	public Optional<Properties> createJobKafkaProducerProps(String jobName) {
		Optional<Properties> jobProps = flowConfig.getEventConfiguration(jobName);
		return jobProps.map(p -> p.keySet().stream().map(Object::toString))
				.map(propsStream -> propsStream.filter(prop -> prop.indexOf(KAFKA_CONFIG_PREFIX) == 0))
				.map(kafkaPropsStream -> kafkaPropsStream
						.collect(Collectors.toMap(propKey -> propKey.replaceAll(KAFKA_CONFIG_PREFIX, StringUtils.EMPTY),
								propKey -> jobProps.get().get(propKey))))
				.map(map -> {
					Properties p = new Properties();
					p.putAll(map);
					return p;
				});
	}

	public void sendEvent(MonitoringEvent event) {
		flowConfig.getEventConfiguration(event.getJobName()).flatMap(props -> eventToRecord(event, props))
				.flatMap(record -> produceToKafka(event.getJobName(), record)).flatMap(f -> getMetadata(f, event.getJobName()))
				.ifPresent(meta -> {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Event sent to : " + meta.toString());
					}
				});
	}

	private Optional<ProducerRecord<String, String>> eventToRecord(MonitoringEvent event, Properties props) {
		String topic = props.getProperty(KAFKA_TOPIC_CONF);
		if (topic != null) {
			MonitoringEvent newEvent = updateEventConfig(event, props);

			Map<String, Object> log = new HashMap<>();

			log.put("message", newEvent);
			log.put("date", ISO_DATE_FORMAT.format(System.currentTimeMillis()));

			String jsonEvent = MAPPER.toJson(log);
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("EVENT TO SEND [" + jsonEvent + "]");
			}
			return Optional.of(new ProducerRecord<String, String>(topic, jsonEvent));
		}
		LOGGER.warn("No topic is defined for job (" + event.getJobName() + ") " + props.toString());
		return Optional.empty();
	}

	private MonitoringEvent updateEventConfig(MonitoringEvent event, Properties jobConfig) {
		Map<String, Object> eventProps = jobConfig.keySet().stream().map(Object::toString)
				.filter(key -> key.indexOf(EVENT_CONFIG_PREFIX) == 0)
				.collect(Collectors.toMap(key -> key.toLowerCase().replace(EVENT_CONFIG_PREFIX, StringUtils.EMPTY),
						key -> jobConfig.get(key)));

		Arrays.stream(event.getClass().getDeclaredFields()).map(f -> Pair.of(f, getFieldName(f)))
				.filter(t -> eventProps.get(t.getRight()) != null)
				.map(t -> Pair.of(t.getLeft(), eventProps.get(t.getRight()))).forEach(t -> {
					Object value = t.getRight();
					Field field = t.getLeft();
					populateField(event, field, value);
				});
		return event;
	}

	private void populateField(MonitoringEvent event, Field field, Object value) {
		field.setAccessible(true);
		try {
			field.set(event, value);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			LOGGER.error(e);
		}
		field.setAccessible(false);
	}

	private String getFieldName(Field field) {
		return Arrays.stream(field.getDeclaredAnnotationsByType(SerializedName.class)).findFirst()
				.map(SerializedName::value).map(String::toLowerCase).orElse(field.getName());
	}

	private Optional<Future<RecordMetadata>> produceToKafka(String jobName, ProducerRecord<String, String> record) {
		return getKafkaProducer(jobName).flatMap(p -> send(p, record));
	}
	
	private Optional<Future<RecordMetadata>> send(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
		try {
			return Optional.ofNullable(producer.send(record));
		}catch(KafkaException te) {
			LOGGER.error("Kafka error " + te.getMessage());
			return Optional.empty();
		}
	}

	private Optional<RecordMetadata> getMetadata(Future<RecordMetadata> future, String jobName) {
		try {
			return Optional.of(future.get());
		} catch (InterruptedException | ExecutionException e) {
			LOGGER.error("Kafka future error (" + jobName + ") " + e.getMessage());
			return Optional.empty();
		}
	}
}

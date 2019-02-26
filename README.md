## Oozie kafka job listener

This module allows to send monitoring metrics into Kafka, this solution connects directly to Oozie by implementing Oozie JobEventListener. This has the advantage of direct instrumentation

### Jar preparation

After cloning the project locally, run

```
mvn clean package
```
This will generate a jar (oozie-job-listener-0.0.1) in the target directory

### Installation

Put the jar into oozie lib directory

Add or update this properties these properties in oozie-site.xml file

```xml
<property>
    <name>oozie.services.ext</name>
    <value>
        ...
        org.apache.oozie.service.EventHandlerService,
    </value>
</property>

<property>
    <name>oozie.service.EventHandlerService.event.listeners</name>
    <value>org.monitoring.oozie.kafka.listener.KafkaJobEventListener</value>
</property>

<property>
    <name>oozie.job.listener.kafka.bootstrap.servers</name>
    <value>kafka:9092</value>
</property>

<property>
    <name>oozie.job.listener.zookeeper</name>
    <value>zookeeper:2181</value>
</property>

<property>
    <name>oozie.job.listener.zookeeper.path.prefix</name>
    <value>ZOOKEEPER_APPS_PREFIX_PATH</value>
</property>
```

#### Log4j configuration (optional)
You can also update the log4j configuration by editing the log4j.properties file

```
log4j.appender.oozieKafka=org.apache.log4j.rolling.RollingFileAppender
log4j.appender.oozieKafka.RollingPolicy=org.apache.oozie.util.OozieRollingPolicy
log4j.appender.oozieKafka.File=${oozie.log.dir}/oozie-kafka-listener.log
log4j.appender.oozieKafka.Append=true
log4j.appender.oozieKafka.layout=org.apache.log4j.PatternLayout
log4j.appender.oozieKafka.layout.ConversionPattern=%d{ISO8601} %5p %c{1}:%L - SERVER[${oozie.instance.id}] %m%n
log4j.appender.oozieKafka.RollingPolicy.FileNamePattern=${log4j.appender.oozieKafka.File}-%d{yyyy-MM-dd-HH}
log4j.appender.oozieKafka.RollingPolicy.MaxHistory=720

log4j.logger.org.monitoring.oozie=INFO, oozieKafka
```

Restart oozie

### Jobs Configuration

To configure the job monitoring, we have to create a node in zookeeper, this node will contain all needed configurations to start monitoring the job, so for example if we had a job named XXX we will need to create a Zookeeper node in :
```
ZOOKEEPER_APPS_PREFIX_PATH/XXX
```
Some configurations are optional some are not, for example

```
job.listener.kafka.topic=TOPIC_NAME

job.listener.event.config.irt=APP_IRT_CODE
job.listener.event.config.trigram=APP_TRIGRAMM_CODE
job.listener.event.config.app_mon=APP_MONITORING_CODE
job.listener.event.config.component=SPARK_FOR_EXAMPLE
job.listener.event.config.entity=THE_ENTITY_WHICH_IT_BELONGS_TO
job.listener.event.config.business=APP_BUSINESS
```
These properties are all mandatory

We can also put some Kafka configurations like "acks" or "client.id" (we juste need to prefix this conf with job.listener.kafka.config.), so in real life it would be :

```
job.listener.kafka.config.acks=2
job.listener.kafka.config.client.id=client-1
```

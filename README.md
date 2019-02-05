## oozie kafka job listener

Ce module permet l'envoie des metrics de monitoring dans kafka, cette implémentation se connecte directement à Oozie en implémentant Oozie JobEventListener. Cela a l'avantage d'une instrumentation directe

### Préparation du jar

Apres avoir cloner le projet en local lancer la commande maven

```
mvn clean install
```

### Installation

1. Un jar (oozie-job-listener-0.0.1) va être générer dans le répertoire target
2. Ajouter le jar dans le répertoire lib d'oozie
3. Configuration oozie

Ajouter ou modifier la propriété oozie.services.ext dans le fichier oozie-site.xml

```xml
<property>
    <name>oozie.services.ext</name>
    <value>
        ...
        org.apache.oozie.service.EventHandlerService,
    </value>
</property>
```

Ajouter une autre propriété

```xml
<property>
    <name>oozie.service.EventHandlerService.event.listeners</name>
    <value>org.monitoring.oozie.kafka.listener.KafkaJobEventListener</value>
</property>
```
Redémarrer oozie
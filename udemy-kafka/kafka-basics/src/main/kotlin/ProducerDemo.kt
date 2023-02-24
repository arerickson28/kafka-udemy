import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import org.slf4j.Logger

class ProducerDemo {

    fun produce() {
        val logger: Logger = LoggerFactory.getLogger(javaClass)
        //create producer properties
        var properties:Properties = Properties()
        logger.info("hello, I am a kafka producer")
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        //create the producer
        val kafkaProducer = KafkaProducer<String, String>(properties)
        // create a producer record
        val producerRecord = ProducerRecord<String, String>("demo_kotlin", "hello again, Kafka!")
        //send the data -- asynchronous
        kafkaProducer.send(producerRecord)
        //flush data - synchronous
        kafkaProducer.flush()
        // flush and close producer
        kafkaProducer.close()
    }
}

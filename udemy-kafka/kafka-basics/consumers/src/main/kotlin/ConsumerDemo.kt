import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import java.util.*

class ConsumerDemo {

    fun consume() {
        val logger: Logger = LoggerFactory.getLogger(javaClass)
        //create consumer properties
        var properties:Properties = Properties()
        logger.info("hello, I am a kafka consumer")

        val groupId = "my-kotlin-app"

        val topic = "demo_kotlin"

//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty("bootstrap.servers","127.0.0.1:9092" )

        //create consumer configs
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.getName())
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.getName())
        properties.setProperty("key.deserializer", StringDeserializer::class.java.getName())
        properties.setProperty("value.deserializer", StringDeserializer::class.java.getName())

        //set consumer id
        properties.setProperty("group.id", groupId)
        properties.setProperty("auto.offset.reset", "earliest")

        //create a consumer
        val kafkaConsumer = KafkaConsumer<String, String>(properties)

        //subscribe to a topic
        kafkaConsumer.subscribe(listOf(topic))

        //poll for data (infinite loop)
        while (true) {
            logger.info("Polling")
            val consumerRecords: ConsumerRecords<String, String> = kafkaConsumer.poll(1000)

            for (record: ConsumerRecord<String, String> in consumerRecords) {
                logger.info("Key: " + record.key() + ", Value: " + record.value())
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset())
            }
        }
    }
}

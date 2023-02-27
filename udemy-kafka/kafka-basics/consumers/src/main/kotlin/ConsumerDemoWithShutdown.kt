import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import java.util.*

class ConsumerDemoWithShutdown {

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

        //get a reference to the main thread
        val mainThread:Thread = Thread.currentThread()

        //add the shutdown hook
        Runtime.getRuntime().addShutdownHook(Thread() {
            run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...")
                kafkaConsumer.wakeup()

                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join()
                } catch (e: InterruptedException) {
                    e.printStackTrace()
                }
            }
        })

        try {
            //subscribe to a topic
            kafkaConsumer.subscribe(listOf(topic))

            //poll for data (infinite loop)
            while (true) {
//                logger.info("Polling")
                val consumerRecords: ConsumerRecords<String, String> = kafkaConsumer.poll(1000)

                for (record: ConsumerRecord<String, String> in consumerRecords) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value())
                    logger.info("Partition: " + record.partition() + ", Offset: " + record.offset())
                }
            }
        } catch (e:WakeupException) {
            logger.info("Consumer is starting to shut down")
        } catch (e: Exception) {
            logger.error("Unexpected error in consumer", e)
        } finally {
            //close the consumer, this will also commit the offsets
            kafkaConsumer.close()
            logger.info("The consumer is now gracefully shut down")
        }

    }
}

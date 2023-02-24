import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import org.slf4j.Logger
import java.lang.Exception

class ProducerDemoCallbacks {

    fun produce() {
        val logger: Logger = LoggerFactory.getLogger(javaClass)
        //create producer properties
        var properties:Properties = Properties()
        logger.info("hello, I am a kafka producer")
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())

        properties.setProperty("batch.size", "400")

        //properties.setProperty("partitioner.class", RoundRobinPartitioner::class.java.getName())
        //create the producer
        val kafkaProducer = KafkaProducer<String, String>(properties)

        for(j in 10 downTo 0 ) {
            // create a producer record
            for (i in 30 downTo 0) {
                val producerRecord = ProducerRecord<String, String>("demo_kotlin",
                    "hello again, number: $i Kafka with Kotlin!"
                )
                //send the data -- asynchronous
                kafkaProducer.send(producerRecord, Callback() {
                        metadata, exception ->  run {
                    //                //Executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        ////                   The record was successfully sent
                        logger.info("\n" +
                                "received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n"

                        )
                    } else {
                        logger.error("Error while producing", exception)
                    }
                }
                })
            }

            try {
                Thread.sleep(500)
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }

        }


        //flush data - synchronous
        kafkaProducer.flush()
        // flush and close producer
        kafkaProducer.close()
    }
}

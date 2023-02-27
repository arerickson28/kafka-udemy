fun main(args: Array<String>) {
    println("Hello World!")
//    val myConsumer: ConsumerDemo = ConsumerDemo()
    val myConsumerWithShutdown: ConsumerDemoWithShutdown = ConsumerDemoWithShutdown()
//    val mySecondConsumerWithShutdown: ConsumerDemoWithShutdown = ConsumerDemoWithShutdown()
    myConsumerWithShutdown.consume()
//    mySecondConsumerWithShutdown.consume()
}
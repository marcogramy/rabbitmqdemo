package com.example;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class RabbitMqDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitMqDemoApplication.class, args);
    }

    @Value("${rabbitmq.host}")
    private String host;

    @Value("${rabbitmq.port}")
    private int port;

    @Value("${rabbitmq.username}")
    private String username;

    @Value("${rabbitmq.password}")
    private String password;

    @Value("${rabbitmq.cache.channel.size}")
    private int cacheChannelSize;

    @Value("${rabbitmq.queue.name}")
    private String queueName;

    @Value("${rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${rabbitmq.routing.key}")
    private String routingKey;

    @Value("${producer.threads}")
    private int producerThreads;

    @Value("${consumer.prefetch}")
    private int prefetchCount;

    @Value("${consumer.minCount}")
    private int consumerCount;

    @Value("${consumer.maxCount}")
    private int consumerMaxCount;

    @Value("${consumer.batchSize}")
    private int batchSize;

    @Bean
    public Queue queue() {
        return new Queue(queueName, true);
    }

    @Bean
    public DirectExchange exchange() {
        return new DirectExchange(exchangeName);
    }

    @Bean
    public Binding binding(Queue queue, DirectExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(routingKey);
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setChannelCacheSize(cacheChannelSize);
        return connectionFactory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(CachingConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setChannelTransacted(true);

        rabbitTemplate.setReturnsCallback(returnedMessage -> { // Log undeliverable message
            System.out.println("Undelivered message returned from RabbitMq exchange: " + returnedMessage);
        });

        return rabbitTemplate;
    }

    @Bean
    public RabbitListenerContainerFactory<SimpleMessageListenerContainer> rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setPrefetchCount(prefetchCount);
        factory.setConcurrentConsumers(consumerCount);
        factory.setMaxConcurrentConsumers(consumerMaxCount);
        factory.setBatchSize(batchSize);
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        factory.setContainerCustomizer(c -> c.setShutdownTimeout(20 * 1000L)); // Milliseconds
        factory.setMissingQueuesFatal(true);
        factory.setMismatchedQueuesFatal(false);
        factory.setDefaultRequeueRejected(false);
        factory.setChannelTransacted(true);
        return factory;
    }

    // Shutdown hook
    @EventListener
    public void handleContextClosedEvent(ContextClosedEvent event) {
        RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry =
                event.getApplicationContext().getBean(RabbitListenerEndpointRegistry.class);

        // Stop RabbitMq listeners
        rabbitListenerEndpointRegistry.getListenerContainerIds().forEach((id) -> {
            SimpleMessageListenerContainer listener = (SimpleMessageListenerContainer)rabbitListenerEndpointRegistry.getListenerContainer(id);
            System.out.printf("Shutting down %s with %d consumer/s%n", id, listener.getActiveConsumerCount());
            listener.stop();
        });

        // Prevent "Rejecting received message" warning (see https://stackoverflow.com/questions/68995828/stop-rabbitmq-connection-in-spring-boot)
        CachingConnectionFactory cachingConnectionFactory =
                event.getApplicationContext().getBean(CachingConnectionFactory.class);
        cachingConnectionFactory.resetConnection();
    }

    // PRODUCER
    @Bean
    public CommandLineRunner producerRunner(RabbitTemplate rabbitTemplate) {
        return args -> {
            ExecutorService executorService = Executors.newFixedThreadPool(producerThreads);
            for (int i = 0; i < producerThreads; i++) {
                executorService.submit(() -> {
                    while (true) {
                        rabbitTemplate.convertAndSend(exchangeName, routingKey, UUID.randomUUID() + " " + System.currentTimeMillis());
                        try {
                            Thread.sleep(200L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            }
        };
    }

    // CONSUMER
    @RabbitListener(queues = "${rabbitmq.queue.name}")
    public void consumeMessage(Message message) {
        String body = new String(message.getBody());
        boolean redelivered = message.getMessageProperties().isRedelivered();

        if (redelivered) {
            System.out.println("Consumed: " + body + ", Redelivered: " + redelivered);
        }

        // Add delay while processing message
        try {
            Thread.sleep(500L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

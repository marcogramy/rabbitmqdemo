package com.example;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class RabbitMqDemoApplication {

    private CachingConnectionFactory connectionFactory;

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
        this.connectionFactory = connectionFactory;
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setChannelTransacted(true);
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
        return factory;
    }

    @Bean
    public CommandLineRunner producerRunner(RabbitTemplate rabbitTemplate) {
        return args -> {
            ExecutorService executorService = Executors.newFixedThreadPool(producerThreads);
            for (int i = 0; i < producerThreads; i++) {
                executorService.submit(() -> {
                    while (true) {
                        rabbitTemplate.convertAndSend(exchangeName, routingKey, "Test Message " + System.currentTimeMillis());
                        try {
                            Thread.sleep(1); // 1000 messages per second per thread
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            }
        };
    }

    @RabbitListener(queues = "${rabbitmq.queue.name}")
    public void consumeMessage(String message) {
        System.out.println("Consumed: " + message);
    }
}

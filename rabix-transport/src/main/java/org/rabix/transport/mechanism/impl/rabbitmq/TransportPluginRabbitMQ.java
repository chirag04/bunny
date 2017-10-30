package org.rabix.transport.mechanism.impl.rabbitmq;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.Configuration;
import org.rabix.common.json.BeanSerializer;
import org.rabix.common.json.processor.BeanProcessorException;
import org.rabix.transport.mechanism.TransportPlugin;
import org.rabix.transport.mechanism.TransportPluginException;
import org.rabix.transport.mechanism.TransportPluginType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

public class TransportPluginRabbitMQ implements TransportPlugin<TransportQueueRabbitMQ> {

  public static final String DEFAULT_ENCODING = "UTF-8";
  public static final int RETRY_TIMEOUT = 5; // Seconds

  private static final Logger logger = LoggerFactory.getLogger(TransportPluginRabbitMQ.class);

  private Connection connection;
  private ConnectionFactory factory;
  private Configuration configuration;

  private ConcurrentMap<TransportQueueRabbitMQ, Receiver<?>> receivers = new ConcurrentHashMap<>();

  private ExecutorService receiverThreadPool = Executors.newCachedThreadPool();

  private boolean durable;
  
  private int threads;
  private Channel channel1, channel2;

  public TransportPluginRabbitMQ(Configuration configuration) throws TransportPluginException {
    this.configuration = configuration;
    threads = configuration.getInt("rabbitmq.threads",16);
    while (true) {
      try {
        initConnection();
        logger.info("TransportPluginRabbitMQ created");
        break;
      } catch (TransportPluginException e1) {
        logger.info("RabbitMQ connect failed. Trying again in {} seconds.", RETRY_TIMEOUT);
      }

      try {
        Thread.sleep(RETRY_TIMEOUT * 1000);
      } catch (InterruptedException e1) {
        // Ignore
      }
    }
  }

  public void initConnection() throws TransportPluginException {
    factory = new ConnectionFactory();

    try {
      if (TransportConfigRabbitMQ.isDev(configuration)) {
        factory.setHost(TransportConfigRabbitMQ.getHost(configuration));
      } else {
        factory.setHost(TransportConfigRabbitMQ.getHost(configuration));
        factory.setPort(TransportConfigRabbitMQ.getPort(configuration));
        factory.setUsername(TransportConfigRabbitMQ.getUsername(configuration));
        factory.setPassword(TransportConfigRabbitMQ.getPassword(configuration));
        factory.setAutomaticRecoveryEnabled(true);
        factory.setVirtualHost(TransportConfigRabbitMQ.getVirtualhost(configuration));
        if (TransportConfigRabbitMQ.isSSL(configuration)) {
          factory.useSslProtocol();
        }
      }
      durable = TransportConfigRabbitMQ.durableQueues(configuration);
      connection = factory.newConnection(Executors.newFixedThreadPool(threads));
      this.channel1 = connection.createChannel();
      this.channel2 = connection.createChannel();
    } catch (Exception e) {
      throw new TransportPluginException("Failed to initialize TransportPluginRabbitMQ", e);
    }
  }

  /**
   * {@link TransportPluginRabbitMQ} extension for Exchange initialization
   */
  public void initializeExchange(String exchange, String type) throws TransportPluginException {
    try {
      channel1.exchangeDeclare(exchange, type, durable);
    } catch (Exception e) {
      throw new TransportPluginException("Failed to declare RabbitMQ exchange " + exchange + " and type " + type, e);
    }
  }

  public void initQueue(TransportQueueRabbitMQ queue) throws TransportPluginException {
    try {
      channel1.queueDeclare(queue.getQueueName(), durable, false, false, null);
      channel1.queueBind(queue.getQueueName(), queue.getExchange(), queue.getRoutingKey());
    } catch (Exception e) {
      throw new TransportPluginException("Failed to bind RabbitMQ queue " + queue, e);
    }
  }

  /**
   * {@link TransportPluginRabbitMQ} extension for Exchange initialization
   */
  public void deleteExchange(String exchange) throws TransportPluginException {
    try {
      channel1.exchangeDelete(exchange, true);
    } catch (Exception e) {
      throw new TransportPluginException("Failed to delete RabbitMQ exchange " + exchange, e);
    } finally {
    }
  }

  public void deleteQueue(String queue) {
    Channel channel = null;
    try {
      channel = connection.createChannel();
      channel.queueDelete(queue);
    } catch (Exception e) {
      logger.info("Failed to delete RabbitMQ queue " + queue, e);
    } finally {
      if (channel != null) {
        try {
          channel.close();
        } catch (Exception ignore) {
        }
      }
    }
  }

  @Override
  public <T> ResultPair<T> send(TransportQueueRabbitMQ queue, T entity) {
    String payload = BeanSerializer.serializeFull(entity);
    while (true) {
      try {
        channel1.basicPublish(queue.getExchange(), queue.getRoutingKey(), MessageProperties.PERSISTENT_TEXT_PLAIN, payload.getBytes(DEFAULT_ENCODING));
        return ResultPair.success();
      } catch (Exception e) {
        logger.error("Failed to send a message to " + queue, e);
        while (true) {
          try {
            Thread.sleep(RETRY_TIMEOUT * 1000);
          } catch (InterruptedException e1) {
            // Ignore
          }
          try {
            initConnection();
            logger.info("Reconnected to {}", queue);
            break;
          } catch (TransportPluginException e1) {
            logger.info("Sender reconnect failed. Trying again in {} seconds.", RETRY_TIMEOUT);
          }
        }
      }
    }
  }

  @Override
  public TransportPluginType getType() {
    return TransportPluginType.RABBIT_MQ;
  }

  @Override
  public <T> void startReceiver(TransportQueueRabbitMQ sourceQueue, Class<T> clazz, ReceiveCallback<T> receiveCallback, ErrorCallback errorCallback) {
    final Receiver<T> receiver = new Receiver<>(clazz, receiveCallback, errorCallback, sourceQueue);
    receivers.put(sourceQueue, receiver);
    receiverThreadPool.submit(new Runnable() {
      @Override
      public void run() {
        receiver.start();
      }
    });
  }

  @Override
  public void stopReceiver(TransportQueueRabbitMQ queue) {
    Receiver<?> receiver = receivers.get(queue);
    if (receiver != null) {
      receiver.stop();
      receivers.remove(queue);
    }
  }

  private class Receiver<T> {

    private Class<T> clazz;
    private ReceiveCallback<T> callback;
    private ErrorCallback errorCallback;

    private TransportQueueRabbitMQ queue;

    private volatile boolean isStopped = false;

    public Receiver(Class<T> clazz, ReceiveCallback<T> callback, ErrorCallback errorCallback, TransportQueueRabbitMQ queue) {
      this.clazz = clazz;
      this.callback = callback;
      this.errorCallback = errorCallback;
      this.queue = queue;
    }

    void start() {
      DefaultConsumer consumer = null;
      String queueName = queue.getQueueName();
      try {
        consumer = new DefaultConsumer(channel2) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            String message = new String(body, "UTF-8");
            try {
              callback.handleReceive(BeanSerializer.deserialize(message, clazz));
            } catch (TransportPluginException e) {
              throw new IOException();
            }
            channel2.basicAck(envelope.getDeliveryTag(), false);
          }
        };
        channel2.basicQos(threads*50);
        channel2.basicConsume(queueName, false, consumer);
      } catch (BeanProcessorException e) {
        logger.error("Failed to deserialize message payload", e);
        errorCallback.handleError(e);
      } catch (Exception e) {
        while (!isStopped) {
          logger.error("Failed to receive a message from " + queue, e);
          try {
            logger.error("Failed to receive a message from " + queue, e);
            Thread.sleep(RETRY_TIMEOUT * 1000);
          } catch (InterruptedException e1) {
            // Ignore
          }
          try {
            initConnection();
            logger.info("Reconnected to {}", queueName);
            break;
          } catch (TransportPluginException e1) {
            logger.info("Receiver reconnect failed. Trying again in {} seconds.", RETRY_TIMEOUT);
          }
        }
      }
    }
    void stop() {
      isStopped = true;
    }
  }

}

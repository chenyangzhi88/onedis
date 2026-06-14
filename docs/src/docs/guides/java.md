# Using Rudis in Java

This guide explains how to use Rudis in Java applications with the mature Jedis client library.

## Installation

### Maven

Add the following dependency to your `pom.xml` file:

```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>4.3.1</version>
</dependency>
```

### Gradle

Add the following to your `build.gradle` file:

```gradle
implementation 'redis.clients:jedis:4.3.1'
```

## Basic Usage

Here's a simple example of how to connect to Rudis and perform operations:

```java
import redis.clients.jedis.Jedis;

public class RudisExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            // Set a key
            jedis.set("key", "value");
            
            // Get a key
            String value = jedis.get("key");
            System.out.println("Value: " + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## Connection Management

For production applications, use connection pooling:

```java
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RudisPoolExample {
    public static void main(String[] args) {
        // Configure connection pool
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        
        // Create connection pool
        try (JedisPool jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6379)) {
            // Get connection from pool
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.set("key", "value");
                String value = jedis.get("key");
                System.out.println("Value: " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## Error Handling

Jedis provides robust error handling mechanisms:

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class RudisErrorHandlingExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            try {
                jedis.set("key", "value");
                System.out.println("Key set successfully");
            } catch (JedisException e) {
                System.err.println("Failed to set key: " + e.getMessage());
                // Handle specific Jedis exceptions
            }
        } catch (Exception e) {
            System.err.println("Connection failed: " + e.getMessage());
        }
    }
}
```

## Advanced Usage

### Using Pipelining for Batch Operations

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RudisPipelineExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            // Create pipeline
            Pipeline pipeline = jedis.pipelined();
            
            // Add commands to pipeline
            pipeline.set("key1", "value1");
            pipeline.set("key2", "value2");
            pipeline.get("key1");
            pipeline.get("key2");
            
            // Execute all commands in pipeline
            List<Object> results = pipeline.syncAndReturnAll();
            
            System.out.println("Results: " + results);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Using Transactions

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RudisTransactionExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            // Start transaction
            Transaction transaction = jedis.multi();
            
            // Add commands to transaction
            transaction.set("key1", "value1");
            transaction.set("key2", "value2");
            transaction.get("key1");
            
            // Execute transaction
            List<Object> results = transaction.exec();
            
            System.out.println("Transaction results: " + results);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Using Publish/Subscribe

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class RudisPubSubExample {
    public static void main(String[] args) {
        // Publisher
        new Thread(() -> {
            try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
                for (int i = 0; i < 10; i++) {
                    jedis.publish("channel1", "Message " + i);
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        
        // Subscriber
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    System.out.println("Received message: " + message + " from channel: " + channel);
                }
            }, "channel1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```
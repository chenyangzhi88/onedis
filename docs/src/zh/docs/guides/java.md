# 在 Java 中使用 Rudis

本指南介绍了如何在 Java 应用程序中使用 Rudis，使用成熟的 Jedis 客户端库。

## 安装

### Maven

将以下依赖项添加到您的 `pom.xml` 文件中：

```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>4.3.1</version>
</dependency>
```

### Gradle

将以下内容添加到您的 `build.gradle` 文件中：

```gradle
implementation 'redis.clients:jedis:4.3.1'
```

## 基本用法

以下是一个简单的示例，展示如何连接到 Rudis 并执行操作：

```java
import redis.clients.jedis.Jedis;

public class RudisExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            // 设置一个键
            jedis.set("key", "value");
            
            // 获取一个键
            String value = jedis.get("key");
            System.out.println("值: " + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## 连接管理

对于生产应用程序，请使用连接池：

```java
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RudisPoolExample {
    public static void main(String[] args) {
        // 配置连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        
        // 创建连接池
        try (JedisPool jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6379)) {
            // 从连接池获取连接
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.set("key", "value");
                String value = jedis.get("key");
                System.out.println("值: " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## 错误处理

Jedis 提供了健壮的错误处理机制：

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class RudisErrorHandlingExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            try {
                jedis.set("key", "value");
                System.out.println("键设置成功");
            } catch (JedisException e) {
                System.err.println("设置键失败: " + e.getMessage());
                // 处理特定的 Jedis 异常
            }
        } catch (Exception e) {
            System.err.println("连接失败: " + e.getMessage());
        }
    }
}
```

## 高级用法

### 使用管道执行批量操作

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RudisPipelineExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            // 创建管道
            Pipeline pipeline = jedis.pipelined();
            
            // 添加命令到管道
            pipeline.set("key1", "value1");
            pipeline.set("key2", "value2");
            pipeline.get("key1");
            pipeline.get("key2");
            
            // 执行管道中的所有命令
            List<Object> results = pipeline.syncAndReturnAll();
            
            System.out.println("结果: " + results);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### 使用事务

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RudisTransactionExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            // 开始事务
            Transaction transaction = jedis.multi();
            
            // 添加命令到事务
            transaction.set("key1", "value1");
            transaction.set("key2", "value2");
            transaction.get("key1");
            
            // 执行事务
            List<Object> results = transaction.exec();
            
            System.out.println("事务结果: " + results);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### 使用发布/订阅

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class RudisPubSubExample {
    public static void main(String[] args) {
        // 发布者
        new Thread(() -> {
            try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
                for (int i = 0; i < 10; i++) {
                    jedis.publish("channel1", "消息 " + i);
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        
        // 订阅者
        try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    System.out.println("收到消息: " + message + " 来自频道: " + channel);
                }
            }, "channel1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```
# 在 Rust 中使用 Rudis

本指南介绍了如何在 Rust 应用程序中使用 Rudis，使用成熟的 redis-rs 客户端库。

## 安装

将以下内容添加到您的 `Cargo.toml` 文件中：

```toml
[dependencies]
redis = "0.23"
tokio = { version = "1", features = ["full"] }
```

## 基本用法

以下是一个简单的示例，展示如何连接到 Rudis 并执行操作：

```rust
use redis::{Client, Commands};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_connection()?;
    
    // 设置一个键
    redis::cmd("SET").arg("key").arg("value").execute(&mut con);
    
    // 获取一个键
    let value: String = redis::cmd("GET").arg("key").query(&mut con)?;
    println!("值: {}", value);
    
    Ok(())
}
```

## 连接管理

对于生产应用程序，请考虑使用连接池：

```rust
use redis::Client;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = Client::open("redis://127.0.0.1:6379/")?;
    
    // 创建连接池
    let mut conn = client.get_connection()?;
    
    // 执行命令
    redis::cmd("SET").arg("key").arg("value").execute(&mut conn);
    
    Ok(())
}
```

## 错误处理

redis-rs 提供了健壮的错误处理机制：

```rust
use redis::{Client, RedisResult};

#[tokio::main]
async fn main() {
    match Client::open("redis://127.0.0.1:6379/") {
        Ok(client) => {
            match client.get_connection() {
                Ok(mut con) => {
                    match redis::cmd("SET").arg("key").arg("value").execute(&mut con) {
                        Ok(_) => println!("键设置成功"),
                        Err(e) => eprintln!("设置键失败: {}", e),
                    }
                }
                Err(e) => eprintln!("获取连接失败: {}", e),
            }
        }
        Err(e) => eprintln!("连接失败: {}", e),
    }
}
```

## 高级用法

### 使用管道执行批量操作

```rust
use redis::{Client, PipelineCommands};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_connection()?;
    
    // 使用管道执行多个命令
    let (k1, k2): (i32, i32) = redis::pipe()
        .cmd("SET").arg("key_1").arg(42).ignore()
        .cmd("SET").arg("key_2").arg(43).ignore()
        .cmd("GET").arg("key_1")
        .cmd("GET").arg("key_2")
        .query(&mut con)?;
        
    println!("值1: {}, 值2: {}", k1, k2);
    
    Ok(())
}
```

### 使用异步客户端

```rust
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_async_connection().await?;
    
    // 异步设置键
    con.set("key", "value").await?;
    
    // 异步获取键
    let value: String = con.get("key").await?;
    println!("值: {}", value);
    
    Ok(())
}
```
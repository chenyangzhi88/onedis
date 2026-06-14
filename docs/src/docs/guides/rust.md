# Using Rudis in Rust

This guide explains how to use Rudis in Rust applications with the mature redis-rs client library.

## Installation

Add the following to your `Cargo.toml` file:

```toml
[dependencies]
redis = "0.23"
tokio = { version = "1", features = ["full"] }
```

## Basic Usage

Here's a simple example of how to connect to Rudis and perform operations:

```rust
use redis::{Client, Commands};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_connection()?;
    
    // Set a key
    redis::cmd("SET").arg("key").arg("value").execute(&mut con);
    
    // Get a key
    let value: String = redis::cmd("GET").arg("key").query(&mut con)?;
    println!("Value: {}", value);
    
    Ok(())
}
```

## Connection Management

For production applications, consider using a connection pool:

```rust
use redis::Client;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = Client::open("redis://127.0.0.1:6379/")?;
    
    // Create connection pool
    let mut conn = client.get_connection()?;
    
    // Execute commands
    redis::cmd("SET").arg("key").arg("value").execute(&mut conn);
    
    Ok(())
}
```

## Error Handling

redis-rs provides robust error handling mechanisms:

```rust
use redis::{Client, RedisResult};

#[tokio::main]
async fn main() {
    match Client::open("redis://127.0.0.1:6379/") {
        Ok(client) => {
            match client.get_connection() {
                Ok(mut con) => {
                    match redis::cmd("SET").arg("key").arg("value").execute(&mut con) {
                        Ok(_) => println!("Key set successfully"),
                        Err(e) => eprintln!("Failed to set key: {}", e),
                    }
                }
                Err(e) => eprintln!("Failed to get connection: {}", e),
            }
        }
        Err(e) => eprintln!("Connection failed: {}", e),
    }
}
```

## Advanced Usage

### Using Pipelining for Batch Operations

```rust
use redis::{Client, PipelineCommands};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_connection()?;
    
    // Use pipeline to execute multiple commands
    let (k1, k2): (i32, i32) = redis::pipe()
        .cmd("SET").arg("key_1").arg(42).ignore()
        .cmd("SET").arg("key_2").arg(43).ignore()
        .cmd("GET").arg("key_1")
        .cmd("GET").arg("key_2")
        .query(&mut con)?;
        
    println!("Value 1: {}, Value 2: {}", k1, k2);
    
    Ok(())
}
```

### Using Async Client

```rust
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_async_connection().await?;
    
    // Async set key
    con.set("key", "value").await?;
    
    // Async get key
    let value: String = con.get("key").await?;
    println!("Value: {}", value);
    
    Ok(())
}
```
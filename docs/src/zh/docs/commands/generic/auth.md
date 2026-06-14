# AUTH

用于验证服务器端连接密码。如果启用了密码保护，客户端在执行命令前需要通过 AUTH 命令进行身份验证。

## Syntax

```
AUTH password
```

## Return

Simple string reply: 
- 如果密码正确返回 OK
- 如果密码错误返回 ERR invalid password

## Examples

```
redis> AUTH secret
OK
redis> AUTH wrongpassword
ERR invalid password
```
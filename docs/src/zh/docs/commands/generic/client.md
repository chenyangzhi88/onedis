# CLIENT

CLIENT 命令用于获取或设置客户端连接的相关信息。

## Syntax

```
CLIENT subcommand [arguments]
```

## Subcommands

- SETINFO: 设置客户端库相关信息

## Return

Simple string reply: OK 或错误信息

## Examples

```
redis> CLIENT SETINFO lib-name redis
OK
redis> CLIENT SETINFO lib-ver 1.0
OK
```
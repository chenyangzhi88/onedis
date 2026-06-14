# EXPIRE

为给定的键设置过期时间（以秒为单位）。如果键不存在则返回 0，如果成功设置过期时间则返回 1。

## Syntax

```
EXPIRE key seconds
```

## Return

Integer reply: 1 如果设置了过期时间，0 如果键不存在。

## Examples

```
redis> SET mykey "Hello"
OK
redis> EXPIRE mykey 10
(integer) 1
redis> TTL mykey
(integer) 10
redis> SET mykey "Hello World"
OK
redis> TTL mykey
(integer) -1
```
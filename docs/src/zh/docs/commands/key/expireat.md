# EXPIREAT

为给定的键设置过期时间戳（以秒为单位的 Unix 时间戳）。如果键不存在则返回 0，如果成功设置过期时间则返回 1。

## Syntax

```
EXPIREAT key timestamp
```

## Return

Integer reply: 1 如果设置了过期时间，0 如果键不存在。

## Examples

```
redis> SET mykey "Hello"
OK
redis> EXPIREAT mykey 1293840000
(integer) 1
redis> TTL mykey
(integer) 1234567890
```
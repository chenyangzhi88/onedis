# PEXPIRE

为给定的键设置过期时间（以毫秒为单位）。如果键不存在则返回 0，如果成功设置过期时间则返回 1。

## Syntax

```
PEXPIRE key milliseconds
```

## Return

Integer reply: 1 如果设置了过期时间，0 如果键不存在。

## Examples

```
redis> SET mykey "Hello"
OK
redis> PEXPIRE mykey 1000
(integer) 1
redis> PTTL mykey
(integer) 1000
```
# PERSIST

移除给定键的过期时间，使键成为持久化的键。如果键不存在或没有设置过期时间，则返回 0。

## Syntax

```
PERSIST key
```

## Return

Integer reply: 1 如果成功移除了过期时间，0 如果键不存在或没有设置过期时间。

## Examples

```
redis> SET mykey "Hello"
OK
redis> EXPIRE mykey 10
(integer) 1
redis> TTL mykey
(integer) 10
redis> PERSIST mykey
(integer) 1
redis> TTL mykey
(integer) -1
```
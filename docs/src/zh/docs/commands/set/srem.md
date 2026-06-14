# SREM

从集合中移除一个或多个元素。

## Syntax

```
SREM key member [member ...]
```

## Return

Integer reply: 被成功移除的元素数量，不包括不存在的元素。

## Examples

```
redis> SADD myset "Hello"
(integer) 1
redis> SADD myset "World"
(integer) 1
redis> SREM myset "Hello"
(integer) 1
redis> SMEMBERS myset
1) "World"
```
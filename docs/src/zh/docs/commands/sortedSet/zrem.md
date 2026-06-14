# ZREM

从有序集合中移除一个或多个成员。

## Syntax

```
ZREM key member [member ...]
```

## Return

Integer reply: 被成功移除的成员数量，不包括不存在的成员。

## Examples

```
redis> ZADD myzset 1 "one"
(integer) 1
redis> ZADD myzset 2 "two"
(integer) 1
redis> ZADD myzset 3 "three"
(integer) 1
redis> ZREM myzset "two"
(integer) 1
redis> ZRANGE myzset 0 -1
1) "one"
2) "three"
```
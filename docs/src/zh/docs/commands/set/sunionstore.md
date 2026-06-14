# SUNIONSTORE

将给定所有集合的并集存储在指定的集合中。如果目标集合已存在，则会被覆盖。

## Syntax

```
SUNIONSTORE destination key [key ...]
```

## Return

Integer reply: 结果集合中的元素数量。

## Examples

```
redis> SADD myset1 "a"
(integer) 1
redis> SADD myset1 "b"
(integer) 1
redis> SADD myset1 "c"
(integer) 1
redis> SADD myset2 "c"
(integer) 1
redis> SADD myset2 "d"
(integer) 1
redis> SADD myset2 "e"
(integer) 1
redis> SUNIONSTORE myset3 myset1 myset2
(integer) 5
redis> SMEMBERS myset3
1) "a"
2) "b"
3) "c"
4) "d"
5) "e"
```
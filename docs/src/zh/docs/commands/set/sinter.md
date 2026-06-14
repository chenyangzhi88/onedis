# SINTER

返回给定所有集合的交集。

## Syntax

```
SINTER key [key ...]
```

## Return

Array reply: 交集的结果集，如果其中一个集合为空或不存在，则返回空数组。

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
redis> SINTER myset1 myset2
1) "c"
```
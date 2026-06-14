# SUNION

返回给定所有集合的并集。

## Syntax

```
SUNION key [key ...]
```

## Return

Array reply: 并集的结果集，结果集中不包含重复元素。

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
redis> SUNION myset1 myset2
1) "a"
2) "b"
3) "c"
4) "d"
5) "e"
```
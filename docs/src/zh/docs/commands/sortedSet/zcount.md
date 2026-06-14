# ZCOUNT

返回有序集合中指定分数区间内的成员数量。

## Syntax

```
ZCOUNT key min max
```

## Return

Integer reply: 分数区间内的成员数量。

## Examples

```
redis> ZADD myzset 1 "one"
(integer) 1
redis> ZADD myzset 2 "two"
(integer) 1
redis> ZADD myzset 3 "three"
(integer) 1
redis> ZCOUNT myzset 1 2
(integer) 2
```
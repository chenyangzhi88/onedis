# ZRANK

返回有序集合中指定成员的排名，按分数从小到大排序。

## Syntax

```
ZRANK key member
```

## Return

Integer reply: 成员的排名，如果成员不存在或键不存在则返回 nil。

## Examples

```
redis> ZADD myzset 1 "one"
(integer) 1
redis> ZADD myzset 2 "two"
(integer) 1
redis> ZADD myzset 3 "three"
(integer) 1
redis> ZRANK myzset "two"
(integer) 1
```
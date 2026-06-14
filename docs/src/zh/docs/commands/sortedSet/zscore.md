# ZSCORE

返回有序集合中指定成员的分数。

## Syntax

```
ZSCORE key member
```

## Return

Bulk string reply: 成员的分数，如果成员不存在或键不存在则返回 nil。

## Examples

```
redis> ZADD myzset 1 "one"
(integer) 1
redis> ZADD myzset 2 "two"
(integer) 1
redis> ZSCORE myzset "one"
"1"
redis> ZSCORE myzset "three"
(nil)
```
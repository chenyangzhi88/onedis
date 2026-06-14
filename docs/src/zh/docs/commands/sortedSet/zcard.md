# ZCARD

返回有序集合中元素的数量。

## Syntax

```
ZCARD key
```

## Return

Integer reply: 有序集合中元素的数量，如果键不存在则返回 0。

## Examples

```
redis> ZADD myzset 1 "one"
(integer) 1
redis> ZADD myzset 2 "two"
(integer) 1
redis> ZCARD myzset
(integer) 2
```
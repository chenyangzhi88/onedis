# LRANGE

返回列表中指定范围内的元素。开始和结束偏移量都是基于 0 的索引，负数偏移量表示从列表末尾开始计数。

## Syntax

```
LRANGE key start stop
```

## Return

Array reply: 指定范围内的元素列表。

## Examples

```
redis> RPUSH mylist "one"
(integer) 1
redis> RPUSH mylist "two"
(integer) 2
redis> RPUSH mylist "three"
(integer) 3
redis> LRANGE mylist 0 0
1) "one"
redis> LRANGE mylist -3 2
1) "one"
2) "two"
3) "three"
redis> LRANGE mylist -100 100
1) "one"
2) "two"
3) "three"
redis> LRANGE mylist 5 10
(empty list or set)
```
# RPOP

移除并返回列表的最后一个元素（右边）。

## Syntax

```
RPOP key
```

## Return

Bulk string reply: 列表的最后一个元素，如果列表为空或键不存在则返回 nil。

## Examples

```
redis> RPUSH mylist "one"
(integer) 1
redis> RPUSH mylist "two"
(integer) 2
redis> RPUSH mylist "three"
(integer) 3
redis> RPOP mylist
"three"
redis> LRANGE mylist 0 -1
1) "one"
2) "two"
```
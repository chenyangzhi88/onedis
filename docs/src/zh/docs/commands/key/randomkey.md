# RANDOMKEY

从当前数据库中随机返回一个键。如果数据库为空则返回 nil。

## Syntax

```
RANDOMKEY
```

## Return

Bulk string reply: 随机返回一个键，如果数据库为空则返回 nil。

## Examples

```
redis> MSET fruit "apple" vegetable "carrot" drink "water"
OK
redis> RANDOMKEY
"fruit"
redis> RANDOMKEY
"drink"
redis> FLUSHDB
OK
redis> RANDOMKEY
(nil)
```
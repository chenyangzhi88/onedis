# HGETALL

返回哈希表中所有的字段和值。

## Syntax

```
HGETALL key
```

## Return

Array reply: 以列表形式返回哈希表中的所有字段和值，格式为 [field1, value1, field2, value2, ...]。

## Examples

```
redis> HSET myhash field1 "Hello"
(integer) 1
redis> HSET myhash field2 "World"
(integer) 1
redis> HGETALL myhash
1) "field1"
2) "Hello"
3) "field2"
4) "World"
```
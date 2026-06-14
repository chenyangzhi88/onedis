# HMGET

返回哈希表中一个或多个指定字段的值。

## Syntax

```
HMGET key field [field ...]
```

## Return

Array reply: 以列表形式返回所有给定字段的值，不存在的字段返回 nil。

## Examples

```
redis> HSET myhash field1 "Hello"
(integer) 1
redis> HSET myhash field2 "World"
(integer) 1
redis> HMGET myhash field1 field2 nofield
1) "Hello"
2) "World"
3) (nil)
```
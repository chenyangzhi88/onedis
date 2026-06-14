# HVALS

返回哈希表中所有的值。

## Syntax

```
HVALS key
```

## Return

Array reply: 以列表形式返回哈希表中的所有值。

## Examples

```
redis> HSET myhash field1 "Hello"
(integer) 1
redis> HSET myhash field2 "World"
(integer) 1
redis> HVALS myhash
1) "Hello"
2) "World"
```
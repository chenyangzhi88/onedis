# SMEMBERS

返回集合中的所有元素。

## Syntax

```
SMEMBERS key
```

## Return

Array reply: 集合中的所有元素，如果键不存在则返回空数组。

## Examples

```
redis> SADD myset "Hello"
(integer) 1
redis> SADD myset "World"
(integer) 1
redis> SMEMBERS myset
1) "Hello"
2) "World"
```
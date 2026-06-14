# SCARD

返回集合中元素的数量。

## Syntax

```
SCARD key
```

## Return

Integer reply: 集合中元素的数量，如果键不存在则返回 0。

## Examples

```
redis> SADD myset "Hello"
(integer) 1
redis> SADD myset "World"
(integer) 1
redis> SCARD myset
(integer) 2
```
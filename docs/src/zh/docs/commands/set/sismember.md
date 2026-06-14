# SISMEMBER

判断指定元素是否存在于集合中。

## Syntax

```
SISMEMBER key member
```

## Return

Integer reply: 如果元素存在于集合中返回 1，如果元素不存在或键不存在返回 0。

## Examples

```
redis> SADD myset "Hello"
(integer) 1
redis> SISMEMBER myset "Hello"
(integer) 1
redis> SISMEMBER myset "World"
(integer) 0
```
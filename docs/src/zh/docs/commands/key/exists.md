# EXISTS

检查给定的一个或多个键是否存在。如果键存在则返回 1，否则返回 0。

## Syntax

```
EXISTS key [key ...]
```

## Return

Integer reply: 存在的键的数量。

## Examples

```
redis> SET key1 "Hello"
OK
redis> EXISTS key1
(integer) 1
redis> EXISTS nosuchkey
(integer) 0
redis> SET key2 "World"
OK
redis> EXISTS key1 key2 nosuchkey
(integer) 2
```
# DEL

删除指定的键。如果键不存在，则忽略该键。

## Syntax

```
DEL key [key ...]
```

## Return

Integer reply: 被删除的键的数量。

## Examples

```
redis> SET key1 "Hello"
OK
redis> SET key2 "World"
OK
redis> DEL key1 key2 key3
(integer) 2
```
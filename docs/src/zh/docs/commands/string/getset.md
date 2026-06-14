# GETSET

设置键的字符串值，并返回键的旧值。如果键不存在，则返回 nil。

## Syntax

```
GETSET key value
```

## Return

Bulk string reply: 键的旧值，如果键不存在则返回 nil。

## Examples

```
redis> GETSET mykey "Hello"
(nil)
redis> GETSET mykey "World"
"Hello"
redis> GET mykey
"World"
```
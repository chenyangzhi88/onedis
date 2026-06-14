# DECR

将键存储的数字值减一。如果键不存在，则在执行操作前将其设置为 0。如果键的值不是整数或超出范围，则返回错误。

## Syntax

```
DECR key
```

## Return

Integer reply: 减一操作之后键的值。

## Examples

```
redis> SET mykey "10"
OK
redis> DECR mykey
(integer) 9
redis> SET mykey "234293482390480948029348230948"
OK
redis> DECR mykey
ERR value is not an integer or out of range
```
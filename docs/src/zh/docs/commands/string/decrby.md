# DECRBY

将键存储的数字值减去指定的整数。如果键不存在，则在执行操作前将其设置为 0。如果键的值不是整数或超出范围，则返回错误。

## Syntax

```
DECRBY key decrement
```

## Return

Integer reply: 减去指定整数之后键的值。

## Examples

```
redis> SET mykey "10"
OK
redis> DECRBY mykey 3
(integer) 7
redis> SET mykey "234293482390480948029348230948"
OK
redis> DECRBY mykey 3
ERR value is not an integer or out of range
```
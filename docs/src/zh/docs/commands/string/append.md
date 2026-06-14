# APPEND

如果键已经存在并且是一个字符串，该命令将值追加到键的字符串值末尾。如果键不存在，则会先创建一个空字符串，然后再执行追加操作。

## Syntax

```
APPEND key value
```

## Return

Integer reply: 追加操作完成后键值的长度。

## Examples

```
redis> EXISTS mykey
(integer) 0
redis> APPEND mykey "Hello"
(integer) 5
redis> APPEND mykey " World"
(integer) 11
redis> GET mykey
"Hello World"
```
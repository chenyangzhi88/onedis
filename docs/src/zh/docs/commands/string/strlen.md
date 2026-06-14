# STRLEN

返回键存储的字符串值的长度。如果键不存在，则返回 0。

## Syntax

```
STRLEN key
```

## Return

Integer reply: 字符串值的长度，如果键不存在则返回 0。

## Examples

```
redis> SET mykey "Hello world"
OK
redis> STRLEN mykey
(integer) 11
redis> STRLEN nonexisting
(integer) 0
```
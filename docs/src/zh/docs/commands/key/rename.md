# RENAME

将键重命名为新键。如果键不存在则返回错误。如果新键已经存在，则会被覆盖。当这种情况发生时，RENAME 会执行一个隐式的 DEL 操作，因此如果被删除的键包含很大的值，即使 RENAME 本身通常是常量时间操作，也可能导致高延迟。

## Syntax

```
RENAME key newkey
```

## Return

Simple string reply: 如果键被成功重命名则返回 OK，否则返回错误。

## Examples

```
redis> SET mykey "Hello"
OK
redis> RENAME mykey myotherkey
OK
redis> GET myotherkey
"Hello"
```
# PTTL

类似于 TTL 命令，但以毫秒为单位返回键的剩余生存时间。如果键不存在则返回 -2，如果键存在但没有设置过期时间则返回 -1。

## Syntax

```
PTTL key
```

## Return

Integer reply: 键的剩余生存时间（以毫秒为单位），或者一个负值表示错误。

- -2 表示键不存在
- -1 表示键存在但没有关联的过期时间

## Examples

```
redis> SET mykey "Hello"
OK
redis> PEXPIRE mykey 1000
(integer) 1
redis> PTTL mykey
(integer) 1000
```
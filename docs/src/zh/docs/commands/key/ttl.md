# TTL

返回键的剩余生存时间（以秒为单位）。与 PTTL 命令类似，唯一的区别是 TTL 返回剩余时间的秒数，而 PTTL 返回毫秒数。

## Syntax

```
TTL key
```

## Return

Integer reply: 以秒为单位的 TTL，或负值表示错误。

- 如果键不存在，命令返回 -2
- 如果键存在但没有关联的过期时间，命令返回 -1

## Examples

```
redis> SET mykey "Hello"
OK
redis> EXPIRE mykey 10
(integer) 1
redis> TTL mykey
(integer) 10
```
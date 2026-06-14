# MOVE

将指定的键从当前数据库移动到指定编号的数据库。如果键已经存在于目标数据库，则不会执行任何操作。

## Syntax

```
MOVE key db
```

## Return

Integer reply: 1 如果键被成功移动，0 如果键不存在或者目标数据库中已存在同名键。

## Examples

```
redis> SELECT 0
OK
redis> SET mykey "Hello"
OK
redis> MOVE mykey 1
(integer) 1
redis> EXISTS mykey
(integer) 0
redis> SELECT 1
OK
redis> EXISTS mykey
(integer) 1
```
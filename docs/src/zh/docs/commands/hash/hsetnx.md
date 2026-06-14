# HSETNX

仅当字段不存在时，为哈希表中的字段赋值。如果字段已经存在，操作将被忽略。

## Syntax

```
HSETNX key field value
```

## Return

Integer reply: 如果字段是新创建的并成功设置值则返回 1，如果字段已经存在则返回 0。

## Examples

```
redis> HSETNX myhash field "Hello"
(integer) 1
redis> HSETNX myhash field "World"
(integer) 0
redis> HGET myhash field
"Hello"
```
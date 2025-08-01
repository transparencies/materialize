---
title: "DROP SOURCE"
description: "`DROP SOURCE` removes a source from Materialize."
menu:
  main:
    parent: commands
---

`DROP SOURCE` removes a source from Materialize. If there are objects depending
on the source, you must explicitly drop them first, or use the `CASCADE`
option.

## Syntax

{{< diagram "drop-source.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the named source does not exist.
_source&lowbar;name_ | The name of the source you want to remove.
**CASCADE** | Remove the source and its dependent views.
**RESTRICT** | Do not remove this source if any views depend on it. _(Default.)_

## Examples

### Remove a source with no dependent objects

```mzsql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```mzsql
DROP SOURCE my_source;
```

### Remove a source with dependent objects

```mzsql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```mzsql
DROP SOURCE my_source CASCADE;
```

### Remove a source only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP SOURCE my_source;
  ```
- ```mzsql
  DROP SOURCE my_source RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent source

```mzsql
DROP SOURCE IF EXISTS my_source;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-source.md" >}}

## Related pages

- [`CREATE SOURCE`](../create-source)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW CREATE SOURCE`](../show-create-source)
- [`DROP OWNED`](../drop-owned)

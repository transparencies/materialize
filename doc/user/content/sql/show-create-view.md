---
title: "SHOW CREATE VIEW"
description: "`SHOW CREATE VIEW` returns the `SELECT` statement used to create the view."
menu:
  main:
    parent: commands
---

`SHOW CREATE VIEW` returns the [`SELECT`](../select) statement used to create the view.

## Syntax

```sql
SHOW [REDACTED] CREATE VIEW <view_name>
```

{{< yaml-table data="show_create_redacted_option" >}}

For available view names, see [`SHOW VIEWS`](/sql/show-views).

## Examples

```mzsql
SHOW CREATE VIEW my_view;
```
```nofmt
            name            |                                            create_sql
----------------------------+--------------------------------------------------------------------------------------------------
 materialize.public.my_view | CREATE VIEW "materialize"."public"."my_view" AS SELECT * FROM "materialize"."public"."my_source"
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/show-create-view.md"
>}}

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`CREATE VIEW`](../create-view)

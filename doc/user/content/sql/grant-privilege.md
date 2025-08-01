---
title: "GRANT PRIVILEGE"
description: "`GRANT` grants privileges on a database object."
menu:
  main:
    parent: commands
---

`GRANT` grants privileges on a database object. The `PUBLIC` pseudo-role can
be used to indicate that the privileges should be granted to all roles
(including roles that might not exist yet).

Privileges are cumulative: revoking a privilege from `PUBLIC` does not mean all
roles have lost that privilege, if certain roles were explicitly granted that
privilege.

## Syntax

{{< diagram "grant-privilege.svg" >}}

### `privilege`

{{< diagram "privilege.svg" >}}

Field                                               | Use
----------------------------------------------------|--------------------------------------------------
_object_name_                                       | The object that privileges are being granted on.
**ALL** _object_type_ **IN SCHEMA** schema_name     | The privilege will be granted on all objects of _object_type_ in _schema_name_.
**ALL** _object_type_ **IN DATABASE** database_name | The privilege will be granted on all objects of _object_type_ in _database_name_.
**ALL** _object_type_                               | The privilege will be granted on all objects of _object_type_, excluding system objects.
_role_name_                                         | The role name that is gaining privileges. Use the `PUBLIC` pseudo-role to grant privileges to all roles.
**SELECT**                                          | Allows reading rows from an object. The abbreviation for this privilege is 'r' (read).
**INSERT**                                          | Allows inserting into an object. The abbreviation for this privilege is 'a' (append).
**UPDATE**                                          | Allows updating an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'w' (write).
**DELETE**                                          | Allows deleting from an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'd'.
**CREATE**                                          | Allows creating a new object within another object. The abbreviation for this privilege is 'C'.
**USAGE**                                           | Allows using an object or looking up members of an object. The abbreviation for this privilege is 'U'.
**CREATEROLE**                                      | Allows creating, altering, deleting roles and the ability to grant and revoke role membership. This privilege is very powerful. It allows roles to grant and revoke membership in other roles, even if it doesn't have explicit membership in those roles. As a consequence, any role with this privilege can obtain the privileges of any other role in the system. The abbreviation for this privilege is 'R' (Role).
**CREATEDB**                                        | Allows creating databases. The abbreviation for this privilege is 'B' (dataBase).
**CREATECLUSTER**                                   | Allows creating clusters. The abbreviation for this privilege is 'N' (compute Node).
**CREATENETWORKPOLICY**                             | Allows creating network policies. The abbreviation for this privilege is 'P' (newtwork Policy).
**ALL PRIVILEGES**                                  | All applicable privileges for the provided object type.
**GROUP**                                           | This is an optional keyword that has no effect but is part of the SQL standard.

## Details

The following table describes which privileges are applicable to which objects:

| Object type           | All privileges |
|-----------------------|----------------|
| `SYSTEM`              | RBN            |
| `DATABASE`            | UC             |
| `SCHEMA`              | UC             |
| `TABLE`               | arwd           |
| (`VIEW`)              | r              |
| (`MATERIALIZED VIEW`) | r              |
| `INDEX`               |                |
| `TYPE`                | U              |
| (`SOURCE`)            | r              |
| `SINK`                |                |
| `CONNECTION`          | U              |
| `SECRET`              | U              |
| `CLUSTER`             | UC             |

Unlike PostgreSQL, `UPDATE` and `DELETE` always require `SELECT` privileges on the object being
updated.

### Compatibility

For PostgreSQL compatibility reasons, you must specify `TABLE` as the object
type for sources, views, and materialized views, or omit the object type.

## Examples

```mzsql
GRANT SELECT ON mv TO joe, mike;
```

```mzsql
GRANT USAGE, CREATE ON DATABASE materialize TO joe;
```

```mzsql
GRANT ALL ON CLUSTER dev TO joe;
```

```mzsql
GRANT CREATEDB ON SYSTEM TO joe;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/grant-privilege.md"
>}}

## Useful views

- [`mz_internal.mz_show_system_privileges`](/sql/system-catalog/mz_internal/#mz_show_system_privileges)
- [`mz_internal.mz_show_my_system_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_system_privileges)
- [`mz_internal.mz_show_cluster_privileges`](/sql/system-catalog/mz_internal/#mz_show_cluster_privileges)
- [`mz_internal.mz_show_my_cluster_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_cluster_privileges)
- [`mz_internal.mz_show_database_privileges`](/sql/system-catalog/mz_internal/#mz_show_database_privileges)
- [`mz_internal.mz_show_my_database_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_database_privileges)
- [`mz_internal.mz_show_schema_privileges`](/sql/system-catalog/mz_internal/#mz_show_schema_privileges)
- [`mz_internal.mz_show_my_schema_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_schema_privileges)
- [`mz_internal.mz_show_object_privileges`](/sql/system-catalog/mz_internal/#mz_show_object_privileges)
- [`mz_internal.mz_show_my_object_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_object_privileges)
- [`mz_internal.mz_show_all_privileges`](/sql/system-catalog/mz_internal/#mz_show_all_privileges)
- [`mz_internal.mz_show_all_my_privileges`](/sql/system-catalog/mz_internal/#mz_show_all_my_privileges)

## Related pages

- [`SHOW PRIVILEGES`](../show-privileges)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](../alter-owner)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)

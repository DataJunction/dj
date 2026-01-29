---
weight: 2
title: "Column Types"
---

DJ's type system is based on **Apache Spark SQL types**. This ensures compatibility with Spark-based query engines and provides a familiar type system for users working with big data platforms.

## Specifying Types

How you specify types depends on the node type:

**Source nodes**: You manually specify column types as strings when defining the source, since DJ cannot infer types from an external table without connecting to it.

```yaml
# Example: Specifying column types for a source node
columns:
  - name: user_id
    type: bigint
  - name: username
    type: string
  - name: metadata
    type: map<string, string>
  - name: tags
    type: array<string>
```

**Transform, dimension, and metric nodes**: DJ automatically parses your SQL query and infers column types based on the expressions and upstream node types. You don't need to specify types manually.

```yaml
# Example: Transform node - types are inferred from the query
query: |
  SELECT
    user_id,                                -- inferred as bigint from source
    UPPER(username) AS username,            -- inferred as string
    CAST(created_at AS date) AS order_date  -- inferred as date from CAST
  FROM source.events
```

Using `CAST` is a common way to explicitly control column types in your transforms.

## Primitive Types

### Numeric Types

| Type | Aliases | Description | Range |
|------|---------|-------------|-------|
| `tinyint` | | 8-bit signed integer | -128 to 127 |
| `smallint` | | 16-bit signed integer | -32,768 to 32,767 |
| `int` | `integer` | 32-bit signed integer | -2,147,483,648 to 2,147,483,647 |
| `bigint` | `long` | 64-bit signed integer | -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 |
| `float` | | 32-bit IEEE 754 floating point | |
| `double` | | 64-bit IEEE 754 floating point | |
| `decimal(p, s)` | | Fixed-precision decimal | Precision up to 38, scale up to 38 |

**Example:**
```sql
SELECT
    CAST(quantity AS int),
    CAST(price AS decimal(10, 2)),
    CAST(large_value AS bigint)
FROM orders
```

### Boolean Type

| Type | Aliases | Description |
|------|---------|-------------|
| `boolean` | `bool` | True or false values |

### String Types

| Type | Aliases | Description |
|------|---------|-------------|
| `string` | `str` | Arbitrary-length UTF-8 character sequence |
| `varchar` | `varchar(n)` | Variable-length character string, optionally with max length |

**Example:**
```sql
SELECT
    CAST(name AS string),
    CAST(code AS varchar(10))
FROM products
```

### Date and Time Types

| Type | Aliases | Description |
|------|---------|-------------|
| `date` | | Calendar date (year, month, day) without time or timezone |
| `time` | | Time of day without date or timezone (microsecond precision) |
| `timestamp` | `datetime` | Date and time without timezone (microsecond precision) |
| `timestamptz` | | Date and time with timezone, stored as UTC |

**Example:**
```sql
SELECT
    CAST(order_date AS date),
    CAST(created_at AS timestamp),
    CAST(event_time AS timestamptz)
FROM events
```

### Binary Types

| Type | Aliases | Description |
|------|---------|-------------|
| `binary` | `byte`, `bytes` | Arbitrary-length byte array |

### Other Types

| Type | Description |
|------|-------------|
| `uuid` | Universally unique identifier |
| `null` | Represents missing or unknown values |

## Complex Types

DJ supports Spark's complex types for nested and structured data.

### Struct

A struct is a collection of named fields, each with its own type. Use `struct<field_name type, ...>` syntax.

```sql
-- Struct type definition
struct<name string, age int, active boolean>

-- Accessing struct fields
SELECT user_info.name, user_info.age FROM users
```

### Array

An array is an ordered collection of elements of the same type. Use `array<element_type>` syntax.

```sql
-- Array type definition
array<string>
array<int>
array<struct<id int, value double>>

-- Working with arrays
SELECT tags[0], SIZE(tags) FROM items
```

### Map

A map is a collection of key-value pairs. Use `map<key_type, value_type>` syntax.

```sql
-- Map type definition
map<string, int>
map<string, array<double>>

-- Accessing map values
SELECT properties['color'], properties['size'] FROM products
```

## Interval Types

Interval types represent time durations and are useful for date/time arithmetic.

| Type | Description |
|------|-------------|
| `INTERVAL DAY TO SECOND` | Duration in days, hours, minutes, seconds |
| `INTERVAL YEAR TO MONTH` | Duration in years and months |

**Example:**
```sql
SELECT
    order_date + INTERVAL 30 DAY,
    subscription_start + INTERVAL 1 YEAR
FROM subscriptions
```

## Type Compatibility

DJ performs type checking and will report errors when incompatible types are used. Types within the same category are generally compatible:

- **Numeric types**: Can be compared and used in arithmetic operations together. Smaller types are promoted to larger types (e.g., `int` to `bigint`).
- **String types**: `string` and `varchar` are compatible.
- **Date/time types**: Can be compared within the same category.

## Type Casting

Use `CAST` to convert between types:

```sql
SELECT
    CAST(string_column AS int),
    CAST(int_column AS double),
    CAST(timestamp_column AS date),
    CAST(number AS string)
FROM my_table
```

{{< alert icon="👉" >}}
For more details on Spark SQL types, see the [Apache Spark SQL Data Types documentation](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).
{{< /alert >}}

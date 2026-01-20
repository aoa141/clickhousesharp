# ClickHouseSharp

A pure .NET implementation of ClickHouse SQL engine for local, in-memory query execution. This library provides `clickhouse-local` equivalent functionality without requiring a ClickHouse server installation.

## Features

- **Pure .NET 10** - No native dependencies, runs on Windows, Linux, and macOS
- **ClickHouse SQL Dialect** - Supports ClickHouse-specific syntax and functions
- **In-Memory Execution** - Fast local query processing
- **Comprehensive SQL Support**:
  - SELECT with WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT
  - JOINs (INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI)
  - GROUP BY with HAVING
  - UNION, INTERSECT, EXCEPT
  - Common Table Expressions (CTEs)
  - Subqueries in FROM and WHERE clauses
  - CASE expressions
  - IN, BETWEEN, LIKE operators

## Installation

```bash
dotnet add package ClickHouseSharp
```

## Quick Start

```csharp
using ClickHouseSharp;

using var ch = new ClickHouseLocal();

// Create a table
ch.Execute("CREATE TABLE users (id Int64, name String, age Int64)");

// Insert data
ch.Execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");

// Query data
var result = ch.Execute("SELECT name, age FROM users WHERE age > 25 ORDER BY age");

foreach (var row in result.Rows)
{
    Console.WriteLine($"{row[0].AsString()}: {row[1].AsInt64()}");
}
// Output:
// Alice: 30
// Charlie: 35
```

## Supported Data Types

| Type | Description |
|------|-------------|
| `Int8`, `Int16`, `Int32`, `Int64` | Signed integers |
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | Unsigned integers |
| `Float32`, `Float64` | Floating point numbers |
| `Decimal(P, S)` | Fixed-point decimal |
| `String` | UTF-8 strings |
| `Bool` | Boolean values |
| `Date`, `DateTime` | Date and time types |
| `UUID` | Universally unique identifiers |
| `Nullable(T)` | Nullable wrapper for any type |
| `Array(T)` | Arrays of any type |
| `Tuple(T1, T2, ...)` | Tuples |
| `Map(K, V)` | Key-value maps |
| `LowCardinality(T)` | Dictionary-encoded values |

## Supported Functions

### Aggregate Functions
- `count()`, `sum()`, `avg()`, `min()`, `max()`
- `groupArray()`, `uniq()`
- `argMin()`, `argMax()`
- `sumIf()`, `countIf()`, `avgIf()`

### Math Functions
- `abs()`, `ceil()`, `floor()`, `round()`
- `sqrt()`, `pow()`, `exp()`, `log()`, `log10()`, `log2()`
- `sin()`, `cos()`, `tan()`, `asin()`, `acos()`, `atan()`
- `greatest()`, `least()`

### String Functions
- `length()`, `lower()`, `upper()`, `trim()`, `ltrim()`, `rtrim()`
- `concat()`, `substring()`, `replace()`
- `position()`, `startsWith()`, `endsWith()`
- `splitByChar()`, `splitByString()`, `arrayStringConcat()`
- `reverse()`, `replicate()`

### Date/Time Functions
- `toYear()`, `toMonth()`, `toDayOfMonth()`, `toDayOfWeek()`
- `toHour()`, `toMinute()`, `toSecond()`
- `toDate()`, `toDateTime()`
- `now()`, `today()`, `yesterday()`

### Array Functions
- `array()`, `arrayLength()`, `empty()`
- `has()`, `indexOf()`, `arrayElement()`
- `arrayConcat()`, `arrayPushBack()`, `arrayPushFront()`
- `arrayReverse()`, `arraySlice()`
- `range()`

### Conditional Functions
- `if()`, `multiIf()`
- `ifNull()`, `nullIf()`, `coalesce()`

### Type Conversion
- `toInt8()`, `toInt16()`, `toInt32()`, `toInt64()`
- `toUInt8()`, `toUInt16()`, `toUInt32()`, `toUInt64()`
- `toFloat32()`, `toFloat64()`
- `toString()`, `toDate()`, `toDateTime()`
- `CAST(expr AS Type)`

### Table Functions
- `numbers(N)` - Generate sequence 0 to N-1
- `zeros(N)` - Generate N zero values

## Examples

### Basic Queries

```csharp
// Literal values and arithmetic
var result = ch.Execute("SELECT 1 + 2, 10 * 5, 'hello' || ' world'");

// Using table functions
var result = ch.Execute("SELECT number, number * 2 AS doubled FROM numbers(10)");

// Filtering with WHERE
var result = ch.Execute("SELECT number FROM numbers(100) WHERE number % 2 = 0 LIMIT 10");
```

### Aggregations and GROUP BY

```csharp
ch.Execute("CREATE TABLE sales (category String, amount Int64)");
ch.Execute("INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150), ('B', 300)");

var result = ch.Execute(@"
    SELECT category, sum(amount) AS total, count() AS cnt
    FROM sales
    GROUP BY category
    ORDER BY total DESC
");
// Returns: B -> 500, A -> 250
```

### JOINs

```csharp
ch.Execute("CREATE TABLE users (id Int64, name String)");
ch.Execute("CREATE TABLE orders (id Int64, user_id Int64, product String)");
ch.Execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
ch.Execute("INSERT INTO orders VALUES (1, 1, 'Laptop'), (2, 1, 'Phone'), (3, 2, 'Tablet')");

var result = ch.Execute(@"
    SELECT u.name, o.product
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    ORDER BY u.name, o.product
");
```

### Common Table Expressions (CTEs)

```csharp
var result = ch.Execute(@"
    WITH doubled AS (
        SELECT number * 2 AS value FROM numbers(5)
    )
    SELECT value FROM doubled WHERE value > 4 ORDER BY value
");
// Returns: 6, 8
```

### UNION and Set Operations

```csharp
var result = ch.Execute(@"
    SELECT number FROM numbers(5)
    UNION ALL
    SELECT number + 10 FROM numbers(5)
    ORDER BY number
");
// Returns: 0, 1, 2, 3, 4, 10, 11, 12, 13, 14
```

### Subqueries

```csharp
ch.Execute("CREATE TABLE products (id Int64, category_id Int64, name String)");
ch.Execute("CREATE TABLE categories (id Int64, name String)");
ch.Execute("INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Books')");
ch.Execute("INSERT INTO products VALUES (1, 1, 'Laptop'), (2, 1, 'Phone'), (3, 2, 'Novel')");

var result = ch.Execute(@"
    SELECT name FROM products
    WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')
    ORDER BY name
");
// Returns: Laptop, Phone
```

### Working with Arrays

```csharp
var result = ch.Execute(@"
    SELECT
        array(1, 2, 3) AS arr,
        arrayLength(array(1, 2, 3, 4, 5)) AS len,
        has(array(1, 2, 3), 2) AS contains_two,
        range(5) AS sequence
");
```

### Conditional Logic

```csharp
var result = ch.Execute(@"
    SELECT
        number,
        CASE
            WHEN number < 3 THEN 'small'
            WHEN number < 7 THEN 'medium'
            ELSE 'large'
        END AS size
    FROM numbers(10)
");
```

## Running Tests

```bash
cd tests/ClickHouseSharp.Tests
dotnet test
```

The test suite includes 78 tests covering:
- Basic SELECT operations
- Aggregations and GROUP BY
- All JOIN types
- UNION, INTERSECT, EXCEPT
- CTEs
- Subqueries
- Built-in functions
- DDL/DML operations

## Limitations

- No persistent storage (in-memory only)
- No window functions (planned)
- No external file I/O yet (CSV/Parquet planned)
- Subset of ClickHouse functions implemented

## License

MIT License

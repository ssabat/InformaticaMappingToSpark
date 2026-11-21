# Informatica Mapping to PySpark

A Python-based reference framework for migrating selected Informatica PowerCenter mappings to PySpark and Delta Lake. The repository includes XML and parameter-file parsing, expression translation, DAG analysis, and an SCD Type 2 merge example.

## Overview

Informatica exports contain transformation metadata that must be translated into equivalent Spark operations. This project demonstrates how to parse that metadata and generate or execute PySpark transformations without relying on a proprietary Informatica runtime.

The repository contains two standalone Python modules:

| Module | Purpose |
|---|---|
| `SCD2MappingtoPySpark.py` | Parses a multi-mapping XML export, executes filter, lookup, expression, and SCD Type 2 transformations, and performs Delta Lake merges. |
| `infa_compiler.py` | Demonstrates parameter parsing, Informatica-to-Spark expression translation, mapping DAG topological sorting, and pytest/chispa test-suite generation. |

## Features

- Parses Informatica parameter files (`.par`) and XML export metadata.
- Resolves `$$` parameter variables in expressions.
- Converts common Informatica functions to Spark SQL:
  - `IIF` → `IF`
  - `ISNULL` → `IS NULL`
  - `NVL` → `COALESCE`
  - `DECODE` → `CASE ... WHEN ... THEN ... ELSE ... END`
- Applies Informatica filter, lookup, expression, and SCD Type 2 operations in PySpark.
- Supports multi-mapping XML processing and composite business keys.
- Sorts port-level mapping connectors using Kahn's topological sorting algorithm.
- Generates an isolated pytest/chispa test scaffold for each mapping.
- Demonstrates Router and Union transformation handling in the compiler module.

## Repository Structure

```text
.
├── infa_compiler.py              # Mapping compiler and test-generation utilities
├── SCD2MappingtoPySpark.py       # Embedded XML parser and SCD2 execution example
└── README.md
```

## Prerequisites

- Python 3.9 or later
- Apache Spark with PySpark
- Delta Lake
- Java 8 or 11 for local Spark execution
- Optional: `pytest` and `chispa` for generated test suites

Install the Python dependencies:

```bash
pip install pyspark delta-spark pytest chispa
```

## Running the SCD2 Example

The SCD2 module contains a sample multi-mapping Informatica XML document and executes it directly:

```bash
python SCD2MappingtoPySpark.py
```

The sample processes customer and product mappings. It reads source and lookup tables from the active Spark session, applies filters and expressions, and merges results into Delta tables named in the source metadata.

Before running it against real data, verify that the following tables exist:

- Customer and product source tables
- Country and other lookup tables
- Customer and product Delta target tables

The target tables must support the fields used by the merge:

- `row_hash`
- `is_current`
- `effective_start_date`
- `effective_end_date`

## Running the Mapping Compiler

The compiler module contains embedded sample `.par` and XML content. Run it to parse the sample parameter file, load the mapping, and print the topologically sorted DAG:

```bash
python infa_compiler.py
```

The `generate_unit_test_file()` function can also generate a pytest/chispa test scaffold for a mapping:

```python
from infa_compiler import generate_unit_test_file

generate_unit_test_file(
    mapping_name="m_LOAD_DIM_CUSTOMER_360_SCD2",
    target_table="dim.dim_customer_360",
    business_keys=["customer_id"],
    tracked_cols=["full_name", "region_name", "total_spend"],
    test_dir="generated_tests",
)
```

## Supported Transformations

| Informatica transformation | PySpark implementation |
|---|---|
| Source | `SparkSession.read.table(...)` |
| Filter | `DataFrame.filter(...)` |
| Lookup | Broadcast left join |
| Expression | `DataFrame.withColumn(...)` with `pyspark.sql.functions.expr(...)` |
| Router | DAG and router-group routing demonstrated in the compiler |
| Union | DAG and union transformation demonstrated in the compiler |
| SCD Type 2 | Delta Lake merge with effective-dating columns |

## Important Limitations

This repository is a migration reference implementation, not a complete Informatica PowerCenter compiler.

- Scripts currently use embedded sample XML and parameter data rather than command-line file inputs.
- Expression conversion relies on regular-expression and parsing rules and should be expanded for complex nested expressions.
- Lookup parsing supports a simple `source_column = lookup_column` condition.
- Port-level lineage and transformation coverage do not represent every PowerCenter transformation.
- Generated tests are initial scaffolds and should be updated with real source, target, and expected-result fixtures.
- Real exports and mappings must be validated for expression semantics, transformation ordering, business keys, composite keys, null handling, and target schema compatibility.

## Migration Considerations

Before promoting a generated pipeline to production:

1. Review the source, target, lookup, and transformation metadata.
2. Compare null, string, numeric, and date semantics with PowerCenter behavior.
3. Define explicit SCD2 effective-dating and late-arriving-row rules.
4. Add data-quality checks, reconciliation totals, and audit columns.
5. Test incremental and full-load behavior against representative production data.
6. Review joins, duplicate handling, and merge conditions for each business key.

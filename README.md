* Module 1 - Enterprise Informatica-to-PySpark Migration Compiler
  
Below is the complete, documented Python framework (infa_compiler.py). When executed, it parses an Informatica .par file and XML export, resolves $$ variables, builds the DAG with Router branching and Union merging, and emits both production PySpark pipelines and isolated pytest + chispa test suites.
python
"""
Enterprise Informatica PowerCenter XML to PySpark + Delta Lake Compiler.

This module provides an end-to-end migration engine capable of:
1. Parsing Informatica Parameter Files (.par) into runtime configurations.
2. Resolving port-level DAG lineage via <CONNECTOR> tags using Kahn's topological sort.
3. Transpiling Informatica SQL/Expression functions (IIF, DECODE, ISNULL, NVL) to Spark SQL.
4. Handling multi-branch transformations: Router (group routing + DEFAULT), Union,
   Joiner, Lookup, Aggregator, and Filter.
5. Generating standalone, documented PySpark SCD Type 2 pipeline scripts.
6. Generating automated `pytest` + `chispa` unit test suites for each pipeline.
"""

* Module 2 - SCD2toPySpark converter

This Python script converts selected Informatica PowerCenter XML mappings into executable PySpark pipelines. It parses source and target metadata, filter conditions, lookups, and expression transformations, then translates common Informatica functions such as `IIF`, `ISNULL`, `NVL`, and `DECODE` into Spark SQL expressions.

For each mapping, the script reads the source table, applies filters and lookup joins, creates derived columns, and calculates a row hash for change detection. It uses Delta Lake merge operations to maintain SCD Type 2 target tables, closing outdated records and inserting changed or new records with effective-dating columns.

The embedded sample processes customer and product mappings. It currently demonstrates XML parsing and runtime execution but should still be tested against real PowerCenter exports, especially for complex expressions, transformation ordering, and composite-key behavior.

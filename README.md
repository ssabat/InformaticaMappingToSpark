Enterprise Informatica-to-PySpark Migration Compiler
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

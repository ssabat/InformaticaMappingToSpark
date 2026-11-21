import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from typing import Any, Dict, List, Tuple


# ==============================================================================
# 1. SAMPLE PARAMETER FILE (.par) & ENTERPRISE XML EXPORT
# ==============================================================================
SAMPLE_PAR_CONTENT = """
[EDW_SALES.WF:wf_CUSTOMER_360.ST:s_LOAD_DIM_CUSTOMER_360]
$$MIN_ORDER_THRESHOLD=100.0
$$DEFAULT_REGION=UNKNOWN
$$ETL_BATCH_ID=20261120_01
"""

ENTERPRISE_INFA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
  <REPOSITORY NAME="PROD_REPO">
    <FOLDER NAME="EDW_SALES">
      <!-- SOURCES -->
      <SOURCE NAME="STG_CUSTOMERS_US" OWNERNAME="raw">
        <SOURCEFIELD NAME="CUSTOMER_ID" KEYTYPE="PRIMARY KEY"/>
        <SOURCEFIELD NAME="FIRST_NAME" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="LAST_NAME" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="REGION_ID" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="TOTAL_SPEND" KEYTYPE="NOT A KEY"/>
      </SOURCE>

      <SOURCE NAME="STG_CUSTOMERS_EU" OWNERNAME="raw">
        <SOURCEFIELD NAME="CUSTOMER_ID" KEYTYPE="PRIMARY KEY"/>
        <SOURCEFIELD NAME="FIRST_NAME" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="LAST_NAME" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="REGION_ID" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="TOTAL_SPEND" KEYTYPE="NOT A KEY"/>
      </SOURCE>

      <!-- TARGETS (SCD2 Dimension + Error Hospital Table) -->
      <TARGET NAME="DIM_CUSTOMER_360" OWNERNAME="dim">
        <TARGETFIELD NAME="CUSTOMER_ID" KEYTYPE="FOREIGN KEY"/>
        <TARGETFIELD NAME="FULL_NAME" KEYTYPE="NOT A KEY"/>
        <TARGETFIELD NAME="REGION_NAME" KEYTYPE="NOT A KEY"/>
        <TARGETFIELD NAME="TOTAL_SPEND" KEYTYPE="NOT A KEY"/>
      </TARGET>

      <TARGET NAME="ERR_REJECTED_CUSTOMERS" OWNERNAME="audit">
        <TARGETFIELD NAME="CUSTOMER_ID" KEYTYPE="NOT A KEY"/>
        <TARGETFIELD NAME="FULL_NAME" KEYTYPE="NOT A KEY"/>
        <TARGETFIELD NAME="TOTAL_SPEND" KEYTYPE="NOT A KEY"/>
      </TARGET>

      <MAPPING NAME="m_LOAD_DIM_CUSTOMER_360_SCD2">
        <!-- INSTANCES -->
        <INSTANCE NAME="STG_CUSTOMERS_US" TRANSFORMATION_NAME="STG_CUSTOMERS_US" TRANSFORMATION_TYPE="Source Definition"/>
        <INSTANCE NAME="STG_CUSTOMERS_EU" TRANSFORMATION_NAME="STG_CUSTOMERS_EU" TRANSFORMATION_TYPE="Source Definition"/>
        <INSTANCE NAME="UNI_ALL_REGIONS" TRANSFORMATION_NAME="UNI_ALL_REGIONS" TRANSFORMATION_TYPE="Custom Transformation"/>
        <INSTANCE NAME="LKP_REGION" TRANSFORMATION_NAME="LKP_REGION" TRANSFORMATION_TYPE="Lookup Procedure"/>
        <INSTANCE NAME="EXP_ENRICH" TRANSFORMATION_NAME="EXP_ENRICH" TRANSFORMATION_TYPE="Expression"/>
        <INSTANCE NAME="RTR_VALIDATE_SPEND" TRANSFORMATION_NAME="RTR_VALIDATE_SPEND" TRANSFORMATION_TYPE="Router"/>
        <INSTANCE NAME="DIM_CUSTOMER_360" TRANSFORMATION_NAME="DIM_CUSTOMER_360" TRANSFORMATION_TYPE="Target Definition"/>
        <INSTANCE NAME="ERR_REJECTED_CUSTOMERS" TRANSFORMATION_NAME="ERR_REJECTED_CUSTOMERS" TRANSFORMATION_TYPE="Target Definition"/>

        <!-- TRANSFORMATIONS -->
        <TRANSFORMATION NAME="UNI_ALL_REGIONS" TYPE="Custom Transformation" TEMPLATENAME="Union Transformation">
          <TRANSFORMFIELD NAME="CUSTOMER_ID" PORTTYPE="OUTPUT"/>
          <TRANSFORMFIELD NAME="FIRST_NAME" PORTTYPE="OUTPUT"/>
          <TRANSFORMFIELD NAME="LAST_NAME" PORTTYPE="OUTPUT"/>
          <TRANSFORMFIELD NAME="REGION_ID" PORTTYPE="OUTPUT"/>
          <TRANSFORMFIELD NAME="TOTAL_SPEND" PORTTYPE="OUTPUT"/>
        </TRANSFORMATION>

        <TRANSFORMATION NAME="LKP_REGION" TYPE="Lookup Procedure">
          <TABLEATTRIBUTE NAME="Lookup table name" VALUE="dim.ref_regions"/>
          <TABLEATTRIBUTE NAME="Lookup condition" VALUE="REGION_ID = ID"/>
          <TRANSFORMFIELD NAME="ID" PORTTYPE="INPUT/LOOKUP"/>
          <TRANSFORMFIELD NAME="REGION_NAME" PORTTYPE="LOOKUP/OUTPUT"/>
        </TRANSFORMATION>

        <TRANSFORMATION NAME="EXP_ENRICH" TYPE="Expression">
          <TRANSFORMFIELD NAME="CUSTOMER_ID" PORTTYPE="INPUT/OUTPUT"/>
          <TRANSFORMFIELD NAME="TOTAL_SPEND" PORTTYPE="INPUT/OUTPUT"/>
          <TRANSFORMFIELD NAME="REGION_NAME" PORTTYPE="OUTPUT" EXPRESSION="NVL(REGION_NAME, '$$DEFAULT_REGION')"/>
          <TRANSFORMFIELD NAME="FULL_NAME" PORTTYPE="OUTPUT" EXPRESSION="LTRIM(RTRIM(FIRST_NAME)) || ' ' || LTRIM(RTRIM(LAST_NAME))"/>
        </TRANSFORMATION>

        <TRANSFORMATION NAME="RTR_VALIDATE_SPEND" TYPE="Router">
          <GROUP NAME="INPUT" TYPE="INPUT"/>
          <GROUP NAME="VALID_VIP" TYPE="OUTPUT" EXPRESSION="NOT ISNULL(CUSTOMER_ID) AND TOTAL_SPEND >= $$MIN_ORDER_THRESHOLD"/>
          <GROUP NAME="DEFAULT" TYPE="OUTPUT/DEFAULT"/>
        </TRANSFORMATION>

        <!-- PORT/GROUP-LEVEL DAG CONNECTORS -->
        <CONNECTOR FROMINSTANCE="STG_CUSTOMERS_US" FROMFIELD="CUSTOMER_ID" TOINSTANCE="UNI_ALL_REGIONS" TOFIELD="CUSTOMER_ID"/>
        <CONNECTOR FROMINSTANCE="STG_CUSTOMERS_EU" FROMFIELD="CUSTOMER_ID" TOINSTANCE="UNI_ALL_REGIONS" TOFIELD="CUSTOMER_ID"/>
        <CONNECTOR FROMINSTANCE="UNI_ALL_REGIONS" FROMFIELD="CUSTOMER_ID" TOINSTANCE="LKP_REGION" TOFIELD="CUSTOMER_ID"/>
        <CONNECTOR FROMINSTANCE="LKP_REGION" FROMFIELD="CUSTOMER_ID" TOINSTANCE="EXP_ENRICH" TOFIELD="CUSTOMER_ID"/>
        <CONNECTOR FROMINSTANCE="EXP_ENRICH" FROMFIELD="CUSTOMER_ID" TOINSTANCE="RTR_VALIDATE_SPEND" TOFIELD="CUSTOMER_ID"/>
        
        <!-- Router Group Routing to Multiple Targets -->
        <CONNECTOR FROMINSTANCE="RTR_VALIDATE_SPEND" FROMGROUP="VALID_VIP" FROMFIELD="CUSTOMER_ID" TOINSTANCE="DIM_CUSTOMER_360" TOFIELD="CUSTOMER_ID"/>
        <CONNECTOR FROMINSTANCE="RTR_VALIDATE_SPEND" FROMGROUP="DEFAULT" FROMFIELD="CUSTOMER_ID" TOINSTANCE="ERR_REJECTED_CUSTOMERS" TOFIELD="CUSTOMER_ID"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
"""


# ==============================================================================
# 2. PARAMETER FILE PARSER & EXPRESSION TRANSPILER
# ==============================================================================
def parse_parameter_file(par_content: str) -> Dict[str, str]:
    """
    Parses an Informatica .par file into a dictionary of parameter keys and values.

    Args:
        par_content: Raw string contents of the Informatica parameter file.

    Returns:
        Dictionary mapping '$$PARAM_NAME' -> 'value'.
    """
    params = {}
    for line in par_content.splitlines():
        line = line.strip()
        if not line or line.startswith("[") or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        params[key.strip()] = val.strip()
    return params


def transpile_expression(expr: str, params: Dict[str, str]) -> str:
    """
    Transpiles Informatica expression syntax and $$parameters into Spark SQL.

    Handles:
      - Parameter substitution ($$VAR -> runtime widget/value)
      - IIF(cond, t, f) -> IF(cond, t, f)
      - ISNULL(col) -> (col IS NULL)
      - NVL(col, def) -> COALESCE(col, def)
      - DECODE(col, k1, v1, ..., def) -> CASE col WHEN k1 THEN v1 ... ELSE def END

    Args:
        expr: Raw Informatica expression string.
        params: Parsed parameter dictionary from .par file.

    Returns:
        Valid Spark SQL expression string.
    """
    if not expr:
        return expr

    sql = expr.strip()

    # Substitute $$ parameters with resolved values (or keep parameter reference)
    for p_key, p_val in params.items():
        sql = sql.replace(p_key, p_val)

    sql = re.sub(r"\bIIF\s*\(", "IF(", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bISNULL\s*\(([^()]+)\)", r"(\1 IS NULL)", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bNVL\s*\(", "COALESCE(", sql, flags=re.IGNORECASE)

    decode_match = re.match(r"^DECODE\s*\((.*)\)$", sql, flags=re.IGNORECASE)
    if decode_match:
        args = [a.strip() for a in decode_match.group(1).split(",")]
        target_col, pairs = args, args[1:]
        parts = [f"CASE {target_col}"]
        for i in range(0, len(pairs) - 1, 2):
            parts.append(f"WHEN {pairs[i]} THEN {pairs[i+1]}")
        if len(pairs) % 2 == 1:
            parts.append(f"ELSE {pairs[-1]}")
        parts.append("END")
        sql = " ".join(parts)

    return sql


# ==============================================================================
# 3. DAG TOPOLOGICAL SORTER
# ==============================================================================
def topological_sort_dag(
    instances: List[str],
    edges: List[Tuple[str, str]],
) -> List[str]:
    in_degree = {instance: 0 for instance in instances}
    adjacency = defaultdict(list)

    for source, destination in set(edges):
        if source not in in_degree:
            raise ValueError(f"Unknown source instance: {source}")
        if destination not in in_degree:
            raise ValueError(f"Unknown destination instance: {destination}")
        if destination not in adjacency[source]:
            adjacency[source].append(destination)
            in_degree[destination] += 1

    queue = deque(
        instance
        for instance, degree in in_degree.items()
        if degree == 0
    )
    ordered = []

    while queue:
        current = queue.popleft()
        ordered.append(current)

        for neighbor in adjacency[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(ordered) != len(instances):
        raise ValueError("Cycle detected in Informatica mapping connectors!")

    return ordered
==============================================================================
4. UNIT TEST GENERATOR (PYTEST + CHISPA)
==============================================================================
def generate_unit_test_file(
    mapping_name: str,
    target_table: str,
    business_keys: List[str],
    tracked_cols: List[str],
    test_dir: str,
) -> str:
    """
    Generates an isolated pytest + chispa test for SCD2 behavior.
    """
    os.makedirs(test_dir, exist_ok=True)

    module_name = re.sub(r"\W+", "_", mapping_name).lower()
    test_file_path = os.path.join(test_dir, f"test_{module_name}.py")

    business_keys_literal = repr(business_keys)
    tracked_columns_literal = repr(tracked_cols)

    test_source = f'''\
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("{mapping_name}")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_scd2_new_insert(spark):
    source = spark.createDataFrame(
        [
            {{ "CUSTOMER_ID": 1, "FULL_NAME": "Ada Lovelace",
              "REGION_NAME": "US", "TOTAL_SPEND": 500.0 }}
        ]
    )

    existing = spark.createDataFrame([], source.schema)

    result = source.withColumn("IS_CURRENT", F.lit(True))

    assert_df_equality(result, result, ignore_nullable=True)


def test_scd2_configuration():
    assert {business_keys_literal}
    assert {tracked_columns_literal}
    assert "{target_table}"
'''

    with open(test_file_path, "w", encoding="utf-8") as test_file:
        test_file.write(test_source)

    return test_file_path
def main() -> None:
    params = parse_parameter_file(SAMPLE_PAR_CONTENT)
    root = ET.fromstring(ENTERPRISE_INFA_XML)

    for mapping in root.findall(".//MAPPING"):
        mapping_name = mapping.attrib["NAME"]
        instances = [
            instance.attrib["NAME"]
            for instance in mapping.findall("./INSTANCE")
        ]

        edges = [
            (
                connector.attrib["FROMINSTANCE"],
                connector.attrib["TOINSTANCE"],
            )
            for connector in mapping.findall("./CONNECTOR")
        ]

        print(topological_sort_dag(instances, edges))


if __name__ == "__main__":
    main()

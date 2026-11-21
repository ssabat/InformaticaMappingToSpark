# Below is a complete, enhanced Python script that parses a multi-mapping Informatica XML export containing Filters, Lookups, and complex Expressions, and executes each SCD Type 2 pipeline in PySpark.
# Enhanced Multi-Mapping Parser & PySpark SCD2 Engine in python
# Expression Transpiler (`translate_infa_expression`): Uses regex rules to rewrite Informatica-specific functions (`IIF`, `DECODE`, `ISNULL`, `NVL`) into valid Spark SQL expressions executed via `F.expr()`.
# Transformation Pipeline Execution : Before calculating the SCD2 MD5 hash, the script dynamically applies:
# Filters (`<TRANSFORMATION TYPE="Filter">`) via `.filter(F.expr(...))`
# Lookups (`<TRANSFORMATION TYPE="Lookup Procedure">`) via broadcast left joins
# Expressions (`<TRANSFORMATION TYPE="Expression">`) via `.withColumn(..., F.expr(...))`
# Batch Multi-Mapping Loop : Indexes all `<SOURCE>` and `<TARGET>` definitions at the folder level and loops through every `<MAPPING>` tag in the XML file.

import re
import xml.etree.ElementTree as ET
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==============================================================================
# 1. SAMPLE MULTI-MAPPING INFORMATICA POWERCENTER XML EXPORT
# ==============================================================================
SAMPLE_COMPLEX_INFA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
  <REPOSITORY NAME="PROD_REPO">
    <FOLDER NAME="EDW_CORE">
      <!-- SOURCES & LOOKUP TABLES -->
      <SOURCE NAME="STG_CUSTOMERS" OWNERNAME="raw">
        <SOURCEFIELD NAME="CUSTOMER_ID" DATATYPE="number" KEYTYPE="PRIMARY KEY"/>
        <SOURCEFIELD NAME="FIRST_NAME" DATATYPE="varchar2" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="LAST_NAME" DATATYPE="varchar2" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="COUNTRY_CODE" DATATYPE="varchar2" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="TIER_CODE" DATATYPE="varchar2" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="STATUS" DATATYPE="varchar2" KEYTYPE="NOT A KEY"/>
      </SOURCE>

      <SOURCE NAME="STG_PRODUCTS" OWNERNAME="raw">
        <SOURCEFIELD NAME="PRODUCT_ID" DATATYPE="number" KEYTYPE="PRIMARY KEY"/>
        <SOURCEFIELD NAME="SKU" DATATYPE="varchar2" KEYTYPE="PRIMARY KEY"/>
        <SOURCEFIELD NAME="PRODUCT_NAME" DATATYPE="varchar2" KEYTYPE="NOT A KEY"/>
        <SOURCEFIELD NAME="PRICE" DATATYPE="decimal" KEYTYPE="NOT A KEY"/>
      </SOURCE>

      <!-- TARGETS -->
      <TARGET NAME="DIM_CUSTOMERS" OWNERNAME="dim">
        <TARGETFIELD NAME="CUSTOMER_ID" KEYTYPE="FOREIGN KEY"/>
        <TARGETFIELD NAME="FULL_NAME" KEYTYPE="NOT A KEY"/>
        <TARGETFIELD NAME="COUNTRY_NAME" KEYTYPE="NOT A KEY"/>
        <TARGETFIELD NAME="TIER_DESC" KEYTYPE="NOT A KEY"/>
      </TARGET>

      <TARGET NAME="DIM_PRODUCTS" OWNERNAME="dim">
        <TARGETFIELD NAME="PRODUCT_ID" KEYTYPE="FOREIGN KEY"/>
        <TARGETFIELD NAME="SKU" KEYTYPE="FOREIGN KEY"/>
        <TARGETFIELD NAME="PRODUCT_NAME" KEYTYPE="NOT A KEY"/>
        <TARGETFIELD NAME="PRICE" KEYTYPE="NOT A KEY"/>
      </TARGET>

      <!-- MAPPING 1: Complex Customer SCD2 (Filter + Lookup + Expressions) -->
      <MAPPING NAME="m_LOAD_DIM_CUSTOMERS_SCD2">
        <INSTANCE NAME="STG_CUSTOMERS" TRANSFORMATION_TYPE="Source Definition"/>
        <INSTANCE NAME="DIM_CUSTOMERS" TRANSFORMATION_TYPE="Target Definition"/>

        <TRANSFORMATION NAME="FIL_ACTIVE_ONLY" TYPE="Filter">
          <TABLEATTRIBUTE NAME="Filter Condition" VALUE="STATUS = 'ACTIVE' AND NOT ISNULL(CUSTOMER_ID)"/>
        </TRANSFORMATION>

        <TRANSFORMATION NAME="LKP_COUNTRY" TYPE="Lookup Procedure">
          <TABLEATTRIBUTE NAME="Lookup table name" VALUE="dim.ref_countries"/>
          <TABLEATTRIBUTE NAME="Lookup condition" VALUE="COUNTRY_CODE = CODE"/>
          <TRANSFORMFIELD NAME="CODE" PORTTYPE="INPUT/LOOKUP"/>
          <TRANSFORMFIELD NAME="COUNTRY_NAME" PORTTYPE="LOOKUP/OUTPUT"/>
        </TRANSFORMATION>

        <TRANSFORMATION NAME="EXP_TRANSFORM_CUST" TYPE="Expression">
          <TRANSFORMFIELD NAME="CUSTOMER_ID" PORTTYPE="INPUT/OUTPUT"/>
          <TRANSFORMFIELD NAME="COUNTRY_NAME" PORTTYPE="INPUT/OUTPUT"/>
          <TRANSFORMFIELD NAME="FULL_NAME" PORTTYPE="OUTPUT"
                          EXPRESSION="LTRIM(RTRIM(FIRST_NAME)) || ' ' || LTRIM(RTRIM(LAST_NAME))"/>
          <TRANSFORMFIELD NAME="TIER_DESC" PORTTYPE="OUTPUT"
                          EXPRESSION="DECODE(TIER_CODE, 'G', 'Gold', 'S', 'Silver', 'Standard')"/>
          <TRANSFORMFIELD NAME="o_MD5_HASH" PORTTYPE="OUTPUT"
                          EXPRESSION="MD5(FULL_NAME || COUNTRY_NAME || TIER_DESC)"/>
        </TRANSFORMATION>
      </MAPPING>

      <!-- MAPPING 2: Composite Key Product SCD2 -->
      <MAPPING NAME="m_LOAD_DIM_PRODUCTS_SCD2">
        <INSTANCE NAME="STG_PRODUCTS" TRANSFORMATION_TYPE="Source Definition"/>
        <INSTANCE NAME="DIM_PRODUCTS" TRANSFORMATION_TYPE="Target Definition"/>

        <TRANSFORMATION NAME="EXP_CLEAN_PROD" TYPE="Expression">
          <TRANSFORMFIELD NAME="PRODUCT_ID" PORTTYPE="INPUT/OUTPUT"/>
          <TRANSFORMFIELD NAME="SKU" PORTTYPE="INPUT/OUTPUT"/>
          <TRANSFORMFIELD NAME="PRODUCT_NAME" PORTTYPE="INPUT/OUTPUT"/>
          <TRANSFORMFIELD NAME="PRICE" PORTTYPE="OUTPUT" EXPRESSION="IIF(ISNULL(PRICE), 0.0, PRICE)"/>
          <TRANSFORMFIELD NAME="o_MD5_HASH" PORTTYPE="OUTPUT" EXPRESSION="MD5(PRODUCT_NAME || PRICE)"/>
        </TRANSFORMATION>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
"""


# ==============================================================================
# 2. INFORMATICA EXPRESSION TO SPARK SQL TRANSPILER
# ==============================================================================
def translate_infa_expression(expr: str) -> str:
    """Converts common Informatica PowerCenter functions into Spark SQL syntax."""
    if not expr:
        return expr

    sql_expr = expr.strip()

    # 1. Replace Informatica string concatenation '||' with Spark concat_ws/concat
    # Note: In Spark SQL, '||' works for strings, or we can keep it native as Spark 3+ supports '||'
    
    # 2. Replace IIF(cond, true_val, false_val) -> IF(cond, true_val, false_val)
    sql_expr = re.sub(r"\bIIF\s*\(", "IF(", sql_expr, flags=re.IGNORECASE)

    # 3. Replace ISNULL(col) -> (col IS NULL)
    sql_expr = re.sub(r"\bISNULL\s*\(([^()]+)\)", r"(\1 IS NULL)", sql_expr, flags=re.IGNORECASE)

    # 4. Replace NVL(col, default) -> COALESCE(col, default)
    sql_expr = re.sub(r"\bNVL\s*\(", "COALESCE(", sql_expr, flags=re.IGNORECASE)

    # 5. Replace DECODE(col, k1, v1, k2, v2, default) -> CASE col WHEN k1 THEN v1 ... ELSE default END
    decode_match = re.match(r"^DECODE\s*\((.*)\)$", sql_expr, flags=re.IGNORECASE)
    if decode_match:
        args = [a.strip() for a in decode_match.group(1).split(",")]
        target_col = args[0]
        pairs = args[1:]
        case_parts = [f"CASE {target_col}"]
        for i in range(0, len(pairs) - 1, 2):
            case_parts.append(f"WHEN {pairs[i]} THEN {pairs[i+1]}")
        if len(pairs) % 2 == 1:
            case_parts.append(f"ELSE {pairs[-1]}")
        case_parts.append("END")
        sql_expr = " ".join(case_parts)

    return sql_expr


# ==============================================================================
# 3. MULTI-MAPPING XML METADATA PARSER
# ==============================================================================
def parse_all_mappings_from_xml(xml_content: str) -> list[dict]:
    """Parses all <MAPPING> elements in an Informatica XML folder export."""
    root = ET.fromstring(xml_content)

    # Index all <SOURCE> and <TARGET> definitions by name
    sources_meta = {}
    for src in root.findall(".//SOURCE"):
        name = src.attrib["NAME"]
        schema = src.attrib.get("OWNERNAME", "default")
        bkeys = [
            f.attrib["NAME"].lower()
            for f in src.findall("SOURCEFIELD")
            if f.attrib.get("KEYTYPE") == "PRIMARY KEY"
        ]
        sources_meta[name] = {
            "table": f"{schema}.{name.lower()}",
            "business_keys": bkeys,
        }

    targets_meta = {}
    for tgt in root.findall(".//TARGET"):
        name = tgt.attrib["NAME"]
        schema = tgt.attrib.get("OWNERNAME", "default")
        cols = [f.attrib["NAME"].lower() for f in tgt.findall("TARGETFIELD")]
        targets_meta[name] = {
            "table": f"{schema}.{name.lower()}",
            "columns": cols,
        }

    parsed_mappings = []

    for mapping in root.findall(".//MAPPING"):
        mapping_name = mapping.attrib["NAME"]

        # Identify Source & Target instances linked in this mapping
        src_inst = mapping.find(".//INSTANCE[@TRANSFORMATION_TYPE='Source Definition']")
        tgt_inst = mapping.find(".//INSTANCE[@TRANSFORMATION_TYPE='Target Definition']")
        src_info = sources_meta[src_inst.attrib["NAME"]]
        tgt_info = targets_meta[tgt_inst.attrib["NAME"]]

        # Extract Filter conditions
        filters = []
        for fil in mapping.findall(".//TRANSFORMATION[@TYPE='Filter']"):
            cond_attr = fil.find("TABLEATTRIBUTE[@NAME='Filter Condition']")
            if cond_attr is not None:
                filters.append(translate_infa_expression(cond_attr.attrib["VALUE"]))

        # Extract Lookup definitions
        lookups = []
        for lkp in mapping.findall(".//TRANSFORMATION[@TYPE='Lookup Procedure']"):
            lkp_table = lkp.find("TABLEATTRIBUTE[@NAME='Lookup table name']").attrib["VALUE"]
            lkp_cond = lkp.find("TABLEATTRIBUTE[@NAME='Lookup condition']").attrib["VALUE"]
            out_cols = [
                f.attrib["NAME"].lower()
                for f in lkp.findall("TRANSFORMFIELD")
                if "OUTPUT" in f.attrib.get("PORTTYPE", "")
            ]
            # Parse simple "SRC_COL = LKP_COL" condition
            src_key, lkp_key = [c.strip().lower() for c in lkp_cond.split("=")]
            lookups.append({
                "table": lkp_table.lower(),
                "src_key": src_key,
                "lkp_key": lkp_key,
                "output_cols": out_cols,
            })

        # Extract Expression derived columns (excluding MD5 port)
        expressions = {}
        for exp in mapping.findall(".//TRANSFORMATION[@TYPE='Expression']"):
            for tf in exp.findall("TRANSFORMFIELD"):
                col_name = tf.attrib["NAME"].lower()
                raw_expr = tf.attrib.get("EXPRESSION")
                if raw_expr and "md5" not in col_name:
                    expressions[col_name] = translate_infa_expression(raw_expr)
        audit_cols = {
            "row_hash",
            "is_current",
            "effective_start_date",
            "effective_end_date",
        }
        tracked_cols = [
            c
            for c in tgt_info["columns"]
            if c not in src_info["business_keys"]
            and c not in audit_cols
            and not c.endswith("_sk")
        ]

        parsed_mappings.append({
            "mapping_name": mapping_name,
            "source_table": src_info["table"],
            "target_table": tgt_info["table"],
            "business_keys": src_info["business_keys"],
            "tracked_cols": tracked_cols,
            "filters": filters,
            "lookups": lookups,
            "expressions": expressions,
        })

    return parsed_mappings  

# ==============================================================================
# 4. DYNAMIC PYSPARK PIPELINE + SCD2 EXECUTOR
# ==============================================================================
def execute_mapping_scd2(spark: SparkSession, cfg: dict):
    print(f"\n--- Executing Mapping: {cfg['mapping_name']} ---")
    df = spark.read.table(cfg["source_table"])

    # 1. Apply Informatica Filter Transformations
    for f_cond in cfg["filters"]:
        print(f"Applying Filter: {f_cond}")
        df = df.filter(F.expr(f_cond))

    # 2. Apply Informatica Lookup Transformations
    for lkp in cfg["lookups"]:
        print(
            f"Applying Lookup: {lkp['table']} "
            f"ON {lkp['src_key']} = {lkp['lkp_key']}"
        )
        lkp_df = (
            spark.read.table(lkp["table"])
            .select(lkp["lkp_key"], *lkp["output_cols"])
        )
        df = (
            df.join(
                F.broadcast(lkp_df),
                df[lkp["src_key"]] == lkp_df[lkp["lkp_key"]],
                how="left",
            )
            .drop(lkp_df[lkp["lkp_key"]])
        )

    # 3. Apply Expression Transformations
    for col_name, spark_sql_expr in cfg["expressions"].items():
        print(f"Applying Expression: {col_name} = {spark_sql_expr}")
        df = df.withColumn(col_name, F.expr(spark_sql_expr))

    business_keys = cfg["business_keys"]
    tracked_cols = cfg["tracked_cols"]
    all_target_data_cols = business_keys + tracked_cols

    src_df = df.select(*all_target_data_cols).withColumn(
        "row_hash",
        F.md5(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                    for c in tracked_cols
                ],
            )
        ),
    )

    target_delta = DeltaTable.forName(spark, cfg["target_table"])
    active_target_df = target_delta.toDF().filter(F.col("is_current") == True)

    joined_df = src_df.alias("src").join(
        active_target_df.alias("tgt"),
        on=business_keys,
        how="left",
    )

    staged_updates_df = joined_df.filter(
        F.col("tgt.row_hash").isNull()
        | (F.col("src.row_hash") != F.col("tgt.row_hash"))
    ).select("src.*")

    concat_bk_expr = F.concat_ws(
        "||",
        *[F.col(k).cast("string") for k in business_keys],
    )
    rows_to_insert = staged_updates_df.withColumn(
        "merge_key", F.lit(None).cast("string")
    )
    rows_to_expire = (
        joined_df.filter(
            F.col("tgt.row_hash").isNotNull()
            & (F.col("src.row_hash") != F.col("tgt.row_hash"))
        )
        .select("src.*")
        .withColumn("merge_key", concat_bk_expr)
    )

    final_staged_df = rows_to_insert.unionByName(rows_to_expire)
    tgt_bk_expr = "CONCAT_WS('||', " + ", ".join(
        f"CAST(tgt.{k} AS STRING)" for k in business_keys
    ) + ")"
    merge_condition = (
        f"{tgt_bk_expr} = stg.merge_key AND tgt.is_current = true"
    )
    current_ts = F.current_timestamp()

    insert_values = {
        col: f"stg.{col}" for col in all_target_data_cols
    }
    insert_values.update({
        "row_hash": "stg.row_hash",
        "is_current": F.lit(True),
        "effective_start_date": current_ts,
        "effective_end_date": (
            F.lit("9999-12-31 23:59:59").cast("timestamp")
        ),
    })

    (
        target_delta.alias("tgt")
        .merge(final_staged_df.alias("stg"), merge_condition)
        .whenMatchedUpdate(
            set={
                "is_current": F.lit(False),
                "effective_end_date": current_ts,
            }
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    print(f"Completed SCD2 Merge for {cfg['target_table']}")

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Infa_Complex_XML_To_PySpark")
        .getOrCreate()
    )
    all_mappings = parse_all_mappings_from_xml(SAMPLE_COMPLEX_INFA_XML)

    for mapping_cfg in all_mappings:
        execute_mapping_scd2(spark, mapping_cfg)

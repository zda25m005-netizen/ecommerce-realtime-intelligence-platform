"""
Natural Language Query Interface - Bonus (+5%)
Users query the Gold Delta Lake in plain English.
Ollama (Llama 3) translates to read-only Spark SQL.
Includes SQL injection prevention.
"""

import os
import re
import ast
import json
import logging
import requests
from typing import Optional, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nl_query")

OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://ollama:11434')
MODEL_NAME = os.getenv('OLLAMA_MODEL', 'llama3')

# Gold Delta Lake tables available for querying
GOLD_TABLES = {
    'user_item_interactions': {
        'description': 'User-item interaction matrix with implicit ratings',
        'columns': ['user_id', 'item_id', 'rating', 'interaction_count',
                     'last_interaction', 'num_sessions', 'has_purchased', 'has_carted'],
    },
    'product_features': {
        'description': 'Product features including demand, price, and inventory',
        'columns': ['item_id', 'total_events', 'view_count', 'cart_count', 'purchase_count',
                     'unique_users', 'view_to_cart_rate', 'cart_to_purchase_rate',
                     'current_stock', 'current_price', 'previous_price', 'avg_price',
                     'category', 'demand_score', 'stock_velocity'],
    },
    'user_profiles': {
        'description': 'Aggregated user behavior profiles',
        'columns': ['user_id', 'total_actions', 'unique_items_interacted', 'total_sessions',
                     'total_views', 'total_carts', 'total_purchases', 'engagement_score',
                     'conversion_rate', 'primary_device', 'primary_region'],
    },
    'rolling_item_ctr': {
        'description': 'Rolling 1-hour CTR per product',
        'columns': ['item_id', 'window_start', 'window_end', 'total_events_1h',
                     'views_1h', 'carts_1h', 'purchases_1h', 'unique_users_1h',
                     'ctr_1h', 'conversion_1h'],
    },
}

# SQL injection prevention: whitelist allowed operations
ALLOWED_SQL_KEYWORDS = {
    'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN',
    'LIKE', 'IS', 'NULL', 'AS', 'ORDER', 'BY', 'ASC', 'DESC',
    'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'JOIN', 'ON', 'LEFT',
    'RIGHT', 'INNER', 'OUTER', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
    'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST',
    'ROUND', 'COALESCE', 'UNION', 'ALL',
}

# Absolutely forbidden keywords
FORBIDDEN_KEYWORDS = {
    'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE',
    'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'MERGE', 'REPLACE',
    'SET', 'CALL', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
}


def build_schema_prompt():
    """Build the schema context for the LLM."""
    schema_text = "Available tables in the Gold Delta Lake:\n\n"
    for table_name, info in GOLD_TABLES.items():
        schema_text += f"Table: {table_name}\n"
        schema_text += f"  Description: {info['description']}\n"
        schema_text += f"  Columns: {', '.join(info['columns'])}\n\n"
    return schema_text


def validate_sql(sql: str) -> Tuple[bool, str]:
    """
    Validate generated SQL for safety.
    Returns (is_safe, reason).
    """
    sql_upper = sql.upper().strip()

    # Must start with SELECT
    if not sql_upper.startswith('SELECT'):
        return False, "Query must start with SELECT (read-only)"

    # Check for forbidden keywords
    for keyword in FORBIDDEN_KEYWORDS:
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_upper):
            return False, f"Forbidden keyword detected: {keyword}"

    # No semicolons (prevent multi-statement injection)
    if ';' in sql and not sql.strip().endswith(';'):
        return False, "Multiple statements not allowed"

    # No comments (prevent comment-based injection)
    if '--' in sql or '/*' in sql:
        return False, "SQL comments not allowed"

    # Table names must be in whitelist
    for table_name in GOLD_TABLES:
        sql_upper = sql_upper.replace(table_name.upper(), '')

    # Check no unknown table references after removing known ones
    from_match = re.findall(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
    join_match = re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)

    all_tables = from_match + join_match
    for table in all_tables:
        if table.lower() not in GOLD_TABLES:
            return False, f"Unknown table: {table}. Only Gold tables are accessible."

    # Limit result size
    if 'LIMIT' not in sql_upper:
        sql = sql.rstrip(';').strip() + ' LIMIT 100'

    return True, sql


def translate_to_sql(natural_query: str) -> Optional[str]:
    """
    Use Ollama (Llama 3) to translate natural language to Spark SQL.
    """
    schema = build_schema_prompt()

    prompt = f"""You are a SQL translator. Convert the user's natural language question
into a valid Spark SQL query. Use ONLY the tables and columns listed below.
Return ONLY the SQL query, nothing else. No explanations.

RULES:
- Only SELECT queries (read-only)
- Only use tables listed below
- Always include LIMIT (max 100)
- Use standard SQL syntax

{schema}

User question: {natural_query}

SQL query:"""

    try:
        response = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                'model': MODEL_NAME,
                'prompt': prompt,
                'stream': False,
                'options': {
                    'temperature': 0.1,
                    'top_p': 0.9,
                    'num_predict': 256,
                },
            },
            timeout=30,
        )

        if response.status_code == 200:
            result = response.json()
            sql = result.get('response', '').strip()

            # Clean up: remove markdown code blocks if present
            sql = re.sub(r'^```sql\s*', '', sql)
            sql = re.sub(r'^```\s*', '', sql)
            sql = re.sub(r'\s*```$', '', sql)
            sql = sql.strip()

            logger.info(f"Generated SQL: {sql}")
            return sql
        else:
            logger.error(f"Ollama error: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to Ollama. Is it running?")
        return None
    except Exception as e:
        logger.error(f"Translation error: {e}")
        return None


def execute_query(spark, sql: str) -> dict:
    """Execute validated SQL query against Gold Delta tables."""
    # Register Gold tables as temp views
    from pyspark.sql import SparkSession

    for table_name in GOLD_TABLES:
        try:
            gold_path = f"s3a://gold/{table_name}"
            df = spark.read.format("delta").load(gold_path)
            df.createOrReplaceTempView(table_name)
        except Exception as e:
            logger.warning(f"Could not load table {table_name}: {e}")

    # Execute the query
    result_df = spark.sql(sql)
    rows = result_df.collect()

    return {
        'columns': result_df.columns,
        'rows': [row.asDict() for row in rows],
        'count': len(rows),
    }


def query(natural_query: str, spark=None) -> dict:
    """
    Full pipeline: Natural Language -> SQL -> Validate -> Execute -> Result
    """
    logger.info(f"NL Query: {natural_query}")

    # Step 1: Translate to SQL
    sql = translate_to_sql(natural_query)
    if not sql:
        return {'error': 'Failed to translate query', 'sql': None}

    # Step 2: Validate SQL
    is_safe, validated_sql = validate_sql(sql)
    if not is_safe:
        return {'error': f'SQL validation failed: {validated_sql}', 'sql': sql}

    # If validated_sql is a modified version (e.g., LIMIT added)
    final_sql = validated_sql if isinstance(validated_sql, str) else sql

    # Step 3: Execute if Spark session provided
    if spark:
        try:
            result = execute_query(spark, final_sql)
            return {'sql': final_sql, 'result': result}
        except Exception as e:
            return {'error': f'Execution error: {str(e)}', 'sql': final_sql}
    else:
        return {'sql': final_sql, 'note': 'Spark session not provided, returning SQL only'}


# ── Interactive CLI ──────────────────────────────────────────────────────────

def main():
    """Interactive CLI for natural language queries."""
    print("=" * 60)
    print("E-Commerce Intelligence Platform")
    print("Natural Language Query Interface")
    print("=" * 60)
    print("\nAsk questions about your e-commerce data in plain English.")
    print("Type 'quit' to exit.\n")

    print("Example queries:")
    print("  - Which products were viewed over 1000 times but never bought?")
    print("  - Show me the top 10 users by engagement score")
    print("  - What is the average conversion rate by device type?")
    print("  - Which items have stock below 10 and high demand?")
    print()

    while True:
        try:
            user_input = input("Ask > ").strip()
            if user_input.lower() in ('quit', 'exit', 'q'):
                print("Goodbye!")
                break
            if not user_input:
                continue

            result = query(user_input)
            print(f"\nGenerated SQL: {result.get('sql', 'N/A')}")

            if 'error' in result:
                print(f"Error: {result['error']}")
            elif 'result' in result:
                print(f"Results ({result['result']['count']} rows):")
                for row in result['result']['rows'][:10]:
                    print(f"  {row}")
            print()

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except EOFError:
            break


if __name__ == '__main__':
    main()

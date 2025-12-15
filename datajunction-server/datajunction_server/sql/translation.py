"""
Function translation layer for Spark SQL to other dialects.

DJ uses Spark SQL as its internal representation. This module provides translation
from Spark function names to dialect-specific equivalents for functions that
SQLGlot doesn't handle automatically.

Example translations:
- Spark hll_sketch_agg -> Druid DS_HLL
- Spark hll_union -> Druid DS_HLL
- Spark hll_sketch_estimate -> Druid APPROX_COUNT_DISTINCT_DS_HLL
"""

import re
from dataclasses import dataclass
from typing import Callable, Optional

from datajunction_server.models.engine import Dialect


@dataclass
class FunctionTranslation:
    """
    Defines how to translate a Spark function to a target dialect.

    Attributes:
        target_name: The dialect-specific function name
        arg_transformer: Optional function to transform arguments
    """

    target_name: str
    arg_transformer: Optional[Callable[[list[str]], list[str]]] = None


# =============================================================================
# Spark -> Target Dialect Translation Registry
# =============================================================================
# Maps Spark function names to their equivalents in other dialects.
# If a dialect is not listed, the Spark function name is used as-is.
# =============================================================================

SPARK_FUNCTION_TRANSLATIONS: dict[str, dict[Dialect, FunctionTranslation]] = {
    # HLL sketch functions
    "hll_sketch_agg": {
        Dialect.DRUID: FunctionTranslation(target_name="DS_HLL"),
        Dialect.TRINO: FunctionTranslation(target_name="approx_set"),
    },
    "hll_union": {
        Dialect.DRUID: FunctionTranslation(target_name="DS_HLL"),
        Dialect.TRINO: FunctionTranslation(target_name="merge"),
    },
    "hll_sketch_estimate": {
        Dialect.DRUID: FunctionTranslation(target_name="APPROX_COUNT_DISTINCT_DS_HLL"),
        Dialect.TRINO: FunctionTranslation(target_name="cardinality"),
    },
}


def get_translation(
    spark_function: str,
    target_dialect: Dialect,
) -> Optional[FunctionTranslation]:
    """
    Get the translation for a Spark function to a target dialect.

    Args:
        spark_function: The Spark function name (e.g., "hll_sketch_agg")
        target_dialect: The target SQL dialect

    Returns:
        FunctionTranslation if translation needed, None otherwise
    """
    spark_lower = spark_function.lower()
    if spark_lower in SPARK_FUNCTION_TRANSLATIONS:
        dialect_translations = SPARK_FUNCTION_TRANSLATIONS[spark_lower]
        return dialect_translations.get(target_dialect)
    return None


def translate_function_name(
    spark_function: str,
    target_dialect: Dialect,
) -> str:
    """
    Translate a Spark function name to a target dialect.

    Args:
        spark_function: The Spark function name
        target_dialect: The target SQL dialect

    Returns:
        The dialect-specific function name, or the original if no translation needed
    """
    # Spark -> Spark is identity
    if target_dialect == Dialect.SPARK:
        return spark_function

    translation = get_translation(spark_function, target_dialect)
    if translation:
        return translation.target_name
    return spark_function


def translate_sql(
    sql: str,
    target_dialect: Dialect,
) -> str:
    """
    Translate Spark SQL to a target dialect.

    This performs string replacement of function names that SQLGlot doesn't handle.
    For more complex translations, a proper AST transformation would be needed.

    Args:
        sql: The SQL string in Spark dialect
        target_dialect: The target SQL dialect

    Returns:
        SQL with Spark functions replaced by dialect-specific equivalents
    """
    # Spark -> Spark is identity
    if target_dialect == Dialect.SPARK:
        return sql

    result = sql

    for spark_func, dialect_translations in SPARK_FUNCTION_TRANSLATIONS.items():
        if target_dialect in dialect_translations:
            translation = dialect_translations[target_dialect]
            # Case-insensitive replacement of function name
            # Match function name followed by opening parenthesis
            pattern = re.compile(
                rf"\b{re.escape(spark_func)}\s*\(",
                re.IGNORECASE,
            )
            result = pattern.sub(f"{translation.target_name}(", result)

    return result

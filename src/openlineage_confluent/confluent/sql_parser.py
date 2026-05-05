"""Parse Flink SQL statements to extract input and output table references.

Handles common patterns:
    INSERT INTO `topic-name` SELECT ... FROM `other-topic` JOIN `yet-another`
    INSERT INTO `sink` SELECT ... FROM `src1`, `src2`
"""

from __future__ import annotations

import re

# Regex for backtick-quoted identifiers  (`my-topic`)
_BACKTICK_ID = r"`([^`]+)`"
# Regex for plain identifiers (no special chars)
_PLAIN_ID = r"([A-Za-z_][A-Za-z0-9_\-]*)"

# Combined identifier pattern (prefer backtick, fall back to plain)
_ID = rf"(?:{_BACKTICK_ID}|{_PLAIN_ID})"


def _strip_comments(sql: str) -> str:
    """Remove -- line comments and /* block comments */."""
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return sql


def extract_output_tables(sql: str) -> list[str]:
    """Return the table(s) written by INSERT INTO / CREATE TABLE AS SELECT."""
    sql = _strip_comments(sql)
    pattern = rf"(?:INSERT\s+INTO|CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)\s*{_ID}"
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return [m[0] or m[1] for m in matches if m[0] or m[1]]


def extract_input_tables(sql: str) -> list[str]:
    """Return all tables read in FROM / JOIN clauses.

    Excludes the INSERT INTO target (which is NOT a source).
    """
    sql = _strip_comments(sql)

    # Collect everything after FROM and JOIN keywords
    raw: list[str] = []
    for kw in ["FROM", "JOIN"]:
        pattern = rf"(?<!\w){kw}\s+{_ID}"
        for m in re.finditer(pattern, sql, re.IGNORECASE):
            name = m.group(1) or m.group(2)
            if name:
                raw.append(name)

    # Remove subquery markers / SQL keywords that sneak through
    sql_keywords = {
        "SELECT", "WHERE", "ON", "AND", "OR", "NOT", "NULL", "AS", "CASE",
        "WHEN", "THEN", "ELSE", "END", "GROUP", "ORDER", "BY", "HAVING",
        "LIMIT", "OFFSET", "INNER", "LEFT", "RIGHT", "OUTER", "CROSS",
        "LATERAL", "TABLESAMPLE", "UNNEST",
    }
    return [t for t in raw if t.upper() not in sql_keywords]


def parse_statement(sql: str) -> tuple[list[str], list[str]]:
    """Return (input_tables, output_tables) for a Flink SQL statement."""
    inputs  = extract_input_tables(sql)
    outputs = extract_output_tables(sql)
    # Inputs must not overlap with outputs (avoid self-reference)
    output_set = set(outputs)
    inputs = [t for t in inputs if t not in output_set]
    return inputs, outputs

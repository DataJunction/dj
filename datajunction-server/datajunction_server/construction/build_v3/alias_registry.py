"""
This module provides the AliasRegistry class which maps semantic names
(like 'orders.customer.country') to clean SQL aliases (like 'country').
"""

from __future__ import annotations

import re


class AliasRegistry:
    """
    Maps semantic names to clean, unique SQL aliases.

    The registry tries to produce the shortest unique alias:
    - 'orders.customer.country' -> 'country' (if unique)
    - 'orders.customer.country' -> 'customer_country' (if 'country' taken)
    - 'orders.customer.country' -> 'orders_customer_country' (if both taken)
    - 'orders.customer.country' -> 'country_1' (fallback with numeric suffix)

    Role support:
    - 'v3.location.country[from]' -> 'country_from' (role incorporated into alias)
    - 'v3.location.country[to]' -> 'country_to'
    - 'v3.date.year[customer->registration]' -> 'year_registration' (last part of role path)

    Usage:
        registry = AliasRegistry()

        alias1 = registry.register('orders.customer.country')  # -> 'country'
        alias2 = registry.register('users.country')  # -> 'users_country' (conflict)
        alias3 = registry.register('v3.location.country[from]')  # -> 'country_from'
        alias4 = registry.register('v3.location.country[to]')  # -> 'country_to'

        # Later, look up the alias
        alias = registry.get_alias('orders.customer.country')  # -> 'country'
        semantic = registry.get_semantic('country')  # -> 'orders.customer.country'
    """

    # Characters that are not valid in SQL identifiers (will be replaced with _)
    INVALID_CHARS = re.compile(r"[^a-zA-Z0-9_]")

    # Pattern to extract role from semantic name: "node.column[role]" or "node.column[hop->role]"
    ROLE_PATTERN = re.compile(r"^(.+)\[([^\]]+)\]$")

    def __init__(self):
        self._semantic_to_alias: dict[str, str] = {}
        self._alias_to_semantic: dict[str, str] = {}
        self._used_aliases: set[str] = set()

    def register(self, semantic_name: str) -> str:
        """
        Register a semantic name and return its SQL alias.

        If already registered, returns the existing alias.
        Otherwise, generates a new unique alias.

        Args:
            semantic_name: Full semantic name (e.g., 'orders.customer.country')

        Returns:
            Clean SQL alias (e.g., 'country')
        """
        # Return existing alias if already registered
        if semantic_name in self._semantic_to_alias:
            return self._semantic_to_alias[semantic_name]

        # Generate a new unique alias
        alias = self._generate_alias(semantic_name)
        self._register(semantic_name, alias)
        return alias

    def get_alias(self, semantic_name: str) -> str | None:
        """Get the alias for a semantic name, or None if not registered."""
        return self._semantic_to_alias.get(semantic_name)

    def get_semantic(self, alias: str) -> str | None:
        """Get the semantic name for an alias, or None if not found."""
        return self._alias_to_semantic.get(alias)

    def is_registered(self, semantic_name: str) -> bool:
        """Check if a semantic name is already registered."""
        return semantic_name in self._semantic_to_alias

    def _register(self, semantic_name: str, alias: str) -> None:
        """Internal: register a semantic name -> alias mapping."""
        self._semantic_to_alias[semantic_name] = alias
        self._alias_to_semantic[alias] = semantic_name
        self._used_aliases.add(alias)

    def _generate_alias(self, semantic_name: str) -> str:
        """
        Generate a unique alias for a semantic name.

        Strategy:
        1. Extract role if present: 'v3.location.country[from]' -> base='v3.location.country', role='from'
        2. If role exists, always include it: 'country_from' (for clarity and predictability)
        3. If no role, try the last part: 'orders.customer.country' -> 'country'
        4. Try progressively longer suffixes: 'customer_country', 'orders_customer_country'
        5. Fall back to numeric suffix: 'country_1', 'country_2', ...
        """
        # Extract role if present (e.g., "v3.location.country[from]" -> base, role="from")
        base_name = semantic_name
        role_suffix = None

        role_match = self.ROLE_PATTERN.match(semantic_name)
        if role_match:
            base_name = role_match.group(1)
            role_path = role_match.group(2)
            # For multi-hop roles like "customer->registration", use the last part
            role_suffix = self._clean_part(role_path.split("->")[-1])

        # Split on dots and clean each part
        parts = [self._clean_part(p) for p in base_name.split(".")]
        parts = [p for p in parts if p]  # Remove empty parts

        if not parts:
            # Edge case: empty or all-invalid name
            return self._generate_fallback_alias("col")

        base_column = parts[-1]

        # If role exists, ALWAYS include it in the alias for clarity
        if role_suffix:
            candidate = f"{base_column}_{role_suffix}"
            if candidate not in self._used_aliases:
                return candidate
            # If that's taken, try with more qualification
            for i in range(1, len(parts)):
                candidate = "_".join(parts[-(i + 1) :]) + f"_{role_suffix}"
                if candidate not in self._used_aliases:
                    return candidate
            # Fallback with numeric suffix
            return self._generate_fallback_alias(f"{base_column}_{role_suffix}")

        # No role: try just the column name
        if base_column not in self._used_aliases:
            return base_column

        # Try progressively longer suffixes from the base path
        for i in range(1, len(parts)):
            candidate = "_".join(parts[-(i + 1) :])
            if candidate not in self._used_aliases:
                return candidate

        # Numeric suffix fallback
        return self._generate_fallback_alias(base_column)

    def _generate_fallback_alias(self, base: str) -> str:
        """Generate an alias with numeric suffix."""
        counter = 1
        while f"{base}_{counter}" in self._used_aliases:
            counter += 1
        return f"{base}_{counter}"

    def _clean_part(self, part: str) -> str:
        """
        Clean a name part to be a valid SQL identifier.

        - Replace invalid characters with underscores
        - Remove leading/trailing underscores
        - Collapse multiple underscores
        """
        # Replace invalid chars with underscore
        cleaned = self.INVALID_CHARS.sub("_", part)

        # Collapse multiple underscores
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")

        # Remove leading/trailing underscores
        cleaned = cleaned.strip("_")

        return cleaned

    def all_mappings(self) -> dict[str, str]:
        """Return all semantic -> alias mappings."""
        return dict(self._semantic_to_alias)

    def clear(self) -> None:
        """Clear all registrations."""
        self._semantic_to_alias.clear()
        self._alias_to_semantic.clear()
        self._used_aliases.clear()

    def __len__(self) -> int:
        """Number of registered aliases."""
        return len(self._semantic_to_alias)

    def __contains__(self, semantic_name: str) -> bool:
        """Check if a semantic name is registered."""
        return semantic_name in self._semantic_to_alias

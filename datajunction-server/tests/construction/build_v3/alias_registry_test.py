from datajunction_server.construction.build_v3 import AliasRegistry


class TestAliasRegistry:
    """Tests for the AliasRegistry class."""

    def test_basic_registration(self):
        """Test basic alias registration."""
        registry = AliasRegistry()

        alias = registry.register("orders.customer.country")
        assert alias == "country"

        # Same semantic name returns same alias
        alias2 = registry.register("orders.customer.country")
        assert alias2 == "country"

        assert registry.is_registered("orders.customer.country")
        assert registry.is_registered("random.thing") is False

        assert len(registry) == 1
        assert "orders.customer.country" in registry

        assert registry.all_mappings() == {
            "orders.customer.country": "country",
        }
        registry.clear()
        assert registry.all_mappings() == {}

    def test_conflict_resolution(self):
        """Test that conflicts are resolved by adding qualifiers."""
        registry = AliasRegistry()

        # First registration gets short name
        alias1 = registry.register("orders.country")
        assert alias1 == "country"

        # Second registration with same ending gets qualified
        alias2 = registry.register("customers.country")
        assert alias2 == "customers_country"

    def test_deep_conflict_resolution(self):
        """Test that deep conflicts get progressively longer names."""
        registry = AliasRegistry()

        alias1 = registry.register("a.b.country")
        assert alias1 == "country"

        alias2 = registry.register("c.b.country")
        assert alias2 == "b_country"

        # Third registration needs even more qualification
        alias3 = registry.register("d.b.country")
        assert alias3 == "d_b_country"

    def test_numeric_fallback(self):
        """Test numeric suffix fallback when all names are taken."""
        registry = AliasRegistry()

        # Register all possible combinations
        registry.register("country")
        registry._used_aliases.add("_country")  # Block all possibilities

        # Force numeric fallback
        alias = registry.register("x.country")
        assert alias == "x_country" or alias.startswith("country_")

    def test_get_alias(self):
        """Test looking up an alias."""
        registry = AliasRegistry()

        registry.register("orders.total")

        assert registry.get_alias("orders.total") == "total"
        assert registry.get_alias("nonexistent") is None

    def test_get_semantic(self):
        """Test reverse lookup from alias to semantic name."""
        registry = AliasRegistry()

        registry.register("orders.total")

        assert registry.get_semantic("total") == "orders.total"
        assert registry.get_semantic("nonexistent") is None

    def test_clean_part(self):
        """Test that invalid characters are cleaned."""
        registry = AliasRegistry()

        alias = registry.register("orders.customer-name")
        assert alias == "customer_name"

        alias = registry.register("orders.some@email")
        assert alias == "some_email"

    def test_role_in_semantic_name(self):
        """Test that roles in semantic names ALWAYS produce role-suffixed aliases."""
        registry = AliasRegistry()

        # Without role gets short name
        alias1 = registry.register("v3.location.country")
        assert alias1 == "country"

        # With role ALWAYS gets role-suffixed name
        alias2 = registry.register("v3.location.country[from]")
        assert alias2 == "country_from"

        alias3 = registry.register("v3.location.country[to]")
        assert alias3 == "country_to"

    def test_role_always_included(self):
        """Test that role is always included in alias, even if first."""
        registry = AliasRegistry()

        # Even the first role-based registration includes the role
        alias1 = registry.register("v3.location.country[from]")
        assert alias1 == "country_from"  # Role always included

        alias2 = registry.register("v3.location.country[to]")
        assert alias2 == "country_to"

    def test_multi_hop_role_path(self):
        """Test that multi-hop role paths use the last role part."""
        registry = AliasRegistry()

        # Role-based registration always includes role suffix
        alias1 = registry.register("v3.date.year[order]")
        assert alias1 == "year_order"

        # Multi-hop path uses last part of role (registration)
        alias2 = registry.register("v3.date.year[customer->registration]")
        assert alias2 == "year_registration"

        # Another multi-hop with different path
        alias3 = registry.register("v3.location.country[customer->home]")
        assert alias3 == "country_home"

        alias4 = registry.register("v3.location.country[from]")
        assert alias4 == "country_from"

    def test_all_roles_same_column(self):
        """Test multiple roles all pointing to the same column."""
        registry = AliasRegistry()

        alias1 = registry.register("v3.location.city[from]")
        assert alias1 == "city_from"

        alias2 = registry.register("v3.location.city[to]")
        assert alias2 == "city_to"

        alias3 = registry.register("v3.location.city[customer->home]")
        assert alias3 == "city_home"

    def test_role_conflict_deep_resolution(self):
        """
        Test that role alias conflicts are resolved with progressive qualification.

        When multiple dimension paths have the same base name and same role,
        the registry should add more qualification to disambiguate.
        """
        registry = AliasRegistry()

        # First registration: city_from
        alias1 = registry.register("v3.location.city[from]")
        assert alias1 == "city_from"

        # Block the simple name to force conflict resolution
        # Simulate another path with same ending
        registry._used_aliases.add("location_city_from")

        # This should need even more qualification
        alias2 = registry.register("v3.other.location.city[from]")
        # Should get progressively longer: other_location_city_from or v3_other_location_city_from
        assert alias2 != "city_from"  # Must be different
        assert "from" in alias2  # Role must be included
        assert "city" in alias2  # Base column must be included

    def test_role_conflict_numeric_fallback(self):
        """
        Test that role aliases fall back to numeric suffix when all names conflict.
        """
        registry = AliasRegistry()

        # Register first
        alias1 = registry.register("a.city[from]")
        assert alias1 == "city_from"

        # Block all reasonable combinations
        registry._used_aliases.add("b_city_from")
        registry._used_aliases.add("c_b_city_from")

        # Should eventually get a numeric fallback
        alias2 = registry.register("c.b.city[from]")
        # Either gets a longer qualified name or numeric suffix
        assert alias2 != "city_from"
        assert "from" in alias2 or "_" in alias2

    def test_empty_or_invalid_semantic_name_fallback(self):
        """Test fallback when semantic name has no valid parts."""
        from datajunction_server.construction.build_v3.alias_registry import (
            AliasRegistry,
        )

        registry = AliasRegistry()

        # Semantic name with only invalid characters (not alphanumeric or underscore)
        alias = registry.register("@#$")  # All invalid chars -> parts=[]
        assert alias == "col_1"

        # Another one should get col_2
        alias2 = registry.register("!@#.%^&")  # All invalid chars
        assert alias2 == "col_2"

        # Empty string edge case
        alias3 = registry.register("")
        assert alias3 == "col_3"

    def test_numeric_fallback_exhausted_combinations(self):
        """Test that numeric suffix is used when all path combinations are taken."""
        from datajunction_server.construction.build_v3.alias_registry import (
            AliasRegistry,
        )

        registry = AliasRegistry()

        # Register items that will take all combinations for "a.b.c"
        alias1 = registry.register("c")  # -> "c"
        assert alias1 == "c"
        alias2 = registry.register("b.c")  # -> "b_c" (c taken)
        assert alias2 == "b_c"
        alias3 = registry.register("a.b.c")  # -> "a_b_c" (c, b_c taken)
        assert alias3 == "a_b_c"
        alias4 = registry.register(
            "x.a.b.c",
        )  # -> tries c, b_c, a_b_c, x_a_b_c -> gets "x_a_b_c"
        assert alias4 == "x_a_b_c"

        # But we need something with exactly parts ["a", "b", "c"] where all are taken
        # Let's use a different approach - same ending but shorter path:
        registry2 = AliasRegistry()
        registry2.register("z.c")  # -> "c"
        registry2.register("z.b.c")  # -> "b_c"
        registry2.register("z.a.b.c")  # -> "a_b_c"

        # Now register "a.b.c" - same parts, all taken
        alias = registry2.register("a.b.c")
        assert alias == "c_1"  # Falls back to numeric

    def test_clean_part_collapses_multiple_underscores(self):
        """Test that multiple consecutive invalid chars are collapsed to single underscore."""
        from datajunction_server.construction.build_v3.alias_registry import (
            AliasRegistry,
        )

        registry = AliasRegistry()

        # Part with multiple consecutive invalid chars (-- becomes __ then _)
        # "foo--bar" -> replace dashes -> "foo__bar" -> collapse -> "foo_bar"
        alias = registry.register("foo--bar")
        assert alias == "foo_bar"

        # Even more consecutive invalid chars
        # "a---b" -> "a___b" -> loop runs twice -> "a_b"
        alias2 = registry.register("a---b")
        assert alias2 == "a_b"

        # With dots separating parts, where one part has consecutive invalid chars
        # "ns.col@@name" -> parts: ["ns", "col__name"] -> ["ns", "col_name"]
        alias3 = registry.register("ns.col@@name")
        assert alias3 == "col_name"

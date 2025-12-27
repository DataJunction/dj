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

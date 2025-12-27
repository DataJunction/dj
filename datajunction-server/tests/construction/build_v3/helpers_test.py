from datajunction_server.construction.build_v3.builder import parse_dimension_ref


class TestDimensionRefParsing:
    """Tests for dimension reference parsing."""

    def test_simple_dimension_ref(self):
        """Test parsing a simple dimension reference."""
        ref = parse_dimension_ref("v3.customer.name")
        assert ref.node_name == "v3.customer"
        assert ref.column_name == "name"
        assert ref.role is None

    def test_dimension_ref_with_role(self):
        """Test parsing a dimension reference with role."""
        ref = parse_dimension_ref("v3.date.month[order]")
        assert ref.node_name == "v3.date"
        assert ref.column_name == "month"
        assert ref.role == "order"

    def test_dimension_ref_with_multi_hop_role(self):
        """Test parsing a dimension reference with multi-hop role."""
        ref = parse_dimension_ref("v3.date.month[customer->registration]")
        assert ref.node_name == "v3.date"
        assert ref.column_name == "month"
        assert ref.role == "customer->registration"

    def test_dimension_ref_with_deep_role_path(self):
        """Test parsing dimension reference with deep role path."""
        ref = parse_dimension_ref("v3.location.country[customer->home]")
        assert ref.node_name == "v3.location"
        assert ref.column_name == "country"
        assert ref.role == "customer->home"

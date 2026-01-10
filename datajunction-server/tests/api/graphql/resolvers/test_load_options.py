"""
Unit tests for the load options functions in nodes.py.
Tests for load_node_options, load_node_revision_options,
build_cube_metrics_node_revision_options, and build_cube_dimensions_node_revision_options.
"""

from sqlalchemy.orm import load_only

from datajunction_server.api.graphql.resolvers.nodes import (
    build_cube_dimensions_node_revision_options,
    build_cube_metrics_node_revision_options,
    load_node_options,
    load_node_revision_options,
)
from datajunction_server.database.node import NodeRevision as DBNodeRevision


def get_relationship_key(opt):
    """Extract the relationship key from a loader option."""
    if hasattr(opt, "context") and opt.context:
        ctx = opt.context[0]
        if hasattr(ctx, "path") and hasattr(ctx.path, "path"):
            # path.path is a tuple like (Mapper, Relationship, Mapper)
            # The relationship is at index 1
            path_tuple = ctx.path.path
            if len(path_tuple) >= 2 and hasattr(path_tuple[1], "key"):
                return path_tuple[1].key
    return None


def get_strategy(opt):
    """Extract the strategy from a loader option."""
    if hasattr(opt, "context") and opt.context:
        ctx = opt.context[0]
        if hasattr(ctx, "strategy"):
            return ctx.strategy
    return None


def has_noload_for(options, relationship_name):
    """Check if a noload option exists for the given relationship."""
    for opt in options:
        key = get_relationship_key(opt)
        strategy = get_strategy(opt)
        if key == relationship_name and strategy == (("lazy", "noload"),):
            return True
    return False


def has_selectinload_for(options, relationship_name):
    """Check if a selectinload option exists for the given relationship."""
    for opt in options:
        key = get_relationship_key(opt)
        strategy = get_strategy(opt)
        if key == relationship_name and strategy == (("lazy", "selectin"),):
            return True
    return False


def has_joinedload_for(options, relationship_name):
    """Check if a joinedload option exists for the given relationship."""
    for opt in options:
        key = get_relationship_key(opt)
        strategy = get_strategy(opt)
        if key == relationship_name and strategy == (("lazy", "joined"),):
            return True
    return False


class TestLoadNodeOptions:
    """Tests for load_node_options function."""

    def test_with_current_only(self):
        """Only current field requested - should noload revisions."""
        fields = {"current": {"displayName": {}}}
        options = load_node_options(fields)

        assert has_noload_for(options, "revisions")
        assert has_joinedload_for(options, "current")
        assert has_noload_for(options, "created_by")
        assert has_noload_for(options, "owners")
        assert has_noload_for(options, "history")
        assert has_noload_for(options, "tags")
        assert has_noload_for(options, "children")

    def test_with_revisions(self):
        """revisions field requested - should joinedload revisions."""
        fields = {"revisions": {"displayName": {}}}
        options = load_node_options(fields)

        assert has_joinedload_for(options, "revisions")
        assert has_noload_for(options, "current")

    def test_with_created_by(self):
        """created_by field requested - should selectinload created_by."""
        fields = {"created_by": {"username": {}}}
        options = load_node_options(fields)

        assert has_selectinload_for(options, "created_by")

    def test_with_owners(self):
        """owners field requested - should selectinload owners."""
        fields = {"owners": {"username": {}}}
        options = load_node_options(fields)

        assert has_selectinload_for(options, "owners")

    def test_with_edited_by(self):
        """edited_by field requested - should selectinload history."""
        fields = {"edited_by": {}}
        options = load_node_options(fields)

        assert has_selectinload_for(options, "history")

    def test_with_tags(self):
        """tags field requested - should selectinload tags."""
        fields = {"tags": {"name": {}}}
        options = load_node_options(fields)

        assert has_selectinload_for(options, "tags")

    def test_children_always_noloaded(self):
        """children should always be noloaded (not exposed in GraphQL)."""
        fields = {"name": {}}
        options = load_node_options(fields)

        assert has_noload_for(options, "children")

    def test_all_fields_requested(self):
        """All fields requested - should load all relationships."""
        fields = {
            "current": {"displayName": {}},
            "revisions": {"displayName": {}},
            "created_by": {"username": {}},
            "owners": {"username": {}},
            "edited_by": {},
            "tags": {"name": {}},
        }
        options = load_node_options(fields)

        assert has_joinedload_for(options, "current")
        assert has_joinedload_for(options, "revisions")
        assert has_selectinload_for(options, "created_by")
        assert has_selectinload_for(options, "owners")
        assert has_selectinload_for(options, "history")
        assert has_selectinload_for(options, "tags")
        assert has_noload_for(options, "children")


class TestLoadNodeRevisionOptions:
    """Tests for load_node_revision_options function."""

    def test_minimal_fields(self):
        """Minimal fields - should noload most relationships."""
        fields = {"displayName": {}}
        options = load_node_revision_options(fields)

        assert has_noload_for(options, "columns")
        assert has_noload_for(options, "catalog")
        assert has_noload_for(options, "parents")
        assert has_noload_for(options, "materializations")
        assert has_noload_for(options, "metric_metadata")
        assert has_noload_for(options, "availability")
        assert has_noload_for(options, "dimension_links")
        assert has_noload_for(options, "required_dimensions")
        assert has_noload_for(options, "cube_elements")
        # Always noloaded
        assert has_noload_for(options, "created_by")
        assert has_noload_for(options, "node")
        assert has_noload_for(options, "missing_parents")

    def test_with_columns(self):
        """columns field requested - should selectinload with full relationships."""
        fields = {"columns": {"name": {}, "type": {}}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "columns")

    def test_with_primary_key(self):
        """primary_key field requested - should also load columns."""
        fields = {"primary_key": {}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "columns")

    def test_with_catalog(self):
        """catalog field requested - should joinedload catalog."""
        fields = {"catalog": {"name": {}}}
        options = load_node_revision_options(fields)

        assert has_joinedload_for(options, "catalog")

    def test_with_parents(self):
        """parents field requested - should selectinload parents."""
        fields = {"parents": {"name": {}}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "parents")

    def test_with_materializations(self):
        """materializations field requested - should selectinload."""
        fields = {"materializations": {}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "materializations")

    def test_with_metric_metadata(self):
        """metric_metadata field requested - should selectinload."""
        fields = {"metric_metadata": {}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "metric_metadata")

    def test_with_availability(self):
        """availability field requested - should selectinload."""
        fields = {"availability": {}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "availability")

    def test_with_dimension_links(self):
        """dimension_links field requested - should selectinload."""
        fields = {"dimension_links": {"dimension": {}}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "dimension_links")

    def test_with_required_dimensions(self):
        """required_dimensions field requested - should selectinload."""
        fields = {"required_dimensions": {}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "required_dimensions")

    def test_with_cube_metrics(self):
        """cube_metrics field requested - should load cube_elements with nested options."""
        fields = {"cube_metrics": {"name": {}, "displayName": {}}}
        options = load_node_revision_options(fields)

        # Should load cube_elements for cube_metrics
        assert has_selectinload_for(options, "cube_elements")
        # Should load minimal columns for cube request
        assert has_selectinload_for(options, "columns")

    def test_with_cube_dimensions(self):
        """cube_dimensions field requested - should load cube_elements with minimal options."""
        fields = {"cube_dimensions": {"name": {}, "type": {}}}
        options = load_node_revision_options(fields)

        # Should load cube_elements for cube_dimensions
        assert has_selectinload_for(options, "cube_elements")
        # Should load minimal columns for cube request
        assert has_selectinload_for(options, "columns")

    def test_with_cube_elements_direct(self):
        """cube_elements field requested directly - should selectinload."""
        fields = {"cube_elements": {}}
        options = load_node_revision_options(fields)

        assert has_selectinload_for(options, "cube_elements")

    def test_cube_request_loads_minimal_columns(self):
        """Cube requests should load columns (for matching cube elements)."""
        fields = {"cube_metrics": {"name": {}}}
        options = load_node_revision_options(fields)

        # Cube requests should selectinload columns with minimal fields
        assert has_selectinload_for(options, "columns")

    def test_query_ast_always_deferred(self):
        """query_ast should always be deferred."""
        fields = {"displayName": {}}
        options = load_node_revision_options(fields)

        # Check for defer on query_ast
        # defer uses ColumnProperty which is at path.path[1]
        has_defer = False
        for opt in options:
            if hasattr(opt, "context") and opt.context:
                ctx = opt.context[0]
                if hasattr(ctx, "path") and hasattr(ctx.path, "path"):
                    path_tuple = ctx.path.path
                    if len(path_tuple) >= 2 and hasattr(path_tuple[1], "key"):
                        if path_tuple[1].key == "query_ast":
                            has_defer = True
                            break
        assert has_defer


class TestBuildCubeMetricsNodeRevisionOptions:
    """Tests for build_cube_metrics_node_revision_options function."""

    def test_with_no_fields(self):
        """No fields requested - should noload all relationships."""
        options = build_cube_metrics_node_revision_options(None)

        assert has_noload_for(options, "columns")
        assert has_noload_for(options, "catalog")
        assert has_noload_for(options, "metric_metadata")
        # Always noloaded
        assert has_noload_for(options, "node")
        assert has_noload_for(options, "created_by")
        assert has_noload_for(options, "missing_parents")
        assert has_noload_for(options, "cube_elements")
        assert has_noload_for(options, "required_dimensions")
        assert has_noload_for(options, "parents")
        assert has_noload_for(options, "dimension_links")
        assert has_noload_for(options, "availability")
        assert has_noload_for(options, "materializations")

    def test_with_columns(self):
        """columns field requested - should selectinload with full relationships."""
        fields = {"columns": {"name": {}, "type": {}}}
        options = build_cube_metrics_node_revision_options(fields)

        assert has_selectinload_for(options, "columns")
        assert has_noload_for(options, "catalog")
        assert has_noload_for(options, "metric_metadata")

    def test_with_catalog(self):
        """catalog field requested - should joinedload catalog."""
        fields = {"catalog": {"name": {}}}
        options = build_cube_metrics_node_revision_options(fields)

        assert has_noload_for(options, "columns")
        assert has_joinedload_for(options, "catalog")
        assert has_noload_for(options, "metric_metadata")

    def test_with_metric_metadata(self):
        """metric_metadata field requested - should selectinload."""
        fields = {"metric_metadata": {}}
        options = build_cube_metrics_node_revision_options(fields)

        assert has_noload_for(options, "columns")
        assert has_noload_for(options, "catalog")
        assert has_selectinload_for(options, "metric_metadata")

    def test_with_all_fields(self):
        """All supported fields requested."""
        fields = {
            "columns": {"name": {}},
            "catalog": {"name": {}},
            "metric_metadata": {},
        }
        options = build_cube_metrics_node_revision_options(fields)

        assert has_selectinload_for(options, "columns")
        assert has_joinedload_for(options, "catalog")
        assert has_selectinload_for(options, "metric_metadata")

    def test_always_noloads_heavy_relationships(self):
        """Should always noload relationships not needed for cube_metrics."""
        fields = {"columns": {}, "catalog": {}, "metric_metadata": {}}
        options = build_cube_metrics_node_revision_options(fields)

        # These should always be noloaded regardless of what's requested
        assert has_noload_for(options, "node")
        assert has_noload_for(options, "created_by")
        assert has_noload_for(options, "missing_parents")
        assert has_noload_for(options, "cube_elements")
        assert has_noload_for(options, "required_dimensions")
        assert has_noload_for(options, "parents")
        assert has_noload_for(options, "dimension_links")
        assert has_noload_for(options, "availability")
        assert has_noload_for(options, "materializations")


class TestBuildCubeDimensionsNodeRevisionOptions:
    """Tests for build_cube_dimensions_node_revision_options function."""

    def test_returns_minimal_options(self):
        """Should return minimal options - only id, name, type."""
        options = build_cube_dimensions_node_revision_options()

        # Should have load_only for id, name, type
        has_load_only = any(
            isinstance(opt, type(load_only(DBNodeRevision.id))) for opt in options
        )
        assert has_load_only
        # The function uses load_only, check that it's in the options
        assert len(options) > 0

    def test_noloads_all_relationships(self):
        """Should noload all relationships."""
        options = build_cube_dimensions_node_revision_options()

        assert has_noload_for(options, "columns")
        assert has_noload_for(options, "catalog")
        assert has_noload_for(options, "metric_metadata")
        assert has_noload_for(options, "node")
        assert has_noload_for(options, "created_by")
        assert has_noload_for(options, "missing_parents")
        assert has_noload_for(options, "cube_elements")
        assert has_noload_for(options, "required_dimensions")
        assert has_noload_for(options, "parents")
        assert has_noload_for(options, "dimension_links")
        assert has_noload_for(options, "availability")
        assert has_noload_for(options, "materializations")


class TestLoadOptionsIntegration:
    """Integration tests for load options working together."""

    def test_nested_current_with_cube_metrics(self):
        """Test load_node_options with nested cube_metrics in current."""
        fields = {
            "name": {},
            "current": {
                "displayName": {},
                "cube_metrics": {"name": {}, "displayName": {}},
            },
        }
        options = load_node_options(fields)

        # Should load current with nested options
        assert has_joinedload_for(options, "current")
        # Should noload everything else at node level
        assert has_noload_for(options, "revisions")
        assert has_noload_for(options, "created_by")
        assert has_noload_for(options, "owners")
        assert has_noload_for(options, "tags")

    def test_full_cube_query_fields(self):
        """Test with fields matching a full cube query."""
        fields = {
            "name": {},
            "tags": {"name": {}},
            "created_by": {"username": {}},
            "current": {
                "description": {},
                "displayName": {},
                "cube_metrics": {
                    "name": {},
                    "version": {},
                    "type": {},
                    "displayName": {},
                    "updatedAt": {},
                    "id": {},
                },
                "cube_dimensions": {
                    "name": {},
                    "type": {},
                    "role": {},
                    "dimensionNode": {"name": {}},
                    "attribute": {},
                },
            },
        }
        options = load_node_options(fields)

        # Should load tags and created_by
        assert has_selectinload_for(options, "tags")
        assert has_selectinload_for(options, "created_by")
        # Should load current
        assert has_joinedload_for(options, "current")
        # Should noload unused relationships
        assert has_noload_for(options, "revisions")
        assert has_noload_for(options, "owners")
        assert has_noload_for(options, "history")

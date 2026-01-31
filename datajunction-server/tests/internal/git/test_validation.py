"""
Unit tests for git namespace validation functions.
"""

import pytest

from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.namespaces import (
    validate_sibling_relationship,
    validate_one_primary_branch_per_repo,
    detect_parent_cycle,
)


class TestValidateSiblingRelationship:
    """Unit tests for validate_sibling_relationship function."""

    def test_valid_siblings_same_prefix(self):
        """Test that siblings with the same prefix are allowed."""
        # Should not raise any exception
        validate_sibling_relationship("demo.feature", "demo.main")
        validate_sibling_relationship("team.hotfix", "team.production")
        validate_sibling_relationship("project.dev", "project.staging")

    def test_valid_siblings_deep_hierarchy(self):
        """Test that deep namespace hierarchies work correctly."""
        # Should not raise any exception
        validate_sibling_relationship(
            "company.division.team.feature",
            "company.division.team.main",
        )
        validate_sibling_relationship(
            "org.dept.project.branch_a",
            "org.dept.project.branch_b",
        )

    def test_valid_siblings_top_level_namespaces(self):
        """Test that top-level namespaces (no dots) can be siblings."""
        # Both have empty string as prefix
        validate_sibling_relationship("feature", "main")
        validate_sibling_relationship("dev", "prod")

    def test_invalid_siblings_different_single_segment_prefixes(self):
        """Test that namespaces with different single-segment prefixes are blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("team.feature", "demo.main")

        error_message = str(exc_info.value.message)
        assert "team.feature" in error_message
        assert "demo.main" in error_message
        assert "prefix: 'team'" in error_message
        assert "prefix: 'demo'" in error_message
        assert "Branch namespaces must be siblings" in error_message

    def test_invalid_siblings_nested_vs_flat(self):
        """Test that nested namespace cannot have flat namespace as parent."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("demo.prod.feature", "demo.main")

        error_message = str(exc_info.value.message)
        assert "demo.prod.feature" in error_message
        assert "demo.main" in error_message
        assert "prefix: 'demo.prod'" in error_message
        assert "prefix: 'demo'" in error_message

    def test_invalid_siblings_top_level_vs_nested(self):
        """Test that top-level namespace cannot have nested namespace as parent."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("feature", "demo.main")

        error_message = str(exc_info.value.message)
        assert "feature" in error_message
        assert "demo.main" in error_message
        # feature has empty prefix ""
        assert "prefix: ''" in error_message or 'prefix: ""' in error_message
        assert "prefix: 'demo'" in error_message

    def test_invalid_siblings_cross_project(self):
        """Test that namespaces from different projects cannot be related."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("analytics.feature", "demo.main")

        error_message = str(exc_info.value.message)
        assert "analytics.feature" in error_message
        assert "demo.main" in error_message
        assert "prefix: 'analytics'" in error_message
        assert "prefix: 'demo'" in error_message

    def test_invalid_siblings_different_deep_hierarchies(self):
        """Test that namespaces with different deep hierarchy prefixes are blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship(
                "org.team_a.project.feature",
                "org.team_b.project.main",
            )

        error_message = str(exc_info.value.message)
        assert "org.team_a.project" in error_message
        assert "org.team_b.project" in error_message

    def test_multi_level_hierarchy_all_same_prefix(self):
        """Test that multi-level git hierarchies work (main -> dev -> feature)."""
        # All have the same prefix "flow"
        validate_sibling_relationship("flow.dev", "flow.main")
        validate_sibling_relationship("flow.feature", "flow.dev")
        # Even though feature's ultimate parent is main, we only check immediate parent
        validate_sibling_relationship("flow.feature", "flow.main")

    def test_edge_case_single_character_namespaces(self):
        """Test edge case with single-character namespace segments."""
        validate_sibling_relationship("a.b", "a.c")

        with pytest.raises(DJInvalidInputException):
            validate_sibling_relationship("a.b", "x.y")

    def test_edge_case_numbers_in_namespaces(self):
        """Test that namespaces with numbers work correctly."""
        validate_sibling_relationship("project2.feature", "project2.main")

        with pytest.raises(DJInvalidInputException):
            validate_sibling_relationship("project1.feature", "project2.main")

    def test_edge_case_underscores_and_hyphens(self):
        """Test that namespaces with special characters work correctly."""
        validate_sibling_relationship("my_project.feature_1", "my_project.main_branch")

        with pytest.raises(DJInvalidInputException):
            validate_sibling_relationship("my_project.feature", "other_project.main")

    def test_prefix_extraction_logic(self):
        """Test the prefix extraction logic with various namespace formats."""
        # These should all pass because they have the same prefix
        validate_sibling_relationship("x.y", "x.z")  # prefix: "x"
        validate_sibling_relationship("a.b.c", "a.b.d")  # prefix: "a.b"
        validate_sibling_relationship(
            "one.two.three.four",
            "one.two.three.five",
        )  # prefix: "one.two.three"

    def test_error_message_includes_expected_prefix(self):
        """Test that error message includes helpful information about expected prefix."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("analytics.feature", "demo.main")

        error_message = str(exc_info.value.message)
        # Should tell user what prefix was expected
        assert "Expected parent to have prefix 'analytics'" in error_message


class TestValidateOnePrimaryBranchPerRepo:
    """Unit tests for validate_one_primary_branch_per_repo function."""

    @pytest.fixture
    async def session_with_namespaces(self, session: AsyncSession):
        """Create some test namespaces in the database."""
        # Create a root namespace
        root_ns = NodeNamespace(
            namespace="project.main",
            github_repo_path="corp/project",
            git_branch="main",
            parent_namespace=None,
        )
        session.add(root_ns)

        # Create a branch namespace
        branch_ns = NodeNamespace(
            namespace="project.feature",
            github_repo_path="corp/project",
            git_branch="feature",
            parent_namespace="project.main",
        )
        session.add(branch_ns)

        # Create another root in different repo
        other_root = NodeNamespace(
            namespace="analytics.main",
            github_repo_path="corp/analytics",
            git_branch="main",
            parent_namespace=None,
        )
        session.add(other_root)

        await session.commit()
        return session

    @pytest.mark.asyncio
    async def test_first_root_namespace_allowed(self, session: AsyncSession):
        """Test that the first root namespace in a repo is allowed."""
        # No existing namespaces, so this should pass
        await validate_one_primary_branch_per_repo(
            session,
            namespace="demo.main",
            github_repo_path="corp/demo",
            parent_namespace=None,
        )

    @pytest.mark.asyncio
    async def test_second_root_namespace_blocked(
        self,
        session_with_namespaces: AsyncSession,
    ):
        """Test that a second root namespace in the same repo is blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            await validate_one_primary_branch_per_repo(
                session_with_namespaces,
                namespace="project.production",
                github_repo_path="corp/project",  # Same repo as project.main
                parent_namespace=None,
            )

        error_message = str(exc_info.value.message)
        assert error_message == (
            "Repository 'corp/project' already has primary branch namespace(s): project.main. "
            "A repository should have only one primary branch. Set parent_namespace to create "
            "a branch relationship, or use a different repository."
        )

    @pytest.mark.asyncio
    async def test_branch_namespace_with_parent_allowed(
        self,
        session_with_namespaces: AsyncSession,
    ):
        """Test that branch namespaces (with parent) are allowed."""
        # Has parent_namespace, so validation should skip
        await validate_one_primary_branch_per_repo(
            session_with_namespaces,
            namespace="project.hotfix",
            github_repo_path="corp/project",
            parent_namespace="project.main",  # Has parent
        )

    @pytest.mark.asyncio
    async def test_multiple_roots_in_different_repos_allowed(
        self,
        session_with_namespaces: AsyncSession,
    ):
        """Test that different repos can each have their own root."""
        # project.main already exists in corp/project
        # analytics.main already exists in corp/analytics
        # So a new root in corp/demo should be fine
        await validate_one_primary_branch_per_repo(
            session_with_namespaces,
            namespace="demo.main",
            github_repo_path="corp/demo",  # Different repo
            parent_namespace=None,
        )

    @pytest.mark.asyncio
    async def test_updating_existing_root_namespace_allowed(
        self,
        session_with_namespaces: AsyncSession,
    ):
        """Test that updating an existing root namespace is allowed."""
        # project.main already exists as root in corp/project
        # Updating project.main itself should be allowed (it's excluded from check)
        await validate_one_primary_branch_per_repo(
            session_with_namespaces,
            namespace="project.main",  # Same namespace
            github_repo_path="corp/project",  # Same repo
            parent_namespace=None,
        )

    @pytest.mark.asyncio
    async def test_changing_to_repo_with_existing_root_blocked(
        self,
        session_with_namespaces: AsyncSession,
    ):
        """Test that changing to a repo that already has a root is blocked."""
        # analytics.main already exists as root in corp/analytics
        # Trying to make demo.main also a root in corp/analytics should fail
        with pytest.raises(DJInvalidInputException) as exc_info:
            await validate_one_primary_branch_per_repo(
                session_with_namespaces,
                namespace="demo.main",
                github_repo_path="corp/analytics",  # Repo with existing root
                parent_namespace=None,
            )

        error_message = str(exc_info.value.message)
        assert error_message == (
            "Repository 'corp/analytics' already has primary branch namespace(s): analytics.main. "
            "A repository should have only one primary branch. Set parent_namespace to create a "
            "branch relationship, or use a different repository."
        )

    @pytest.mark.asyncio
    async def test_null_repo_path_skips_validation(
        self,
        session_with_namespaces: AsyncSession,
    ):
        """Test that validation is skipped when repo_path is None."""
        # When repo_path is None, validation shouldn't check for conflicts
        # (though this is an edge case - you probably shouldn't call this with None)
        await validate_one_primary_branch_per_repo(
            session_with_namespaces,
            namespace="orphan.main",
            github_repo_path=None,  # type: ignore
            parent_namespace=None,
        )


class TestDetectParentCycle:
    """Unit tests for detect_parent_cycle function."""

    @pytest.fixture
    async def session_with_linear_chain(self, session: AsyncSession):
        """Create a linear chain: feature -> dev -> main -> null."""
        main = NodeNamespace(
            namespace="chain.main",
            github_repo_path="corp/chain",
            git_branch="main",
            parent_namespace=None,
        )
        session.add(main)

        dev = NodeNamespace(
            namespace="chain.dev",
            github_repo_path="corp/chain",
            git_branch="dev",
            parent_namespace="chain.main",
        )
        session.add(dev)

        feature = NodeNamespace(
            namespace="chain.feature",
            github_repo_path="corp/chain",
            git_branch="feature",
            parent_namespace="chain.dev",
        )
        session.add(feature)

        await session.commit()
        return session

    @pytest.fixture
    async def session_with_two_node_cycle(self, session: AsyncSession):
        """Create a 2-node cycle: A -> B, and we'll try to set B -> A."""
        ns_b = NodeNamespace(
            namespace="cycle.b",
            github_repo_path="corp/cycle",
            git_branch="b",
            parent_namespace=None,  # Will try to set this to cycle.a
        )
        session.add(ns_b)

        ns_a = NodeNamespace(
            namespace="cycle.a",
            github_repo_path="corp/cycle",
            git_branch="a",
            parent_namespace="cycle.b",
        )
        session.add(ns_a)

        await session.commit()
        return session

    @pytest.fixture
    async def session_with_three_node_chain(self, session: AsyncSession):
        """Create a 3-node chain: A -> B -> C, we'll try to set C -> A."""
        ns_c = NodeNamespace(
            namespace="cycle3.c",
            github_repo_path="corp/cycle3",
            git_branch="c",
            parent_namespace=None,  # Will try to set this to cycle3.a
        )
        session.add(ns_c)

        ns_b = NodeNamespace(
            namespace="cycle3.b",
            github_repo_path="corp/cycle3",
            git_branch="b",
            parent_namespace="cycle3.c",
        )
        session.add(ns_b)

        ns_a = NodeNamespace(
            namespace="cycle3.a",
            github_repo_path="corp/cycle3",
            git_branch="a",
            parent_namespace="cycle3.b",
        )
        session.add(ns_a)

        await session.commit()
        return session

    @pytest.mark.asyncio
    async def test_no_cycle_linear_chain(
        self,
        session_with_linear_chain: AsyncSession,
    ):
        """Test that a valid linear chain is allowed."""
        # chain.feature -> chain.dev -> chain.main -> null
        # Adding a new namespace that points to chain.feature should be fine
        await detect_parent_cycle(
            session_with_linear_chain,
            child_namespace="chain.hotfix",
            new_parent="chain.feature",
        )

    @pytest.mark.asyncio
    async def test_no_cycle_no_parent(self, session: AsyncSession):
        """Test that setting a parent on a namespace with no existing chain is allowed."""
        # Create a simple parent
        parent = NodeNamespace(
            namespace="simple.main",
            github_repo_path="corp/simple",
            git_branch="main",
            parent_namespace=None,
        )
        session.add(parent)
        await session.commit()

        # Setting a child to point to this parent should be fine
        await detect_parent_cycle(
            session,
            child_namespace="simple.feature",
            new_parent="simple.main",
        )

    @pytest.mark.asyncio
    async def test_two_node_cycle_detected(
        self,
        session_with_two_node_cycle: AsyncSession,
    ):
        """Test that a 2-node cycle is detected: A -> B -> A."""
        # cycle.a -> cycle.b (already set)
        # Now trying to set cycle.b -> cycle.a creates a cycle
        with pytest.raises(DJInvalidInputException) as exc_info:
            await detect_parent_cycle(
                session_with_two_node_cycle,
                child_namespace="cycle.b",
                new_parent="cycle.a",
            )

        error_message = str(exc_info.value.message)
        assert "Circular parent reference detected" in error_message
        assert "cycle.b" in error_message
        assert "cycle.a" in error_message

    @pytest.mark.asyncio
    async def test_three_node_cycle_detected(
        self,
        session_with_three_node_chain: AsyncSession,
    ):
        """Test that a 3-node cycle is detected: A -> B -> C -> A."""
        # cycle3.a -> cycle3.b -> cycle3.c (already set)
        # Now trying to set cycle3.c -> cycle3.a creates a cycle
        with pytest.raises(DJInvalidInputException) as exc_info:
            await detect_parent_cycle(
                session_with_three_node_chain,
                child_namespace="cycle3.c",
                new_parent="cycle3.a",
            )

        error_message = str(exc_info.value.message)
        assert "Circular parent reference detected" in error_message

    @pytest.mark.asyncio
    async def test_four_node_cycle_detected(self, session: AsyncSession):
        """Test that a 4-node cycle is detected: A -> B -> C -> D -> A."""
        # Create chain: A -> B -> C -> D
        ns_a = NodeNamespace(
            namespace="cycle4.a",
            github_repo_path="corp/cycle4",
            git_branch="a",
            parent_namespace="cycle4.b",
        )
        ns_b = NodeNamespace(
            namespace="cycle4.b",
            github_repo_path="corp/cycle4",
            git_branch="b",
            parent_namespace="cycle4.c",
        )
        ns_c = NodeNamespace(
            namespace="cycle4.c",
            github_repo_path="corp/cycle4",
            git_branch="c",
            parent_namespace="cycle4.d",
        )
        ns_d = NodeNamespace(
            namespace="cycle4.d",
            github_repo_path="corp/cycle4",
            git_branch="d",
            parent_namespace=None,
        )
        session.add_all([ns_d, ns_c, ns_b, ns_a])
        await session.commit()

        # Try to set D -> A, creating a 4-node cycle
        with pytest.raises(DJInvalidInputException) as exc_info:
            await detect_parent_cycle(
                session,
                child_namespace="cycle4.d",
                new_parent="cycle4.a",
            )

        error_message = str(exc_info.value.message)
        assert "Circular parent reference detected" in error_message

    @pytest.mark.asyncio
    async def test_self_reference_detected(self, session: AsyncSession):
        """Test that a self-reference is detected: A -> A."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            await detect_parent_cycle(
                session,
                child_namespace="self.ref",
                new_parent="self.ref",
            )

        error_message = str(exc_info.value.message)
        assert "Circular parent reference detected" in error_message
        assert "self.ref" in error_message

    @pytest.mark.asyncio
    async def test_deep_chain_without_cycle_allowed(self, session: AsyncSession):
        """Test that a deep valid chain (20 levels) doesn't hit max_depth."""
        # Create a 20-level deep chain
        parent = None
        for i in range(20):
            ns = NodeNamespace(
                namespace=f"deep.level{i}",
                github_repo_path="corp/deep",
                git_branch=f"level{i}",
                parent_namespace=parent,
            )
            session.add(ns)
            parent = f"deep.level{i}"

        await session.commit()

        # Adding level20 -> level19 should be fine (no cycle)
        await detect_parent_cycle(
            session,
            child_namespace="deep.level20",
            new_parent="deep.level19",
        )

    @pytest.mark.asyncio
    async def test_max_depth_exceeded(self, session: AsyncSession):
        """Test that exceeding max_depth (50) raises an error."""
        # Create a 51-level deep chain
        parent = None
        for i in range(51):
            ns = NodeNamespace(
                namespace=f"verydeep.level{i}",
                github_repo_path="corp/verydeep",
                git_branch=f"level{i}",
                parent_namespace=parent,
            )
            session.add(ns)
            parent = f"verydeep.level{i}"

        await session.commit()

        # Trying to add another level should fail due to max_depth
        with pytest.raises(DJInvalidInputException) as exc_info:
            await detect_parent_cycle(
                session,
                child_namespace="verydeep.level51",
                new_parent="verydeep.level50",
            )

        error_message = str(exc_info.value.message)
        assert "exceeds maximum depth" in error_message
        assert "50" in error_message

    @pytest.mark.asyncio
    async def test_cycle_in_middle_of_chain(self, session: AsyncSession):
        """Test that a cycle in the middle of a chain is detected."""
        # Create: A -> B -> C -> B (cycle in middle)
        ns_a = NodeNamespace(
            namespace="mid.a",
            github_repo_path="corp/mid",
            git_branch="a",
            parent_namespace="mid.b",
        )
        ns_b = NodeNamespace(
            namespace="mid.b",
            github_repo_path="corp/mid",
            git_branch="b",
            parent_namespace="mid.c",
        )
        ns_c = NodeNamespace(
            namespace="mid.c",
            github_repo_path="corp/mid",
            git_branch="c",
            parent_namespace=None,
        )
        session.add_all([ns_c, ns_b, ns_a])
        await session.commit()

        # Try to set C -> B, creating a cycle B -> C -> B
        with pytest.raises(DJInvalidInputException) as exc_info:
            await detect_parent_cycle(
                session,
                child_namespace="mid.c",
                new_parent="mid.b",
            )

        error_message = str(exc_info.value.message)
        assert "Circular parent reference detected" in error_message

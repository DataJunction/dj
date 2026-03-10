"""
Unit tests for git namespace validation functions.
"""

import pytest

from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.namespaces import (
    detect_parent_cycle,
    get_git_info_for_namespace,
    validate_git_path,
    validate_sibling_relationship,
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
        assert error_message == (
            "Namespace 'team.feature' (prefix: 'team') cannot have parent 'demo.main'. "
            "Expected parent to either be 'team' (direct parent) or have prefix 'team' (sibling)."
        )

    def test_invalid_siblings_nested_vs_flat(self):
        """Test that nested namespace cannot have flat namespace as parent."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("demo.prod.feature", "demo.main")

        error_message = str(exc_info.value.message)
        assert error_message == (
            "Namespace 'demo.prod.feature' (prefix: 'demo.prod') cannot have parent 'demo.main'."
            " Expected parent to either be 'demo.prod' (direct parent) or have prefix "
            "'demo.prod' (sibling)."
        )

    def test_invalid_siblings_top_level_vs_nested(self):
        """Test that top-level namespace cannot have nested namespace as parent."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("feature", "demo.main")

        error_message = str(exc_info.value.message)
        assert error_message == (
            "Namespace 'feature' (prefix: '') cannot have parent 'demo.main'. "
            "Expected parent to either be '' (direct parent) or have prefix '' (sibling)."
        )

    def test_invalid_siblings_cross_project(self):
        """Test that namespaces from different projects cannot be related."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_sibling_relationship("analytics.feature", "demo.main")

        error_message = str(exc_info.value.message)
        assert error_message == (
            "Namespace 'analytics.feature' (prefix: 'analytics') cannot have "
            "parent 'demo.main'. Expected parent to either be 'analytics' (direct parent) or have "
            "prefix 'analytics' (sibling)."
        )

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
        assert error_message == (
            "Namespace 'analytics.feature' (prefix: 'analytics') cannot have parent 'demo.main'. "
            "Expected parent to either be 'analytics' (direct parent) or have prefix 'analytics'"
            " (sibling)."
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


class TestValidateGitPath:
    """Unit tests for validate_git_path function."""

    def test_valid_relative_paths(self):
        """Test that valid relative paths are allowed."""
        # These should all pass without raising
        validate_git_path("nodes/")
        validate_git_path("definitions/")
        validate_git_path("definitions/metrics/")
        validate_git_path("src/nodes/")
        validate_git_path("a/b/c/d/")
        validate_git_path("definitions")  # Without trailing slash
        validate_git_path("my-nodes/")  # With hyphen
        validate_git_path("my_nodes/")  # With underscore

    def test_null_path_allowed(self):
        """Test that None/null path is allowed (validation skipped)."""
        validate_git_path(None)

    def test_empty_path_allowed(self):
        """Test that empty string is allowed (validation skipped)."""
        validate_git_path("")
        validate_git_path("   ")  # Just whitespace strips to empty

    def test_path_with_parent_directory_blocked(self):
        """Test that paths with .. are blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_git_path("../other-repo/")

        error_message = str(exc_info.value.message)
        assert "path traversal" in error_message.lower()

    def test_path_with_parent_directory_in_middle_blocked(self):
        """Test that .. in the middle of path is blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_git_path("nodes/../other/")

        error_message = str(exc_info.value.message)
        assert "path traversal" in error_message.lower()

    def test_path_with_parent_directory_at_end_blocked(self):
        """Test that path ending with .. is blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_git_path("nodes/..")

        error_message = str(exc_info.value.message)
        assert "path traversal" in error_message.lower()

    def test_absolute_path_blocked(self):
        """Test that absolute paths are blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_git_path("/etc/passwd")

        error_message = str(exc_info.value.message)
        assert "must be a relative path" in error_message
        assert "cannot start with '/'" in error_message

    def test_absolute_path_with_slash_blocked(self):
        """Test that paths starting with / are blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_git_path("/usr/local/")

        error_message = str(exc_info.value.message)
        assert "relative path" in error_message

    def test_network_path_blocked(self):
        """Test that network-style paths are blocked."""
        with pytest.raises(DJInvalidInputException) as exc_info:
            validate_git_path("//network/share")

        error_message = str(exc_info.value.message)
        assert "relative path" in error_message

    def test_path_with_spaces(self):
        """Test that paths with spaces are allowed (but trimmed)."""
        # Leading/trailing spaces should be trimmed
        validate_git_path("  nodes/  ")

    def test_path_with_dots_in_name(self):
        """Test that dots in directory names (not ..) are allowed."""
        validate_git_path("my.nodes/")
        validate_git_path("nodes.v2/")
        validate_git_path("definitions/v1.0/")

    def test_path_with_multiple_slashes(self):
        """Test paths with multiple slashes."""
        # Multiple slashes should be fine (not our job to normalize)
        validate_git_path("nodes//subdirectory/")

    def test_windows_style_path_blocked(self):
        """Test that Windows-style absolute paths are blocked."""
        # This might start with a letter, but if it starts with /, it's blocked
        # Actually, "C:/..." doesn't start with /, so this would pass
        # Only paths starting with / are blocked
        validate_git_path("C:/nodes/")  # Would pass (doesn't start with /)

        # But if someone tries /C:/, that's blocked
        with pytest.raises(DJInvalidInputException):
            validate_git_path("/C:/nodes/")

    def test_hidden_directory_with_dot_prefix(self):
        """Test that hidden directories (.git, .hidden) are allowed."""
        # These are valid relative paths, just with dot prefix
        validate_git_path(".hidden/")
        validate_git_path(".config/")

    def test_complex_parent_directory_patterns_blocked(self):
        """Test various complex patterns with .. are all blocked."""
        with pytest.raises(DJInvalidInputException):
            validate_git_path("../../etc/")

        with pytest.raises(DJInvalidInputException):
            validate_git_path("a/b/../../c/")

        with pytest.raises(DJInvalidInputException):
            validate_git_path("nodes/../../etc/passwd")


class TestGetGitInfoForNamespace:
    """Tests for get_git_info_for_namespace."""

    # ------------------------------------------------------------------
    # Basic resolution
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_namespace_with_repo_and_branch_directly(self, session: AsyncSession):
        """Namespace has both github_repo_path and git_branch set directly."""
        session.add(
            NodeNamespace(
                namespace="proj.main",
                github_repo_path="org/repo",
                git_branch="main",
                default_branch="main",
                git_path="defs/",
                git_only=False,
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.main")

        assert result is not None
        assert result["repo"] == "org/repo"
        assert result["branch"] == "main"
        assert result["default_branch"] == "main"
        assert result["path"] == "defs/"
        assert result["is_default_branch"] is True
        assert result["git_only"] is False

    @pytest.mark.asyncio
    async def test_no_git_config_anywhere(self, session: AsyncSession):
        """Namespace exists but has no git config in any ancestor."""
        session.add(NodeNamespace(namespace="plain"))
        session.add(NodeNamespace(namespace="plain.sub"))
        await session.commit()

        result = await get_git_info_for_namespace(session, "plain.sub")

        assert result is None

    @pytest.mark.asyncio
    async def test_namespace_not_in_db(self, session: AsyncSession):
        """Namespace doesn't exist in the DB at all."""
        result = await get_git_info_for_namespace(session, "nonexistent.ns")

        assert result is None

    # ------------------------------------------------------------------
    # Branch walking
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_branch_ns_has_git_branch_config_ns_has_repo(
        self,
        session: AsyncSession,
    ):
        """projectx has github_repo_path; projectx.feature_one has git_branch only."""
        session.add(
            NodeNamespace(
                namespace="projectx",
                github_repo_path="org/repo",
                default_branch="main",
                git_path="defs/",
                git_only=False,
            ),
        )
        session.add(
            NodeNamespace(
                namespace="projectx.feature_one",
                git_branch="feature_one",
                parent_namespace="projectx",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "projectx.feature_one")

        assert result is not None
        assert result["repo"] == "org/repo"
        assert result["branch"] == "feature_one"
        assert result["default_branch"] == "main"
        assert result["path"] == "defs/"
        assert result["is_default_branch"] is False
        assert result["parent_namespace"] == "projectx"

    @pytest.mark.asyncio
    async def test_sub_namespace_inherits_branch_from_parent(
        self,
        session: AsyncSession,
    ):
        """projectx.feature_one.cubes has nothing; branch comes from projectx.feature_one."""
        session.add(
            NodeNamespace(
                namespace="projectx",
                github_repo_path="org/repo",
                default_branch="main",
                git_path="defs/",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="projectx.feature_one",
                git_branch="feature_one",
                parent_namespace="projectx",
            ),
        )
        session.add(NodeNamespace(namespace="projectx.feature_one.cubes"))
        await session.commit()

        result = await get_git_info_for_namespace(session, "projectx.feature_one.cubes")

        assert result is not None
        assert result["branch"] == "feature_one"
        assert result["repo"] == "org/repo"

    @pytest.mark.asyncio
    async def test_deep_sub_namespace(self, session: AsyncSession):
        """projectx.feature_one.cubes.x.y — several levels above the branch namespace."""
        session.add(
            NodeNamespace(
                namespace="projectx",
                github_repo_path="org/repo",
                default_branch="main",
                git_path="defs/",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="projectx.feature_one",
                git_branch="feature_one",
                parent_namespace="projectx",
            ),
        )
        session.add(NodeNamespace(namespace="projectx.feature_one.cubes"))
        session.add(NodeNamespace(namespace="projectx.feature_one.cubes.x"))
        session.add(NodeNamespace(namespace="projectx.feature_one.cubes.x.y"))
        await session.commit()

        result = await get_git_info_for_namespace(
            session,
            "projectx.feature_one.cubes.x.y",
        )

        assert result is not None
        assert result["branch"] == "feature_one"
        assert result["repo"] == "org/repo"

    # ------------------------------------------------------------------
    # is_default_branch
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_is_default_branch_true_when_matching(self, session: AsyncSession):
        """git_branch == default_branch → is_default_branch is True."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.main",
                git_branch="main",
                parent_namespace="proj",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.main")

        assert result is not None
        assert result["is_default_branch"] is True

    @pytest.mark.asyncio
    async def test_is_default_branch_false_when_not_matching(
        self,
        session: AsyncSession,
    ):
        """git_branch != default_branch → is_default_branch is False."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.feature_x",
                git_branch="feature_x",
                parent_namespace="proj",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.feature_x")

        assert result is not None
        assert result["is_default_branch"] is False

    @pytest.mark.asyncio
    async def test_is_default_branch_true_when_no_git_branch(
        self,
        session: AsyncSession,
    ):
        """Root namespace with only github_repo_path (no git_branch) → defaults True."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj")

        assert result is not None
        assert result["branch"] is None
        assert result["is_default_branch"] is True

    @pytest.mark.asyncio
    async def test_is_default_branch_true_when_no_default_branch(
        self,
        session: AsyncSession,
    ):
        """git_branch set but default_branch is None → defaults to True."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch=None,
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.feature_x",
                git_branch="feature_x",
                parent_namespace="proj",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.feature_x")

        assert result is not None
        assert result["is_default_branch"] is True

    # ------------------------------------------------------------------
    # parent_namespace
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_parent_namespace_from_branch_ns_not_leaf(
        self,
        session: AsyncSession,
    ):
        """parent_namespace comes from branch_ns, not the leaf sub-namespace."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.feature_x",
                git_branch="feature_x",
                parent_namespace="proj",
            ),
        )
        session.add(NodeNamespace(namespace="proj.feature_x.cubes"))
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.feature_x.cubes")

        assert result is not None
        assert result["parent_namespace"] == "proj"

    @pytest.mark.asyncio
    async def test_parent_namespace_none_when_no_branch_ns(self, session: AsyncSession):
        """When there's no branch_ns (root is the config), parent_namespace is None."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
                parent_namespace=None,
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj")

        assert result is not None
        assert result["parent_namespace"] is None

    # ------------------------------------------------------------------
    # Nearest wins
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_most_specific_config_ns_wins(self, session: AsyncSession):
        """When multiple ancestors have github_repo_path, most specific wins."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/root-repo",
                default_branch="main",
                git_path="root/",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.team",
                github_repo_path="org/team-repo",
                default_branch="main",
                git_path="team/",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.team.feature_x",
                git_branch="feature_x",
                parent_namespace="proj.team",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.team.feature_x")

        assert result is not None
        assert result["repo"] == "org/team-repo"
        assert result["path"] == "team/"

    @pytest.mark.asyncio
    async def test_most_specific_branch_ns_wins(self, session: AsyncSession):
        """When multiple ancestors have git_branch, most specific wins."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.feature_x",
                git_branch="feature_x",
                parent_namespace="proj",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.feature_x.sub",
                git_branch="sub_branch",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.feature_x.sub")

        assert result is not None
        assert result["branch"] == "sub_branch"

    # ------------------------------------------------------------------
    # git_only and path
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_git_only_returned_from_config_ns(self, session: AsyncSession):
        """git_only flag is taken from config namespace (the one with github_repo_path)."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
                git_only=True,
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.feature_x",
                git_branch="feature_x",
                parent_namespace="proj",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.feature_x")

        assert result is not None
        assert result["git_only"] is True

    @pytest.mark.asyncio
    async def test_git_path_returned_from_config_ns(self, session: AsyncSession):
        """git_path is taken from config namespace."""
        session.add(
            NodeNamespace(
                namespace="proj",
                github_repo_path="org/repo",
                default_branch="main",
                git_path="definitions/metrics/",
            ),
        )
        session.add(
            NodeNamespace(
                namespace="proj.main",
                git_branch="main",
                parent_namespace="proj",
            ),
        )
        await session.commit()

        result = await get_git_info_for_namespace(session, "proj.main")

        assert result is not None
        assert result["path"] == "definitions/metrics/"

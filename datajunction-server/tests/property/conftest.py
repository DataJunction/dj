"""
Hypothesis profiles for the property tests, chosen with `DJ_PBT_PROFILE`:

- dev (default locally): random examples; failures are saved and replayed.
- ci (default when `CI` is set): a fixed sequence of examples and no shrinking,
  so runs are repeatable and a failure is reported quickly.
- nightly: ten times the examples, random, with shrinking.
"""

from hypothesis import HealthCheck, Phase, settings

from tests.property.budget import profile

COMMON = {
    # Examples that create nodes through the API take well over the default
    # 200ms, and vary with load.
    "deadline": None,
    "suppress_health_check": [
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
    "print_blob": True,
}

settings.register_profile("dev", **COMMON)
settings.register_profile(
    "ci",
    derandomize=True,
    database=None,
    phases=[Phase.explicit, Phase.reuse, Phase.generate],
    **COMMON,
)
settings.register_profile("nightly", **COMMON)
settings.load_profile(profile())

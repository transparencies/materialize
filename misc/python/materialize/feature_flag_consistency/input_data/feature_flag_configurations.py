# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.feature_flag_consistency.feature_flag.feature_flag import (
    FeatureFlagSystemConfigurationPair,
    create_boolean_feature_flag_configuration_pair,
)

FEATURE_FLAG_CONFIGURATION_PAIRS = dict()


def append_config(config_pair: FeatureFlagSystemConfigurationPair) -> None:
    FEATURE_FLAG_CONFIGURATION_PAIRS[config_pair.name] = config_pair


# Test enable_equivalence_propagation enabled.
# Assessment: "definitely good to include because it enables a super-complicated transform that does many things"
append_config(
    create_boolean_feature_flag_configuration_pair(
        "enable_equivalence_propagation", "equiv_prop"
    )
)

# Test enable_letrec_fixpoint_analysis enabled.
# Assessment: "only affects queries with WMR, in theory. But the code around it is complex, and there is a non-0 chance that even non-WMR code is affected if there is a bug in it"
append_config(
    create_boolean_feature_flag_configuration_pair(
        "enable_letrec_fixpoint_analysis", "fp_analysis"
    )
)

# TODO: #19825 (output consistency: support joins): configure
# * enable_outer_join_null_filter
# * enable_variadic_left_join_lowering
# * enable_eager_delta_joins
# * enable_variadic_left_join_lowering && enable_eager_delta_joins

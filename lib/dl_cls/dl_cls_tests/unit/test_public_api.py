import dl_cls
from dl_cls.testing.testing_data import make_multitier_field_cls
from dl_constants.enums import CLSMode


def test_multitier_fixture_selection() -> None:
    field_cls = make_multitier_field_cls()

    # Concrete trusted user -> unmasked, even while in no group.
    trusted = dl_cls.select_effective_rule(field_cls, user_id="trusted_user", allowed_groups=set())
    assert trusted.mode == CLSMode.none

    # Group member -> the group's partial rule.
    grouped = dl_cls.select_effective_rule(field_cls, user_id="someone", allowed_groups={"partial_group"})
    assert grouped.mode == CLSMode.partial

    # Unknown subject with no groups -> the default rule (full); there is no `all` tier.
    unknown = dl_cls.select_effective_rule(field_cls, user_id="nobody", allowed_groups=set())
    assert unknown.mode == CLSMode.full

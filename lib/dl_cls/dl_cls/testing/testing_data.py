from dl_cls.models import (
    CLSMaskSpec,
    CLSRule,
    CLSSubject,
    FieldCLS,
)
from dl_constants.enums import (
    CLSMode,
    CLSSubjectType,
)


def make_multitier_field_cls() -> FieldCLS:
    """A canonical CLS config exercising every selection tier (user, group, default rule)."""
    return FieldCLS(
        default_rule=CLSMaskSpec(mode=CLSMode.full),
        rules=(
            CLSRule(
                subject=CLSSubject(subject_type=CLSSubjectType.user, subject_id="trusted_user"),
                spec=CLSMaskSpec(mode=CLSMode.none),
            ),
            CLSRule(
                subject=CLSSubject(subject_type=CLSSubjectType.group, subject_id="partial_group"),
                spec=CLSMaskSpec(mode=CLSMode.partial, prefix=2, suffix=2, mask="*"),
            ),
        ),
    )

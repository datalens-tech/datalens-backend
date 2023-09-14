from bi_testing.regulated_test import (
    Feature,
    RegulatedTestCase,
    RegulatedTestParams,
    for_features,
    regulated_test_case,
)


class BaseForTestRTClass(RegulatedTestCase):
    my_feature = Feature("my_feature")

    def test_skipped_test(self) -> None:
        raise NotImplementedError

    def test_failed_test(self) -> None:
        raise AssertionError()

    @for_features(my_feature)
    def test_skipped_test_with_feature(self) -> None:
        raise NotImplementedError

    def test_normal_test(self) -> None:
        pass


class TestRTClass(BaseForTestRTClass):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseForTestRTClass.test_skipped_test: "Skip it",
        },
        mark_tests_failed={
            BaseForTestRTClass.test_failed_test: "It fails",
        },
        mark_features_skipped={BaseForTestRTClass.my_feature: "Feature not implemented"},
    )


class BaseForRTDecorator:
    my_feature = Feature("my_feature")

    def test_skipped_test(self) -> None:
        raise NotImplementedError

    def test_failed_test(self) -> None:
        raise AssertionError()

    @for_features(my_feature)
    def test_skipped_test_with_feature(self) -> None:
        raise NotImplementedError

    def test_normal_test(self) -> None:
        pass


@regulated_test_case(
    test_params=RegulatedTestParams(
        mark_tests_skipped={
            BaseForRTDecorator.test_skipped_test: "Skip it",
        },
        mark_tests_failed={
            BaseForRTDecorator.test_failed_test: "It fails",
        },
        mark_features_skipped={BaseForRTDecorator.my_feature: "Feature not implemented"},
    ),
)
class TestRTDecorator(BaseForRTDecorator):
    pass

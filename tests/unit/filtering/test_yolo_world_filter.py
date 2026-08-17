from app.filtering.yolo_world_filter import derive_prompt_classes


class TestDerivePromptClasses:
    def test_splits_on_with(self):
        classes = derive_prompt_classes("people with red bag")
        assert "people with red bag" in classes
        assert "people" in classes
        assert "red bag" in classes

    def test_full_query_is_first_and_most_specific(self):
        classes = derive_prompt_classes("pedestrian")
        assert classes[0] == "pedestrian"

    def test_empty_query_returns_no_classes(self):
        assert derive_prompt_classes("") == []
        assert derive_prompt_classes("   ") == []

    def test_stopwords_are_not_returned_as_standalone_classes(self):
        classes = derive_prompt_classes("person with hardhat")
        assert "with" not in classes

    def test_capped_at_ten_classes(self):
        classes = derive_prompt_classes("a red truck and a blue van and a green car near a yellow bus")
        assert len(classes) <= 10

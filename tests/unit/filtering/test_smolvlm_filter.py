from PIL import Image

from app.filtering.smolvlm_filter import _crop_with_margin, _is_affirmative


class TestIsAffirmative:
    def test_plain_yes(self):
        assert _is_affirmative("yes") is True

    def test_capitalized_with_punctuation(self):
        assert _is_affirmative("Yes.") is True

    def test_yes_with_trailing_explanation(self):
        assert _is_affirmative("yes, it does show a dog") is True

    def test_plain_no(self):
        assert _is_affirmative("no") is False

    def test_unrelated_response_is_not_affirmative(self):
        assert _is_affirmative("it's a cat") is False

    def test_empty_response_is_not_affirmative(self):
        assert _is_affirmative("") is False


class TestCropWithMargin:
    def test_none_box_returns_image_unchanged(self):
        image = Image.new("RGB", (200, 100))
        assert _crop_with_margin(image, None) is image

    def test_expands_box_by_margin_ratio(self):
        image = Image.new("RGB", (200, 200))
        # box is 40x40 centered at (100,100): (80,80)-(120,120); margin 0.25
        # -> pad of 10px each side -> expect crop (70,70)-(130,130) = 60x60
        cropped = _crop_with_margin(image, (80.0, 80.0, 120.0, 120.0), margin_ratio=0.25)
        assert cropped.size == (60, 60)

    def test_clamps_to_image_bounds_near_top_left_corner(self):
        image = Image.new("RGB", (200, 200))
        # box (0,0)-(20,20), margin 0.5 -> pad 10px each side; left/top can't
        # go below 0, right/bottom expand normally -> (0,0)-(30,30)
        cropped = _crop_with_margin(image, (0.0, 0.0, 20.0, 20.0), margin_ratio=0.5)
        assert cropped.size == (30, 30)

    def test_clamps_to_image_bounds_near_bottom_right_corner(self):
        image = Image.new("RGB", (100, 100))
        # box (80,80)-(100,100), margin 0.5 -> pad 10px; right/bottom can't
        # exceed 100, left/top expand normally -> (70,70)-(100,100)
        cropped = _crop_with_margin(image, (80.0, 80.0, 100.0, 100.0), margin_ratio=0.5)
        assert cropped.size == (30, 30)

    def test_full_image_box_returns_full_image(self):
        image = Image.new("RGB", (50, 50))
        cropped = _crop_with_margin(image, (0.0, 0.0, 50.0, 50.0), margin_ratio=0.25)
        assert cropped.size == (50, 50)

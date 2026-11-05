"""Relevance gate behaviour.

The rules pinned here are the ones whose violation caused real damage: a
verdict that cannot be parsed must not read as acceptance, and an error must
never be reported as a rejection, because a rejection deletes the file.
"""

from app.filtering.vlm_filter import VisualVerifier, _is_affirmative, build_question


class TestIsAffirmative:
    def test_accepts_plain_yes(self):
        assert _is_affirmative("yes") is True
        assert _is_affirmative("Yes") is True
        assert _is_affirmative("YES") is True

    def test_tolerates_punctuation_and_whitespace(self):
        assert _is_affirmative("  Yes. ") is True
        assert _is_affirmative("yes, it does") is True

    def test_rejects_no(self):
        assert _is_affirmative("no") is False
        assert _is_affirmative("No.") is False

    def test_unparseable_answer_is_not_acceptance(self):
        """A model that ignores the instruction must not be read as a yes.

        Defaulting the other way would silently accept whenever the model
        rambled, which is the same class of failure as the old gate scoring
        everything 95%.
        """
        assert _is_affirmative("") is False
        assert _is_affirmative("I cannot tell") is False
        assert _is_affirmative("maybe") is False
        # "not yes" must not match on a bare substring search.
        assert _is_affirmative("not yes") is False


class TestBuildQuestion:
    def test_asks_about_visibility_not_subject_matter(self):
        """'clearly show' is load-bearing.

        Without it the model answers about what the scene is *about* rather
        than what is in the frame, so a conversation scene from an action film
        counts as a weapon scene.
        """
        q = build_question("a weapon")
        assert "clearly show" in q

    def test_constrains_the_answer_format(self):
        assert "yes or no" in build_question("a weapon").lower()

    def test_uses_the_query_verbatim(self):
        # Normalisation happens upstream in query_expansion; this must not
        # re-mangle an already-normalised phrase.
        assert "people with red bag" in build_question("people with red bag")


class _StubVerifier(VisualVerifier):
    """A verifier whose per-subject verdicts are scripted, so the any-of logic
    can be tested without loading a model."""

    def __init__(self, script):
        self.script = script  # subject -> {image name: True/False/None}
        self.asked: list[tuple[str, list[str]]] = []

    def verify_batch(self, image_paths, query):
        names = [p.name for p in image_paths]
        self.asked.append((query, names))
        return [self.script[query][n] for n in names]


class TestVerifyBatchAny:
    def _paths(self, *names):
        from pathlib import Path

        return [Path(n) for n in names]

    def test_accepts_when_any_subject_is_visible(self):
        v = _StubVerifier(
            {
                "a car": {"1": True, "2": False, "3": False},
                "a person": {"2": True, "3": False},
            }
        )
        assert v.verify_batch_any(self._paths("1", "2", "3"), ["a car", "a person"]) == [True, True, False]

    def test_later_subjects_are_only_asked_about_unresolved_images(self):
        v = _StubVerifier({"a car": {"1": True, "2": False}, "a person": {"2": False}})
        v.verify_batch_any(self._paths("1", "2"), ["a car", "a person"])
        assert v.asked == [("a car", ["1", "2"]), ("a person", ["2"])]

    def test_undecided_is_never_collapsed_into_rejection(self):
        # A rejection deletes the file, so "could not tell" must stay None.
        v = _StubVerifier({"a car": {"1": None, "2": False}, "a person": {"1": False, "2": False}})
        assert v.verify_batch_any(self._paths("1", "2"), ["a car", "a person"]) == [None, False]

    def test_a_yes_beats_an_earlier_undecided(self):
        v = _StubVerifier({"a car": {"1": None}, "a person": {"1": True}})
        assert v.verify_batch_any(self._paths("1"), ["a car", "a person"]) == [True]

    def test_single_subject_matches_verify_batch(self):
        v = _StubVerifier({"a car": {"1": True, "2": False, "3": None}})
        assert v.verify_batch_any(self._paths("1", "2", "3"), ["a car"]) == [True, False, None]

    def test_empty_input(self):
        assert _StubVerifier({}).verify_batch_any([], ["a car"]) == []


class TestStage5TinyVlmEnhancements:
    def test_verdict_parsing_categories(self):
        from app.filtering.vlm_filter import VlmVerdict, _parse_verdict

        assert _parse_verdict("Yes") == VlmVerdict.YES
        assert _parse_verdict(" yes, absolutely.") == VlmVerdict.YES
        assert _parse_verdict("No") == VlmVerdict.NO
        assert _parse_verdict("no.") == VlmVerdict.NO
        assert _parse_verdict("maybe, it is blurry") == VlmVerdict.UNCERTAIN
        assert _parse_verdict("cannot tell from this angle") == VlmVerdict.UNCERTAIN
        assert _parse_verdict("Random rambling text that ignores instruction") == VlmVerdict.UNCERTAIN

    def test_deterministic_caching_key(self):
        from app.filtering.vlm_filter import _compute_vlm_cache_key

        k1 = _compute_vlm_cache_key("sha_abc_123", "Red Car")
        k2 = _compute_vlm_cache_key("sha_abc_123", "red car")
        assert k1 == k2
        assert k1.startswith("vlm_cache:")

    def test_cached_verdict_retrieval(self):
        from unittest.mock import MagicMock, patch
        from app.filtering.vlm_filter import VisualVerifier, VlmVerdict

        with patch("app.filtering.vlm_filter._load_model") as mock_load:
            mock_proc = MagicMock()
            mock_model = MagicMock()
            mock_load.return_value = (mock_proc, mock_model, "cpu")

            verifier = VisualVerifier()
            # Mock redis cache hit
            with patch.object(verifier, "_get_cached_verdict", return_value=VlmVerdict.YES):
                res = verifier.verify_batch_verdicts(["img_mock_path"], "dog", fingerprints=["sha_123"])
                assert res == [VlmVerdict.YES]
                # Model inference should not have been called!
                assert mock_model.generate.call_count == 0

    def test_and_query_requires_all_subjects(self):
        from unittest.mock import MagicMock, patch
        from app.filtering.vlm_filter import VisualVerifier, VlmVerdict

        with patch("app.filtering.vlm_filter._load_model") as mock_load:
            mock_load.return_value = (MagicMock(), MagicMock(), "cpu")
            v = VisualVerifier()

            with patch.object(v, "verify_batch_verdicts") as mock_batch:
                # Query: "person AND helmet"
                # Call 1 (person): [YES, YES]
                # Call 2 (helmet): [YES, NO]
                mock_batch.side_effect = [
                    [VlmVerdict.YES, VlmVerdict.YES],
                    [VlmVerdict.YES, VlmVerdict.NO],
                ]

                results = v.verify_query_complex(["img1", "img2"], "person AND helmet")
                assert results == [VlmVerdict.YES, VlmVerdict.NO]

    def test_or_query_accepts_if_any_subject_visible(self):
        from unittest.mock import MagicMock, patch
        from app.filtering.vlm_filter import VisualVerifier, VlmVerdict

        with patch("app.filtering.vlm_filter._load_model") as mock_load:
            mock_load.return_value = (MagicMock(), MagicMock(), "cpu")
            v = VisualVerifier()

            with patch.object(v, "verify_batch_verdicts") as mock_batch:
                # Query: "car OR motorcycle"
                # Call 1 (car): [YES, NO]
                # Call 2 (motorcycle): [NO, YES]
                mock_batch.side_effect = [
                    [VlmVerdict.YES, VlmVerdict.NO],
                    [VlmVerdict.NO, VlmVerdict.YES],
                ]

                results = v.verify_query_complex(["img1", "img2"], "car OR motorcycle")
                assert results == [VlmVerdict.YES, VlmVerdict.YES]

    def test_cuda_oom_recovery_to_sequential_inference(self):
        import torch
        from unittest.mock import MagicMock, patch
        from PIL import Image
        from app.filtering.vlm_filter import VisualVerifier, VlmVerdict

        with patch("app.filtering.vlm_filter._load_model") as mock_load:
            mock_proc = MagicMock()
            mock_model = MagicMock()
            mock_load.return_value = (mock_proc, mock_model, "cuda")

            v = VisualVerifier()

            # First attempt (batch generate) raises CUDA OOM
            # Fallback attempts (sequential generate) succeed
            mock_model.generate.side_effect = [
                RuntimeError("CUDA out of memory"),
                MagicMock(),
                MagicMock(),
            ]
            mock_proc.batch_decode.return_value = ["Yes"]

            img1 = Image.new("RGB", (100, 100))
            img2 = Image.new("RGB", (100, 100))

            with patch("torch.cuda.empty_cache") as mock_empty_cache:
                results = v.verify_batch_verdicts([img1, img2], "tree", batch_size=2)
                assert len(results) == 2
                assert all(r == VlmVerdict.YES for r in results)
                assert mock_empty_cache.called

    def test_never_cache_transient_error(self):
        from unittest.mock import MagicMock, patch
        from app.filtering.vlm_filter import VisualVerifier, VlmVerdict

        with patch("app.filtering.vlm_filter._load_model") as mock_load:
            mock_load.return_value = (MagicMock(), MagicMock(), "cpu")
            v = VisualVerifier()

            mock_redis = MagicMock()
            with patch("app.core.redis_client.get_redis", return_value=mock_redis):
                # Attempt to cache ERROR
                v._set_cached_verdict("vlm_cache:test_key", VlmVerdict.ERROR)
                assert mock_redis.set.call_count == 0  # Must NOT be called!

                # Attempt to cache YES
                v._set_cached_verdict("vlm_cache:test_key", VlmVerdict.YES)
                assert mock_redis.set.call_count == 1  # YES is cached

    def test_cache_separation_on_parameter_change(self):
        from app.filtering.vlm_filter import _compute_vlm_cache_key

        fp = "sha256_image_fingerprint"
        q = "excavator"

        k_base = _compute_vlm_cache_key(
            fp, q, prompt_version="v1", model_id="model_a",
            model_revision="commit_rev_1", model_config_identity="fp16_tok5_greedy",
            preprocess_config="dim_384_notile", logical_operator="EXACT"
        )
        k_diff_prompt = _compute_vlm_cache_key(
            fp, q, prompt_version="v2", model_id="model_a",
            model_revision="commit_rev_1", model_config_identity="fp16_tok5_greedy",
            preprocess_config="dim_384_notile", logical_operator="EXACT"
        )
        k_diff_model = _compute_vlm_cache_key(
            fp, q, prompt_version="v1", model_id="model_b",
            model_revision="commit_rev_1", model_config_identity="fp16_tok5_greedy",
            preprocess_config="dim_384_notile", logical_operator="EXACT"
        )
        k_diff_revision = _compute_vlm_cache_key(
            fp, q, prompt_version="v1", model_id="model_a",
            model_revision="commit_rev_2", model_config_identity="fp16_tok5_greedy",
            preprocess_config="dim_384_notile", logical_operator="EXACT"
        )
        k_diff_config = _compute_vlm_cache_key(
            fp, q, prompt_version="v1", model_id="model_a",
            model_revision="commit_rev_1", model_config_identity="fp32_tok10",
            preprocess_config="dim_384_notile", logical_operator="EXACT"
        )
        k_diff_preprocess = _compute_vlm_cache_key(
            fp, q, prompt_version="v1", model_id="model_a",
            model_revision="commit_rev_1", model_config_identity="fp16_tok5_greedy",
            preprocess_config="dim_512_tiled", logical_operator="EXACT"
        )
        k_diff_operator = _compute_vlm_cache_key(
            fp, q, prompt_version="v1", model_id="model_a",
            model_revision="commit_rev_1", model_config_identity="fp16_tok5_greedy",
            preprocess_config="dim_384_notile", logical_operator="AND"
        )

        assert k_base != k_diff_prompt
        assert k_base != k_diff_model
        assert k_base != k_diff_revision
        assert k_base != k_diff_config
        assert k_base != k_diff_preprocess
        assert k_base != k_diff_operator

    def test_redis_unavailable_behavior(self):
        from unittest.mock import MagicMock, patch
        from app.filtering.vlm_filter import VisualVerifier, VlmVerdict

        with patch("app.filtering.vlm_filter._load_model") as mock_load:
            mock_load.return_value = (MagicMock(), MagicMock(), "cpu")
            v = VisualVerifier()

            with patch("app.core.redis_client.get_redis", side_effect=ConnectionError("Redis offline")):
                # Should not raise exception; gracefully returns None
                assert v._get_cached_verdict("vlm_cache:any") is None
                # Should not raise on set
                v._set_cached_verdict("vlm_cache:any", VlmVerdict.YES)

    def test_corrupted_image_handling(self):
        import io
        from unittest.mock import MagicMock, patch
        from app.filtering.vlm_filter import VisualVerifier, VlmVerdict

        with patch("app.filtering.vlm_filter._load_model") as mock_load:
            mock_load.return_value = (MagicMock(), MagicMock(), "cpu")
            v = VisualVerifier()

            # Corrupted image bytes
            corrupted_stream = io.BytesIO(b"not an image byte sequence")
            results = v.verify_batch_verdicts([corrupted_stream], "car")
            assert results == [VlmVerdict.ERROR]

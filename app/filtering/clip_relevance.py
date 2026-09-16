"""CLIP-based relevance scoring — a continuous, near-free replacement for
running a VLM over every candidate asset.

Why this exists
---------------
Relevance used to be decided solely by a 256M-parameter VLM answering a
yes/no question about every image, at roughly 66 seconds per image on CPU.
That is both slow and a single coarse bit of signal: `Asset.relevance_score`
was populated from YOLO-World's detector confidence, which on a real job
(query "weapon on movies scence") was 0.0 for all 754 assets. Every consumer
of that column was therefore sorting or thresholding on a constant -- see
_select_trim_candidates, which decides *which* surplus assets to throw away,
and the export filter's min_relevance_score.

The pipeline already computes a normalised CLIP ViT-B/32 image embedding for
every asset at promotion time (StandardizationService._compute_embedding) and
stores it on the row for deduplication. Scoring relevance against those
stored vectors costs one text-encoder pass per batch plus a matrix multiply:
no image decode, no per-image model call. So the expensive VLM can be
reserved for the genuinely ambiguous minority instead of being asked about
everything.

How the score is defined
------------------------
Not as a raw cosine similarity. CLIP's similarities occupy a narrow band --
measured on this job's own frames, every value fell between 0.20 and 0.26,
including both obvious matches and obvious non-matches -- so no absolute
cutoff on cosine distance separates them. Instead the query's prompt ensemble
is scored against a fixed set of background/contrast prompts in one softmax,
and relevance is the probability mass landing on the query side. That is a
relative judgement ("does this look more like the query than like a title
card, a talking head, an empty room?"), which is both better separated and
stable across queries.

Calibration caveat
------------------
This is a prefilter, not an oracle. On the frames used to develop it, a dark
interior with no visible subject still scored high against an early negative
set -- CLIP is confidently wrong on low-information images. That is precisely
why the caller uses three bands rather than one threshold: confident accept,
confident reject, and an uncertain middle that gets escalated to the VLM. The
negative prompt list in query_expansion.py is the main lever for that failure
mode and is expected to be tuned per deployment.
"""

import logging

logger = logging.getLogger(__name__)

# CLIP's learned logit scale (exp of the trained temperature, ~100). Applied
# before the softmax so the distribution is as sharp as the model intends;
# without it the probabilities collapse toward uniform and the bands lose
# all separation.
_LOGIT_SCALE = 100.0


def _load_text_encoder():
    """Reuses the process-wide CLIP model that already produces image embeddings.

    Imported from standardization_service rather than loaded again: the two
    must be the *same* checkpoint or the text and image vectors live in
    different spaces and the dot product is meaningless. Sharing it also
    avoids a second copy of the weights in every worker process.
    """
    from app.services.standardization_service import _load_clip_model

    return _load_clip_model()


def encode_prompts(prompts: list[str]):
    """Normalised text embeddings for `prompts`, as a (len(prompts), dim) tensor."""
    import clip
    import torch

    model, _preprocess, device = _load_text_encoder()
    tokens = clip.tokenize(prompts, truncate=True).to(device)
    with torch.no_grad():
        features = model.encode_text(tokens)
        features = features / features.norm(dim=-1, keepdim=True)
    return features.float()


def score_embeddings(
    embeddings: list[list[float] | None],
    positive_prompts: list[str],
    negative_prompts: list[str],
) -> list[float | None]:
    """Relevance in [0, 1] for each stored image embedding.

    Returns None in a slot whose embedding is missing (CLIP failed at
    promotion time) so the caller can route that asset to the VLM rather than
    silently scoring it 0 and deleting a possibly-good image -- a missing
    embedding is an absence of evidence, not evidence of irrelevance.

    Embeddings are assumed L2-normalised, which is what
    _compute_embedding guarantees before storing them.
    """
    import torch

    if not positive_prompts:
        return [None] * len(embeddings)

    present = [(i, e) for i, e in enumerate(embeddings) if e]
    scores: list[float | None] = [None] * len(embeddings)
    if not present:
        return scores

    prompts = list(positive_prompts) + list(negative_prompts)
    n_positive = len(positive_prompts)

    try:
        text_features = encode_prompts(prompts)
    except Exception:
        logger.exception("CLIP text encoding failed; deferring the whole batch to the VLM")
        return scores

    try:
        device = text_features.device
        image_features = torch.tensor(
            [e for _, e in present], dtype=torch.float32, device=device
        )
        # Rows are already unit-norm, but a defensive re-normalise costs
        # nothing and protects against an older row written before that
        # guarantee existed.
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)

        logits = _LOGIT_SCALE * (image_features @ text_features.T)
        probabilities = logits.softmax(dim=-1)
        # Probability mass on the query side of the contrast set.
        relevance = probabilities[:, :n_positive].sum(dim=-1)

        for (index, _), value in zip(present, relevance.tolist()):
            scores[index] = float(value)
    except Exception:
        logger.exception("CLIP relevance scoring failed; deferring the batch to the VLM")
        return [None] * len(embeddings)

    return scores


def classify(score: float | None, accept_at: float, reject_at: float) -> str:
    """Maps a score to "accept", "reject" or "uncertain".

    A None score is always "uncertain": the point of the band is that we only
    act without a VLM when CLIP is confident, and no score is not confidence.
    """
    if score is None:
        return "uncertain"
    if score >= accept_at:
        return "accept"
    if score <= reject_at:
        return "reject"
    return "uncertain"

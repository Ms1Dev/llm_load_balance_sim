import numpy as np

# --- Fitted constants from WildChat-1M sample ---
PROMPT_SHAPE, PROMPT_SCALE     = 1.4479, 26.1443
PROMPT_CLIP                    = (1, 2034)

RESPONSE_SHAPE, RESPONSE_SCALE = 1.1516, 212.8026
RESPONSE_CLIP                  = (6, 1446)

SLOPE, INTERCEPT, RESID_STD    = 0.1922, 4.7309, 1.0962

_LOREM_BASE = (
    "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
    "incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis nostrud "
    "exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute "
    "irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
    "pariatur excepteur sint occaecat cupidatat non proident sunt in culpa qui officia "
    "deserunt mollit anim id est laborum sed ut perspiciatis unde omnis iste natus error "
    "sit voluptatem accusantium doloremque laudantium totam rem aperiam eaque ipsa quae "
    "ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo"
).split()
# Repeat to cover max slice range: PROMPT_CLIP[1] + RESPONSE_CLIP[1] = 2034 + 1446 = 3480 words needed
_LOREM = _LOREM_BASE * 40  # ~4000 words

def generate_prompt_tokens(n: int, seed: int = None) -> list[int]:
    rng = np.random.default_rng(seed)
    return rng.lognormal(np.log(PROMPT_SCALE), PROMPT_SHAPE, size=n).clip(*PROMPT_CLIP).astype(int).tolist()

def generate_response_tokens(prompt_tokens: int, seed: int = None) -> int:
    rng = np.random.default_rng(seed)
    log_r = SLOPE * np.log1p(prompt_tokens) + INTERCEPT + rng.normal(0, RESID_STD)
    return int(np.clip(np.expm1(log_r), *RESPONSE_CLIP))


def generate_prompt(n: int, seed: int = None) -> list[str]:
    token_lengths = generate_prompt_tokens(n, seed)
    return [" ".join(_LOREM[i:i + p]) for i, p in enumerate(token_lengths)]

def generate_response(prompt_tokens: int, seed: int = None) -> tuple[str, int]:
    response_tokens = generate_response_tokens(prompt_tokens, seed)
    return " ".join(_LOREM[prompt_tokens:prompt_tokens + response_tokens]), response_tokens
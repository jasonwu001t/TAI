import pytest
from TAI.genai.genai_v0_pending_delete import GenAI

def test_genai():
    genai = GenAI()
    response = genai.generate_text("Hello, world!")
    assert response is not None

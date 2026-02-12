from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, parsers, scenario, then, when
from recommender.main import app as recommender_app

pytestmark = pytest.mark.bdd


@scenario("features/recommender.feature", "Rank job postings by resume overlap")
def test_rank_postings_by_resume_overlap() -> None:
    pass


@scenario("features/recommender.feature", "Reject a resume that is too short")
def test_reject_short_resume() -> None:
    pass


@pytest.fixture
def context() -> dict[str, object]:
    return {}


@given("a resume and job postings for recommendation")
def given_recommend_payload(context: dict[str, object]) -> None:
    context["payload"] = {
        "resume_text": "Experienced backend python developer building API systems and tooling.",
        "postings": [
            {
                "id": "job-1",
                "title": "Backend Engineer",
                "description": "Build Python API services",
            },
            {
                "id": "job-2",
                "title": "Data Scientist",
                "description": "Train machine learning models",
            },
            {"id": "job-3", "title": "Platform Engineer", "description": "Own CI pipelines"},
        ],
    }


@given("a short resume payload")
def given_short_resume_payload(context: dict[str, object]) -> None:
    context["payload"] = {"resume_text": "too short", "postings": []}


@when("recommendations are requested from the recommender API", target_fixture="response")
def when_recommendations_are_requested(context: dict[str, object]):
    with TestClient(recommender_app) as client:
        return client.post("/recommend", json=context["payload"])


@then("the recommender response is successful")
def then_response_is_successful(response) -> None:
    assert response.status_code == 200


@then("the recommendations are sorted by score")
def then_recommendations_are_sorted(response) -> None:
    recommendations = response.json()["recommendations"]
    scores = [item["score"] for item in recommendations]
    assert scores == sorted(scores, reverse=True)


@then(parsers.parse('the top recommendation is "{title}"'))
def then_top_recommendation_matches(response, title: str) -> None:
    assert response.json()["recommendations"][0]["title"] == title


@then("the recommender response has validation errors")
def then_validation_errors_are_reported(response) -> None:
    assert response.status_code == 422

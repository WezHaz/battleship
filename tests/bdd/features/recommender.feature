Feature: Recommender API
  Scenario: Rank job postings by resume overlap
    Given a resume and job postings for recommendation
    When recommendations are requested from the recommender API
    Then the recommender response is successful
    And the recommendations are sorted by score
    And the top recommendation is "Backend Engineer"

  Scenario: Reject a resume that is too short
    Given a short resume payload
    When recommendations are requested from the recommender API
    Then the recommender response has validation errors

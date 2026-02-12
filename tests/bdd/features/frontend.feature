Feature: Frontend recommendation proxy
  Scenario: Proxy recommendation requests to recommender
    Given a frontend request payload
    And the recommender upstream succeeds
    When the frontend proxy endpoint is called
    Then the frontend response is successful
    And the frontend response includes the recommender payload
    And the forwarded payload contains generated posting ids

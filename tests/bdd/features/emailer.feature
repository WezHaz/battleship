Feature: Email digest scheduling
  Scenario: Queue a digest job for each recipient
    Given two digest recipients and jobs
    When the digest cron endpoint is called
    Then the digest endpoint responds with queued status
    And two digest jobs are queued

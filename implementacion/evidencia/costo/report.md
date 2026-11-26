# Harness report - team01

Automated score: **5.0 / 70.0**

| Req | Title | Status | Points |
|---|---|---|---|
| R1 | Every API action emits one valid event | not_verified | 0.0/7 |
| R2 | No event loss under burst, API stays fast | not_verified | 0.0/6 |
| R3 | Live view fed by the stream (event time) | not_verified | 0.0/7 |
| R4 | Events reach the S3 landing zone | not_verified | 0.0/4 |
| R5 | Star schema tables per contract | not_verified | 0.0/7 |
| R6 | Star integrity (keys, orphans, duplicates) | not_verified | 0.0/5 |
| R7 | Business queries return the right numbers | not_verified | 0.0/11 |
| R8 | Dirty events handled, refresh in time | not_verified | 0.0/6 |
| R9 | Slowly changing dimension (type 2) | not_verified | 0.0/8 |
| R10 | Partition pruning and scan limit | not_verified | 0.0/4 |
| R11 | Minimal Kinesis stream | passed | 3.0/3 |
| R12 | Cost projection | passed | 2.0/2 |

## R1 Every API action emits one valid event - not_verified

## R2 No event loss under burst, API stays fast - not_verified

## R3 Live view fed by the stream (event time) - not_verified

## R4 Events reach the S3 landing zone - not_verified

## R5 Star schema tables per contract - not_verified

## R6 Star integrity (keys, orphans, duplicates) - not_verified

## R7 Business queries return the right numbers - not_verified

## R8 Dirty events handled, refresh in time - not_verified

## R9 Slowly changing dimension (type 2) - not_verified

## R10 Partition pruning and scan limit - not_verified

## R11 Minimal Kinesis stream - passed
- passed: `test_stream_is_minimal` 
- evidence: stream: {'mode': 'PROVISIONED', 'open_shards': 1, 'retention_hours': 24, 'consumers': 0}

## R12 Cost projection - passed
- passed: `test_projection_within_budget` 
- passed: `test_projection_lists_the_services_used` 
- evidence: declared 30.98 USD/month; limit 50.00

# Sparta as Flink SQL

## TODO next

- Other idea
    - DataStream until aggregation and throttling and Flink SQL from there
        - Initial stage aggregate risk results
        - Join risk & position 
            - Enforce latest constraint
            - Publish change log
        - Aggregate & throttle

- How to throttle production of aggregate risk
- Figure out firm account indic join
    - Try post first level aggregation
- Test out upsert-kafka support
- Live join with trade indicative
    - Try as lateral processing time join?
    - Try as data-stream primitive
- Flink SQL
    - Test out ROLLUP Cube support in Blink planner

## Scenario setup

### Dimensions
- 5m unique risk records
- 100m risk updates
- 1k firm accounts
- 1k issuers

## Key functional tests
- Updates on the same trade, including out of order updates (such as when some calculations take longer than others)
- Indicative changes 
    - For example on a trade such as change of issuer or firm account
    - Also on account such as description
- Cold start of components
- Drilldown queries
- Flexibility for new use cases (new quant queries, historic queries etc)
- Batch release of risk updates

## Design

- Overall flow
    1. Latest by trade including foreign keys
    2. Group by issuer (and if desired account)
    3. Enrich with further rollups 
- Batching of updates
    - Could be done in a pre-step where results are partitioned by the calc id and 
      then released once the batch is complete
    - This would not guarantee totally simultaneous updates but they would be pretty close.
- Measuring TPS via Flink custom metric or at the source ...


## Flink considerations

### Ingredients needed

- Group by and select latest for issuer update, yielding a retract stream
- Latest join from two sides ideally or at least ref join from one side
    - Lateral join can give good enough results intraday but doesn't really help for long
    running processes where the indicative can equally change

### Aggregation as UDAGG
- Group by with user defined aggregation function
- SELECT issuer, latestRisk(uid, JTD, timestamp) from LiveRisk group by issuer
- latestRisk UDAGG returns the latest risk
  - Prior aggregation by trade that creates a retract stream
- TODO: Can we construct a whole pipeline risk + firm account + ... ?
- TODO: How does cold start work for the UI? And drilldown?
- TODO: Can the latestValue function use Blink planner or a more efficient representation

- Flow:
    latest by trade including identifiers like issuer id and account id
    -> group by issuer
    -> enrich with further roll ups

### Batching in Flink SQL

- Can do in DataStream process function or window
- Maybe can use a session or unbounded window on the aggregation query
    - One day session window with grouping by date and table aggregation function (can this trigger an emit)?
- Could use custom table aggregation function?
- What about a session window that releases after 1 sec quiescence?
    - What would we session over?
        - The trades?
        - The aggregation sum
        - The aggregation result stream?
    - What if there is never quiescence?


### Problems with FlinkSQL approach

- So far no support for
    - Upsert based flows
    - Batching and throttling of updates
    - Cannot mix & match with data streams unless you want append-only behavior

### Streaming SQL against non-historic latest value table
- Create a non-historicized table
- From there stream results (append/retract or C(R)UD

### Observations

- Blink planner LAST_VALUE does not work easily. It either doesn't take a timestamp attribute
  or requires event time windowing
    - Even with eventtime all set up the results seemed incorrect
- Throughput
    - Sum(JTD by issuer) on latest trade: ~200-230k per second on parallelism=4, ~900k TPS/s
    - Sum(JTD by issuer) on latest trade: ~120-125k per second on parallelism=8, ~1mm TPS/s
- Is SQL the right go to way for the core structures or better suited to extensibility?
- Retract stream seems to not be convertible into a table
    - Maybe with a custom TableSource?
    
#### Performance

- Parallelism=4, single threaded generator sources, local
    - 1m positions, 1k/10k issuer, 1k/10k account
        - ~85k rec/s, ~200 min for 1bn records
- Parallelism=8, single thread generator sources, local
    - 1m positions, 1k/10k issuer, 1k/10k account
        - ~90k rec/s, ~185min for 1bn records
    
### Internals 

#### DataStream to changelog table
- Datastream to table conversion relies on planner special support for JavaDataStreamQueryOperation
- Old TableSource interface offers no way to signal that retracts are possible, even though they can be sent in Row object
- New TableSource allows for retractions
    - Linking it to a data stream instead of source function seems not to be possible though
    - Can it support keeping the necessary state?

#### Effective processing time joins with changelog
A full join with changelog send correct retractions of previous events.
    
#### Using upsert instead of delete/insert
The delete/insert cycle turns every message into two, rather than an upsert, which while requiring a key to be defined 
only requires a single message per update.

- However, note that upserts are less generic under conditions of re-keying. For example changing the issuer on a risk record
  means that the old value has to be retracted and the new one inserted - it cannot be upserted in place because
  the state lives on different partitions.


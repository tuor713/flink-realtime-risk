# Sparta in Flink 

## Approach 
Handle all logic in one workflow, use Flink for latest statement management.

#### Flow

```
Risk + Position -> (Risk, Position) -> RiskAggregate<IssuerId, AccountId>
```

## Problems to solve 

#### How to account for updates to ref data like account changes
Some changes will come for free such as trade updates triggering risk updates. For others though
such as account updates the system has to accommodate the update.

Is there a point to try to use broadcast streams 

#### Trade risk changes from one issuer to another

#### Is there a generic notion of RiskAggregate that works for trades & aggregates

#### How to support drilldown to trade level 

#### Alternatively how do we extract a full set of consistent data
For example, can we use a broadcast stream

#### How to join issuer, accounst and products?

#### How to implement complex business logic such as limits

#### How to support batched updates?
- Could we use the Flink watermarks to insert boundaries where data has been fully processed?
- Batching needs to be handled until the very end(s) (or risk updates sent as big messages) because otherwise 
partial data could continue to arise in later processing.
  
#### How to query data
Select query against in memory cache via broadcast stream
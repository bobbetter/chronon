

# Why?

  ML, AI & Metric systems alike need to ensure that data that is going in, and coming out is stable - and hence healthy.
Typically, *more effort* is required to build and configure such monitoring systems that the data systems themselves. 
We aim to build an automated, scalable and realtime monitoring platform to *surface data issues* with high signal. 

## Fan-out & Zero-Touch
To simply monitor a single metric or a feature, one typically needs to monitor three separate aspects

- coverage - indicates the ratio of null (absent) to non-null data. Drop in coverage, or increase in null ratio needs 
             to be monitored.  
- distribution - represents the distribution of continuous or discrete value. Changes to these also indicate presence 
                 of issues.
- rate - is global metric - that represents the number of entries entering or exiting the system. 

For complex types - like sequences, vectors and bucketed columns. Monitoring changes to coverage, distribution and rate 
becomes more complex and involved for such types. However, these features also tend to be higher in their importance 
to the model.

Below are some examples 
- Sequences - user action type - impression, click, buy, call for support, refund etc. Sequences are very effective 
              at modeling user intent and are highly applicable to recommendation and fraud detection systems. Typically 
              represented as a variable sized array with potentially nullable elements.
- Bucketed features - are used to analyze more granular *buckets* or facets of an entity. For example - average txn 
                      price per item category - is a map of category string to a floating point value.  
- Vectors - are *summaries* of natural data such as text and images in a high-dimensional space. Useful for searching 
            relevant items or to transfer learning from larger general purpose model to a smaller task specific model.
            Embedding natural data into vectors is on a rapid rise with recent breakthroughs in large foundation 
            modeling techniques.
 
The main takeaway of this section is that typically *the amount of monitoring logic for a data system is a lot more than the 
effort required to implement the processing logic*. This is a big reason why data systems are poorly monitored. Zipline,
because we understand the underlying processing logic, we can automatically derive the monitoring logic - *without any 
additional user configuration*.

## Shift-left
Current generation of monitoring platforms (opensource & proprietary) all directly index raw data and compute monitoring
metrics on page loads or on alert schedules. This means the amount of raw data to index is humongous and wasteful. Since
we know / derive, the exact set of metrics, we can compute them before they ever reach the index. This approach typically
leads to an order-of-magnitude higher scalability and cost efficiency.

## Density
Many features in a model, many metrics to monitor for each feature. Spotting problematic changes from this massive set 
of metrics needs to be explicitly designed for. Designing for signal at such scale - without requiring a lot of user 
clicks and scrolls is very hard. Most solutions are only useful for verifying *known* problems - we want to make our
system truly useful for surfacing *unknown* problems.

## Realtime
While daily drift monitoring is a reasonable starting point. However, for models with potential for financial or impact
or abuse, it is essential that we surface and remedy issues in realtime. 

# Approach
We have three stages of drift computation
- cardinality estimation - to detect different types of features
- summarization - for each feature type, compute appropriate summaries
- drift computation - comparison of summaries to come up with a drift score
  - we use jensen-shannon distance to compute drift
  - other things that are in the pipeline are Hellinger distance, Kolmogorov-Smirnov & PSI

## Cardinality estimation
Cardinality is estimated via CPCSketch - which has the most optimal storage to accuracy ratio among all sketching 
methods. We will use this to determine if a feature is categorical or numeric.

For numeric features we will compute percentile values using KLL sketch with a fixed number of same width intervals. 
We are going to start with 20 for now. 20 intervals leads to 0.05% of values per interval. 
  - p0, p5, ... p95, p100

For categorical features we will compute histogram of values. To compute drift metrics of numerical value 
summaries we need to convert the percentile array into a probability distribution function.

At a high level, we look at the schema of the dataframe and the types of columns and generate expressions to compute 
cardinality of each column. We implement udafs needed to do this counting. Once a cardinality map is constructed we 
mark each column as either high cardinality or low cardinality, and have a case tree to determine which expressions
to run to generate to "summarize" - high cardinality numerics are summarized via percentiles, while low cardinality numeric
and string values are summarized with a histogram. Some amount of complexity emerges due to handling of nested data - 
sequences or array, maps or bucketed aggregations.

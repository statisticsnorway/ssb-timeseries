# ADR-0011: Iterating a Dataset yields detached Series projections

Status: Accepted

## Context

A dataset is a matrix of equally typed series, but a single series was not an object of its own.
Analysis and forecasting libraries take one series at a time, in their own long format.
Indexing a dataset returned another dataset, so each integration needed its own adapter over the frame.

## Decision

Iterating a dataset yields one Series per series name.
A Series is a projection of the dataset's temporal columns and that series' value column, plus the as-of timestamp when the dataset is as-of versioned.
The data is detached in both directions: a Series does not change the dataset, and later changes to the dataset do not reach a Series already produced.
A Series carries the series' tags and the dataset's type; it does not write them back.

## Consequences

- A series can be handed to another library without carrying the whole dataset.
- Iteration materialises a new frame per series, so it is not free on a large dataset.
- Tags and technical properties are controlled through the dataset; a Series carries a copy and cannot persist a change.
- The facade is experimental; its API may change.

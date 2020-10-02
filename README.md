# Array operations in Apache Flink

This is a fork of the Apache Flink repository, modified to allow array-based stream operations. This project was undertaken for my master's degree project at the University of Edinburgh.

## Array Operations

We allow for array operations such as FFT to be applied directly to an input stream, by guaranteeing a consistent number of events in every window and equal distance between said events. Currently the implementation is to be used with Count Windows, as they offer the same performance as Event Time Windows within this framework. Event Time Windows provide inconsistent results in the current build.

## Resampling

I provide a resampling module that aligns stream events to user-specified intervals, ensuring equal time-distance between every stream element. Each interval has the closest event asigned to it, dropping redundant events and filling in missing ones using user-specified interpolation methods.

## Sliding Windows

We enable array operations on sliding windows, allowing for aggregation of elements assigned to several windows. This enables techniques such as the overlap-add method frequently used in digital signal processing to be applied. Furthermore we offer data forwarding of already-evaluated window data to subsequent windows to enable incremental computation. Note: In the current build the feature requires additional testing and evaluation.

## Examples and Report

Usage examples can be found in: MastersDissFlink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/windowing/ . MSCDissertation.pdf contains the report on my master's project.

## Kafka Streams Demo
Demo使用的是 Kafka 1.0.1 版本的API

[官方文档](http://kafka.apache.org/documentation/streams/developer-guide/)提供了两套开发的API，包括高级 Streams DSL 和底层处理 Processor API

其中：

1、一个 Kafka Streams 应用是由若干个 `处理拓扑` 组成。

2、每个 `处理拓扑` 是一个通过 `流` 连接 `Processor实例`(点，node)形成的图

3、一个 `Processor实例` 是一个 `处理拓扑` 的节点

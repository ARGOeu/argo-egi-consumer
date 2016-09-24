**argo-egi-consumer** is component responsible for fetching raw metric results coming from monitoring instances in EGI infrastructure. Results are exchanged across Message Broker Network where consumer subscribes to and synchronously listens on destinations. Every received result is encoded according to specific schema and is written in AVRO serialization format forming the one day log of metric results suitable for processing by _argo-compute-engine_.

**argo-egi-consumer** is written as a unix daemon forking itself into background, reacts on certain signals and reports it behaviour via system log. Multi instance support is enabled so there could be many instances of consumer running on the same physical machine. It interacts with the broker network
through the use of simple STOMP protocol, but relies on TCP for failover. Results coming to broker network must be properly formatted and within the
allowed retention period, otherwise will be discarded by consumer.

Configuration is simple and is centered around one configuration file `consumer.conf` and message schema definition.

More information: http://argoeu.github.io/guides/consumer

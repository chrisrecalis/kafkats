---
"@kafkats/client": minor
"@kafkats/flow": minor
"@kafkats/codec-zod": minor
"@kafkats/flow-state-lmdb": minor
---

### ACL Support

Added Access Control List (ACL) management to the admin client:

- `admin.describeAcls()` - Query ACLs by filter criteria
- `admin.createAcls()` - Create ACL bindings for principals
- `admin.deleteAcls()` - Delete ACLs matching filters

Includes full protocol support for DescribeAcls, CreateAcls, and DeleteAcls APIs (v2-v3).

### Consumer Seek

Added `consumer.seek(topic, partition, offset)` method to reposition the fetch offset for a partition. Enables replaying messages or skipping ahead to a specific offset. Works with the pause/seek/resume pattern for controlled repositioning.

### Documentation

Added comprehensive KafkaJS migration guide covering client configuration, producer, consumer, transactions, and admin API differences.

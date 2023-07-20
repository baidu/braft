witness 副本只作为仲裁者进行投票，不保存实际的业务数据。
## 实现方案
对于witness的实现，需要考虑部署方式。 对于2+1部署，如果不允许witness当选选主，那么当主节点异常宕机的时候，如果wintess拥有比另外一个副本更新的entry，那么会导致选主失败，为了提高可用性，需要考虑允许witness短时间内允许当选为主，wintess成为主以后再主动transfer leader给另一个副本。**通过允许witness临时成为主可以提高系统的可用性** 。

对于4+1 的部署方式，实现相对简单，只需要让witness不能当选为主即可，因为即便主节点故障，依然至少有一个副本拥有最新的entry从而可以当选为主。由于witness不能当选为主，因此在同步raft log的时候也可以不需要同步log data给witness。当4+1部署的时候，如果不允许witness当选为主，那么最多只能容忍一个节点故障，如果允许witness临时当选为主，那么可以容忍两个节点故障。允许witness当选为主时，实现
则与2+1部署一致。

## 详细实现 
### witness不允许当选为主
当witness不允许当选为主时，只需要在初始化Node的时候禁止election_timeout timer进行初始化即可，同时可以无需进行data复制。

### witness允许临时当选为主
允许witness当选为主可以提升服务的可用性。具体实现为:
* 设置raft_enable_witness_to_leader flag为true，允许witness临时选举为主
* election_timeout设置为正常节点的两倍，在主节点异常宕机的时候，允许witness发起选主，同时由于election_timeout比数据副本大，可以保证数据副本优先被选为主，只有数据副本选主失败时，witness才会主动发起选主。
* witness当选为主时，禁止安装快照请求，避免从节点获取到空快照覆盖原有的业务数据
* 新增witness副本时， witness向leader发送install sanpshot请求，如果replicator本身是witness，则无需进行data文件的复制，只需复制最新的entry即可。

## witness 使用注意事项
* 如果不允许witness当选为主时，相比原有raft方式部署，服务可用性会明显降低
* 当允许witness临时当选为主时，极端情况下，可能导致从节点无法获取到最新的log entry从而导致数据丢失。
例如:
```
2+1的时候，日志为 [1, 8]，某一时刻 replica1(leader) [1, 8] replica2 [1, 5] witness[4,8]。witness snapshot save，truncate 数据到 [7,8]。replica1(leader) 挂了，replica2 [1, 5] 和 witness 的数据接不上了, 此时会导致日志丢失。
```
用户在使用witness的时候，需要评估witness带来的可用性降低以及可能丢失部分最新数据的风险。
如果业务无法接受数据丢失，可以自定义实现LogStorage, 只有半数以上副本拥有entry时，witness才能truncate 该entry之前的log。

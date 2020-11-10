# paxoskv: a Naive and Basic paxos kv storage

![naive](https://github.com/openacid/paxoskv/workflows/test/badge.svg?branch=naive)
[![Build Status](https://travis-ci.com/openacid/paxoskv.svg?branch=naive)](https://travis-ci.com/openacid/paxoskv)

这个repo 目前仅是用于学习的实例代码.

这是一个基于paxos, 只有200行代码的kv存储系统的简单实现, 以最简洁的形式展示paxos如何运行, 作为 [可靠分布式系统-paxos的直观解释][] 这篇教程中的代码示例部分.

因为有不少网友跟我问起这篇教程中的实现问题, 例如怎么把只能确定一个值的paxos应用到实际场景中.
既然**Talk is cheap**, 那么就**Show me the code**, 把教程中描述的内容直接用代码实现出来, 希望能覆盖到教程中的每个细节. 帮助大家理解paxos的运行机制.

NB-paxoskv 通过classic paxos建立一个简单的kv存储,
这个版本只支持指定key-version的写入和读取:

- 写入操作通过一次2轮的paxos实现.

- 读取操作也通过一次1轮或2轮的paxos实现.

- 虽然每个key支持更新(通过多个ver),
    但在这个版本的代码中只能通过指定ver的方式写入,
    目前还不支持把对key的更新自动作为下一个ver来写入(不似生产环境kv存储的实现).

- 没有以状态机的方式实现 WAL and compaction的存储, 它直接把paxos instance对应到key的每个版本上.

# 名词

在paxos相关的paper, [可靠分布式系统-paxos的直观解释][],
以及这个repo中代码涉及到的各种名词, 下面列出的都是等价的:

```
rnd == bal == BallotNum ~= Ballot
quorum == majority == 多数派
voted value == accepted value // by an acceptor
```

# Usage

跑测试: `GO111MODULE=on go test ./...`.

重新build proto文件(如果宁想要修改下玩玩的话): `make gen`.

数据结构使用protobuf 定义; RPC使用grpc实现;


# 目录结构

- `proto/paxoskv.proto`: 定义paxos相关的数据结构.

- `paxoskv/`:

    - `impl.go`: 206行代码实现的paxos协议:
        - 实现paxos Acceptor的`Prepare()`和`Accept()`这两个request handler;
        - 实现Proposer的功能: 执行`Phase1()`和`Phase2()`,
        - 以及完整运行一次paxos的`RunPaxos()`方法;
        - 实现一个kv纯内存的存储, 每个key有多个version, 每个version对应一个paxos instance;
        - 以及启动n个Acceptor的grpc服务函数

    - `paxos_slides_case_test.go`: 按照 [可靠分布式系统-paxos的直观解释][] 给出的两个例子([slide-32][]和[slide-33][]), 调用paxos接口来模拟这2个场景中的paxos运行.

    - `example_set_get_test.go`: 使用paxos提供的接口实现指定key和ver的写入和读取.

# Question

如果有任何问题, 欢迎提[issue] :DDD.


[issue]:                          https://github.com/openacid/paxoskv/issues/new/choose
[可靠分布式系统-paxos的直观解释]: https://blog.openacid.com/algo/paxos/
[slide-32]:                       https://blog.openacid.com/algo/paxos/#slide-32
[slide-33]:                       https://blog.openacid.com/algo/paxos/#slide-33

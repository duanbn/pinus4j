---
layout: default
---

# 事务机制

Pinus 的事务涉及到多个数据库资源，因此属于分布式事务的范畴，但是需要特别说明的是，分布式事务是一种 share everything 的思想，分布式事务相关是一个非常广泛的话题，这里就不展开讨论，Pinus 没有使用 2pc 或者 3pc 来实现分布式事务，使用的是 Best Efforts 1PC 的方式来实现了 JTA 接口。

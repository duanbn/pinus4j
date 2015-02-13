---
layout: default
---
# 事务机制

pinus的事务涉及到多个数据库资源，因此属于分布式事务的范畴，但是需要特别说明的是，分布式事务是一种share everything的思想，分布式事务相关是一个非常广泛的话题，这里就不展开讨论，pinus没有使用2pc或者3pc来实现分布式事务，使用的是Best Efforts 1PC的方式来实现了JTA接口。

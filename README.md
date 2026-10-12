Twitter Clone\
Written in Go with hybrid fan-out, replicated LSM storage (Pebble) and MySQL, and a Redis feed cache.

---

Twitter is read-heavy. Many more users are opening their feed (home page) than users are tweeting every second.\
Opening feeds can be slow across thousands of users when searching the log for all followees and obtaining their latest post.\
The solution to this is that when tweets are posted, they automatically get delivered to a feed mailbox such that users who want to access their feed can quickly open the mailbox. The tweets are also saved to an LSM sorted by poster ID followed by time of posting.

However, if a user has several followers, delivering to all their mailboxes can also be slow because there are so many mailboxes. Thus, celebrities should be handled differently: celebrity tweets are saved on the LSM, and upon opening a feed, they are searched up and merged with the cache containing regular followees' tweets. The cache exists on Redis, capped at the latest 20 tweets, and tweeting sends mail to Redis via 4 concurrent Kafka consumers. Each consumer communicates with Redis once per batch, which saves time instead of communicating once per mailbox; 4.7x faster per tweet (p99: 39.0ms to 8.3ms). If Redis is down, feeds fetch every followee's newest tweet from the LSM instead. 

Using 4 Kafka consumers sped up a burst of 1000 concurrent tweets by 4.2x (p99: 578ms vs 2.4s) compared to using only 1 consumer. Consumers are configured with `FetchMessage()`, which simply reads a task in the beginning. The task is marked as complete only at the end of the function with `CommitMessages()`. This is so that the task is retried if not completed due to the consumer dying mid-task. How do we handle idempotence? We allow a retried task to mail to all mailboxes. The mailboxes check for duplicates before they are displayed. Killing the program mid-delivery during 100,000 deliveries lost zero tweets, and the 16,674 duplicates from retries were all filtered before display.

Why LSM Trees? They are good at fast writes compared to a B-Tree when there is heavy concurrent writing load, which is a common situation for a platform like Twitter. B trees are faster when the goal is to read old data in long stretches. When it comes to reading newer, smaller amounts of data, both are equally fast because the data structures maintain a copy of the latest data in memory. This works out for LSM Trees because feed loading fetches a single latest celebrity tweet, which is more likely to exist in memory. This is why I think the LSM Tree is a better choice here than the B Tree. And when looking up a specific profile to fetch old tweets, the B Tree would be faster, but this use case does not occur very frequently. Even with an LSM Tree, all tweets by a user are sorted and next to each other, so profile loading is faster than a Kafka style unsorted log.

---

Finally, replicating the tweet database. The code was originally written for a single Pebble database and now runs three naive replicas. The idea is that if one database dies, the others act as backups. The nodes are leaderless, and writes are always accepted: a tweet is written to every node that's up, and after one node receives a write, the tweet gets sent to the mailboxes. Why does the mailbox come second? If the main program dies before delivering to the mailboxes, the tweet won't show up in feeds, but it still exists in storage. That's a better scenario than a mailbox showing a tweet that doesn't exist in storage. Nodes that missed a write are repaired by anti-entropy repair, which Twitter calls replica reconciliation, and runs in the background every 10 seconds. This is the core mechanism Twitter's Manhattan database uses to keep its replicas consistent.

Each node runs as its own program and talks to the main program over gRPC. Twitter uses its own RPC framework, Finagle. Originally, Pebble databases ran from inside my main program, but I had to move them to separate servers to be able to kill a single node to test repair.

---

p99 results with 10K users, 10 celebs, and 1.1M follows:

| | Normal tweet | Celeb tweet | Open feed |
|---|---|---|---|
| Hybrid | 48.41ms | 10.84ms | 12.39ms |
| Mail to everyone | 43.02ms | 2.99s | 0.99ms |
| Mail to no one | 11.53ms | 10.72ms | 97.42ms |

Hybrid fan-out makes celeb posts 276× faster than mailing everyone, which makes sense because you're not sending to thousands of mailboxes.\
And loading feeds is 8x faster than mailing no one, which makes sense because you can load cache instead of having to search through the LSM to pull every followee's tweets.

With one node dead, 10K tweets were successfully posted with zero lost.\
Anti-entropy repair repaired all 10K in 222ms, down from 67s after switching to in-memory comparisons and batched writes.

---

There is also a feature to sign up using an email as the key and a username as the value, and this data is stored across 3 MySQL servers. Two users should not be allowed to sign up with the same email. With a leaderless approach (we won't name names, but it might be Cassandra INSERT), a write just goes to whatever nodes are up, with nothing stopping a second signup from claiming the same email. Even if you add a check to prevent duplicates, if one node is behind due to a previous crash and it doesn't have the updated emails, a user may send a signup request and that behind node will end up accepting it, leading to the cluster containing the same email for two different accounts.

The fix is MySQL's group replication, which uses Paxos (*inferior* to Raft btw). It contains one primary or leader node that prevents duplicates before changes get applied elsewhere. Other nodes are not allowed to accept writes with the setting `super_read_only`, which is automatically handled by group replication. This primary is the master list and contains the truth. How? MySQL guarantees that before a new primary is picked, every surviving server contains every committed signup. Furthermore, a quorum is used to ensure that if a signup is written into a database, it must exist across a majority of nodes. The purpose of this is that if the primary dies, the cluster does not lose that node's data because other nodes contain the same data. Uber is an example of a company that uses this setup to enforce consistency in data. The cost is two-fold: 1) availability; signups only go through if a majority of nodes are up, and 2) latency; tests measured Paxos to be 2x slower than leaderless replication (p99: 84ms vs 40ms, 10ms simulated RTT).

---

To run:\
start Redis,\
then Kafka with topic `fanout` and 4 partitions,\
then start the three tweet nodes in separate terminals

```bash
go run ./tweetserver data/tweets0 :50051
go run ./tweetserver data/tweets1 :50052
go run ./tweetserver data/tweets2 :50053
```

then start a 3-node MySQL Group Replication cluster on ports 3306–3308.

Uncomment `Setup()` and `BenchmarkFanOut()` and run `go run .`.

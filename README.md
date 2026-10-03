Twitter Clone\
Written in Go with hybrid fan-out, replicated LSM storage (Pebble), and a Redis feed cache.

---

Twitter is read-heavy. Many more users are opening their feed (home page) than users are tweeting every second.\
Opening feeds can be slow across thousands of users when searching the log for all followees and obtaining their latest post.\
The solution to this is that when tweets are posted, they automatically get delivered to a feed mailbox such that users who want to access their feed can quickly open the mailbox. The tweets are also saved to an LSM sorted by poster ID followed by time of posting.

However, if a user has several followers, delivering to all their mailboxes can also be slow because there are so many mailboxes. Thus, celebrities should be handled differently: celebrity tweets are saved on the LSM, and upon opening a feed, they are searched up and merged with the cache containing regular followees' tweets.
The cache exists on Redis, capped at the latest 20 tweets, and tweeting prepares mail as one batch for Redis to deliver. Communicating with Redis once per batch saves time instead of communicating once per follower. If Redis is down, feeds fetch every followee's newest tweet from the LSM instead.

With this hybrid fan-out solution, most reads now happen through the cache, making the app write-heavy. This is why I chose to use an LSM for storing all tweets, because it's faster for writing compared to a B-tree, which is faster for read-heavy apps.

This program also allows you to load a specific user's profile. This directly pulls their tweets from the LSM. The LSM is useful because all tweets by a user are sorted and next to each other, but reading in general is not the most efficient. This is okay because I am assuming loading specific profiles happens less frequently than writing tweets in general.

---

Finally, replicating the tweet database. The code was originally written for a single Pebble database and now runs three naive replicas. The idea is that if one database dies, the others act as backups. The nodes are leaderless, and writes are always accepted: a tweet is written to every node that's up, and only after those writes finish does it get sent to the mailboxes. Why does the mailbox come second? If the main program dies before delivering to the mailboxes, the tweet won't show up in feeds, but it still exists in storage. That's a better scenario than a mailbox showing a tweet that doesn't exist in storage. Nodes that missed a write are repaired by replica reconciliation, which runs in the background every 10 seconds. This is the core mechanism Twitter's Manhattan database uses to keep its replicas consistent.

Each node runs as its own program and talks to the main program over gRPC. Twitter uses its own RPC framework, Finagle. Originally, Pebble databases ran from inside my main program, but I had to move them to separate servers to be able to kill a single node and test replication and reconciliation.

---

p99 results with 10K users, 10 celebs, and 1.1M follows:

| | Normal tweet | Celeb tweet | Open feed |
|---|---|---|---|
| Hybrid | 12.94 ms | 6.46 ms | 11.63 ms |
| Mail to everyone | 10.47 ms | 79.23 ms | 4.11 ms |
| Mail to no one | 6.29 ms | 6.67 ms | 95.14 ms |

Hybrid makes celeb posts 12× faster than mailing everyone, which makes sense because you're not sending to thousands of mailboxes.\
And loading feeds is 8x faster than mailing no one, which makes sense because you can load cache instead of having to search through the LSM to pull every followee's tweets.

With one node dead, 10000 tweets were successfully posted with zero lost.\
Reconciliation repaired all 10000 in 222ms, down from 67s after switching to in-memory comparisons and batched writes.

---

There is also a feature to sign up using an email as the key and a username as the value, and this data is stored across 3 more nodes. Two users should not be allowed to sign up with the same email. With a leaderless approach, such as a Cassandra INSERT-style write, a write just goes to whatever nodes are up, with nothing stopping a second signup from claiming the same email. Therefore, signups go through a separate 3-node etcd cluster, which uses Raft, a leader-based consensus algorithm. If only one node is up, the signup is rejected. If two nodes are up, the signup succeeds. If those two nodes die and the third one restarts, signups are rejected. If two nodes contain an email, one of them dies, and the third one starts, the node with the most recent entry becomes the leader, which in this case is the node with the email. The follower must then catch up on the email before more entries can be processed. Once synced, a duplicate email is rejected. In this way, Raft ensures that no duplicate emails exist in the dataset. In testing, a leaderless version that writes emails like a Cassandra INSERT gave the same email to two accounts, while etcd rejected the second signup. The cost is latency: tests measured Raft to be 1.7x slower than leaderless replication for signups (p99: 64ms vs 38ms, 10ms simulated RTT).

---

To run:\
start Redis, then start the three tweet nodes in separate terminals

```bash
go run ./tweetserver data/tweets0 :50051
go run ./tweetserver data/tweets1 :50052
go run ./tweetserver data/tweets2 :50053
```

Then run `go run .`.

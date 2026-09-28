A Twitter clone in Go using hybrid fan-out, with LSM trees (Pebble) for storage and Redis for cache.

Twitter is read-heavy. Many more users are opening their feed (home page) than users are tweeting every second.\
Opening feeds can be slow across thousands of users when searching the log for all followees and obtaining their latest post.\
The solution to this is that when tweets are posted, they automatically get delivered to a feed mailbox such that users who want to access their feed can quickly open the mailbox. The tweets are also saved to an LSM sorted by poster ID followed by time of posting.

However, if a user has several followers, delivering to all their mailboxes can also be slow because there are so many mailboxes. Thus, celebrities should be handled differently: celebrity tweets are saved on the LSM, and upon opening a feed, they are searched up and merged with the cache containing regular followees' tweets.
The cache exists on Redis, capped at the latest 20 tweets, and tweeting prepares mail as one batch for Redis to deliver. Communicating with Redis once per batch saves time instead of communicating once per follower. If Redis is down, feeds fetch every followee's newest tweet from the LSM instead.

With this hybrid fan-out solution, most reads now happen through the cache, making the app write-heavy. This is why I chose to use an LSM for storing all tweets, because it's faster for writing compared to a B-tree, which is faster for read-heavy apps.

p99 results with 10K users, 10 celebs, and 1.1M follows:

| | Normal tweet | Celeb tweet | Open feed |
|---|---|---|---|
| Hybrid | 7.5ms | 3.4ms | 1.93ms |
| Mail to everyone | 10.2ms | 133ms | 2.71ms |
| Mail to no one | 8.0ms | 7.6ms | 2.84ms |

Hybrid makes celeb posts 22× faster than mailing everyone, which makes sense because you're not sending to thousands of mailboxes.
And loading feeds is 2x faster than mailing no one, which makes sense because you can load cache instead of having to search through the LSM to pull every followee's tweets.

This program also allows you to load a specific user's profile. This directly pulls their tweets from the LSM. The LSM is useful because all tweets by a user are sorted and next to each other, but reading in general is not the most efficient. This is okay because I am assuming loading specific profiles happens less frequently than writing tweets in general.

To run: start Redis, uncomment `Setup()` in `main.go` and run `go run .` once, then comment it out, uncomment `Benchmark()`, and run `go run .` again.

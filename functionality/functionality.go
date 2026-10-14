package functionality

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"twitter_clone/connections"
	pb "twitter_clone/tweets"
)


func Signup(user, email string) bool {
	//give up after 2s so signups fail when 2+ nodes are down
	c, cancel := context.WithTimeout(connections.Ctx, 2*time.Second)
	defer cancel()
	//group replication sets a primary server that accepts writes while the others are read only
	//and get replicated through Paxos
	for _, db := range connections.Mysql {
		_, err := db.ExecContext(c, "INSERT INTO users (email, username) VALUES (?, ?)", email, user)
		if err == nil {
			return true
		}
	}
	return false
}

//just to see if its faster than leader-based
func SignupLeaderless(user, email string) bool {
	//blind write to all 3, no check, like a Cassandra INSERT
	done := make(chan bool, 3)
	for _, db := range connections.Tweets {
		go func(db pb.TweetsClient) {
			db.Put(connections.Ctx, &pb.PutRequest{Key: []byte("email:" + email), Value: []byte(user), Sync: true})
			done <- true
		}(db)
	}
	<-done
	return true
}

func Follow(user, target string) {
	if !UserExists(user) || !UserExists(target) {
		return
	}
	for _, db := range connections.Mysql {
		tx, _ := db.Begin()
		if _, err := tx.Exec("INSERT INTO follows (follower, followee) VALUES (?, ?)", user, target); err != nil {
			tx.Rollback()
			continue
		}
		tx.Exec("UPDATE users SET follower_count = follower_count + 1 WHERE username = ?", target)
		tx.Commit()
		return
	}
}

func Tweet(user, tweet string) {
	now := time.Now()

	if !UserExists(user) {
		return
	}

	results := make(chan error, 3)
	for _, db := range connections.Tweets {
		//write to all 3 dbs at once
		go func(db pb.TweetsClient) {
			_, err := db.Put(connections.Ctx, &pb.PutRequest{
				Key:   []byte(user + ":" + fmt.Sprintf("%020d", now.UnixNano())),
				Value: []byte(tweet), Sync: true,
			})
			results <- err
		}(db)
	}
	//leaderless: move on once any one server has the tweet, repair catches up the rest
	for i := 0; i < 3; i++ {
		if <-results == nil {
			break
		}
	}

	if !IsCeleb(user) {
		mail, _ := json.Marshal(connections.Mail{User: user, Tweet: tweet, Time: now.UnixNano()})
		connections.Producer.WriteMessages(connections.Ctx, kafka.Message{Value: mail})
	}
}

func Reconcile() {
	//using an in memory map to save repeated reads to the disk
	all := map[string][]byte{}
	var ramCopies [3]map[string]bool
	for i := range connections.Tweets {
		ramCopies[i] = map[string]bool{}
		stream, err := connections.Tweets[i].Scan(connections.Ctx, &pb.ScanRequest{})
		if err != nil {
			continue
		}
		for {
			kv, err := stream.Recv()
			if err != nil {
				break
			}
			all[string(kv.Key)] = kv.Value
			ramCopies[i][string(kv.Key)] = true
		}
	}
	//for each of the 3 copies we turned into maps in memory
	for i := range connections.Tweets {
		//if it's missing from the master map, batch it and add the batch to disk
		//batch saves repeated writes to the disk
		var missing []*pb.KV
		for key, value := range all {
			if !ramCopies[i][key] {
				missing = append(missing, &pb.KV{Key: []byte(key), Value: value})
			}
		}
		if len(missing) > 0 {
			connections.Tweets[i].PutBatch(connections.Ctx, &pb.PutBatchRequest{Kvs: missing})
		}
	}
}

//getters
func PrintTime(nanos int64) {
	fmt.Print(time.Unix(0, nanos).Format("15:04"), " ")
}

func UserExists(user string) bool {
	for _, db := range connections.Mysql {
		var n int
		if db.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", user).Scan(&n) == nil {
			return n > 0
		}
	}
	return false
}

func GetFollowerCount(user string) int {
	for _, db := range connections.Mysql {
		var count int
		if db.QueryRow("SELECT follower_count FROM users WHERE username = ?", user).Scan(&count) == nil {
			return count
		}
	}
	return 0
}

func IsCeleb(user string) bool {
	return GetFollowerCount(user) >= 1000
}

func GetFollowers(user string) []string {
	var followers []string
	for _, db := range connections.Mysql {
		rows, err := db.Query("SELECT follower FROM follows WHERE followee = ?", user)
		if err != nil {
			continue
		}
		for rows.Next() {
			var follower string
			rows.Scan(&follower)
			followers = append(followers, follower)
		}
		rows.Close()
		break
	}
	return followers
}

func GetFollowing(user string) []string {
	var followees []string
	for _, db := range connections.Mysql {
		rows, err := db.Query("SELECT followee FROM follows WHERE follower = ?", user)
		if err != nil {
			continue
		}
		for rows.Next() {
			var followee string
			rows.Scan(&followee)
			followees = append(followees, followee)
		}
		rows.Close()
		break
	}
	return followees
}

func GetProfile(user, target string) {
	if !UserExists(user) || !UserExists(target) {
		return
	}

	//naw screw 3 pointer merge sort. we just put everything into a map from all 3 db and sort
	all := map[string]string{}
	for _, db := range connections.Tweets {
		stream, err := db.Scan(connections.Ctx, &pb.ScanRequest{
			Lower: []byte(target + ":"),
			Upper: []byte(target + ";"),
		})
		if err != nil {
			continue
		}
		for {
			kv, err := stream.Recv()
			if err != nil {
				break
			}
			all[string(kv.Key)] = string(kv.Value)
		}
	}

	for _, k := range slices.Sorted(maps.Keys(all)) {
		nanos, _ := strconv.ParseInt(k[len(target+":"):], 10, 64)
		PrintTime(nanos)
		fmt.Println(all[k])
	}
}

func OpenMail(user string) {
	if !UserExists(user) {
		return
	}

	var mailbox []connections.Mail
	//hashset to remove duplicate mails in feed to handle a lack of idempotency on fan-out
	seen := map[string]bool{}
	texts, redisErr := connections.Redis.LRange(connections.Ctx, "mailbox:"+user, 0, -1).Result()
	for _, text := range texts {
		if seen[text] {
			continue
		}
		seen[text] = true
		var mail connections.Mail
		json.Unmarshal([]byte(text), &mail)
		mailbox = append(mailbox, mail)
	}

	var celebTweets []connections.Mail
	//for every followee
	for _, followee := range GetFollowing(user) {
		//collect the celeb's latest tweet as a mail struct
		//OR if redis is down collect each followee's latest tweet anyways
		if redisErr != nil || IsCeleb(followee) {
			var newest connections.Mail
			hasTweeted := false
			//if one db is still recovery we don't know which is latest
			//check all 3 dbs to determine latest
			for _, db := range connections.Tweets {
				reply, err := db.Last(connections.Ctx, &pb.ScanRequest{
					Lower: []byte(followee + ":"),
					Upper: []byte(followee + ";"),
				})
				if err == nil && reply.Found {
					nanos, _ := strconv.ParseInt(string(reply.Kv.Key[len(followee+":"):]), 10, 64)
					if !hasTweeted || nanos > newest.Time {
						newest = connections.Mail{User: followee, Tweet: string(reply.Kv.Value), Time: nanos}
						hasTweeted = true
					}
				}
			}
			if hasTweeted {
				celebTweets = append(celebTweets, newest)
			}
		}
	}
	sort.Slice(celebTweets, func(i, j int) bool { return celebTweets[i].Time < celebTweets[j].Time })

	//mergesort with two pointer
	i, j := 0, 0
	for i < len(mailbox) || j < len(celebTweets) {
		var mail connections.Mail
		if j == len(celebTweets) || (i < len(mailbox) && mailbox[i].Time < celebTweets[j].Time) {
			mail = mailbox[i]
			i++
		} else {
			mail = celebTweets[j]
			j++
		}
		PrintTime(mail.Time)
		fmt.Println(mail.User+":", mail.Tweet)
	}
}
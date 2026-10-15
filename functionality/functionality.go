package functionality

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"
	"strings"

	"github.com/segmentio/kafka-go"
	"twitter_clone/connections"
	pb "twitter_clone/tweets"
)


var CelebThreshold = 1000


func SignupBatch(users, emails []string) bool {
	//if 2+ nodes are down, insert will timeout as it should.
	c, cancel := context.WithTimeout(connections.Ctx, 2*time.Second)
	defer cancel()

	var values []any
	for i := range users {
		values = append(values, users[i], emails[i])
	}
	query := "INSERT INTO users (username, email) VALUES" + strings.Repeat(" (?, ?),", len(users))

	_, err := connections.Mysql.ExecContext(c, strings.TrimSuffix(query, ","), values...)
	return err == nil
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

func FollowBatch(users, targets []string) {
	insert := "INSERT INTO follows (user, target) VALUES" + strings.Repeat(" (?, ?),", len(users))
	update := "UPDATE users SET follower_count = follower_count + 1 WHERE username IN (" + strings.Repeat("?, ", len(targets))
	var pairs, followed []any
	for i := range users {
		pairs = append(pairs, users[i], targets[i])
		followed = append(followed, targets[i])
	}
	tx, _ := connections.Mysql.Begin()
	if _, err := tx.Exec(strings.TrimSuffix(insert, ","), pairs...); err != nil {
		tx.Rollback()
		return
	}
	tx.Exec(strings.TrimSuffix(update, ", ")+")", followed...)
	tx.Commit()
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

func Repair() {
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
	var n int
	connections.Mysql.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", user).Scan(&n)
	return n > 0
}

func GetFollowerCount(user string) int {
	var count int
	connections.Mysql.QueryRow("SELECT follower_count FROM users WHERE username = ?", user).Scan(&count)
	return count
}

func IsCeleb(user string) bool {
	return GetFollowerCount(user) >= CelebThreshold
}

func GetUsers() []string {
	var users []string
	rows, _ := connections.Mysql.Query("SELECT username FROM users")
	for rows.Next() {
		var user string
		rows.Scan(&user)
		users = append(users, user)
	}
	rows.Close()
	return users
}

func GetCelebs() []string {
	var celebs []string
	rows, _ := connections.Mysql.Query("SELECT username FROM users WHERE follower_count >= ?", CelebThreshold)
	for rows.Next() {
		var celeb string
		rows.Scan(&celeb)
		celebs = append(celebs, celeb)
	}
	rows.Close()
	return celebs
}

func GetRandos() []string {
	var randos []string
	rows, _ := connections.Mysql.Query("SELECT username FROM users WHERE follower_count < ?", CelebThreshold)
	for rows.Next() {
		var rando string
		rows.Scan(&rando)
		randos = append(randos, rando)
	}
	rows.Close()
	return randos
}

func GetFollowers(user string) []string {
	var followers []string
	rows, _ := connections.Mysql.Query("SELECT user FROM follows WHERE target = ?", user)
	for rows.Next() {
		var follower string
		rows.Scan(&follower)
		followers = append(followers, follower)
	}
	rows.Close()
	return followers
}

func GetFollowing(user string) []string {
	var followees []string
	rows, _ := connections.Mysql.Query("SELECT target FROM follows WHERE user = ?", user)
	for rows.Next() {
		var followee string
		rows.Scan(&followee)
		followees = append(followees, followee)
	}
	rows.Close()
	return followees
}

func GetProfile(target string) []string {
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

	var profile []string
	for _, k := range slices.Backward(slices.Sorted(maps.Keys(all))) {
		nanos, _ := strconv.ParseInt(k[len(target+":"):], 10, 64)
		profile = append(profile, time.Unix(0, nanos).Format("15:04")+" "+all[k])
	}
	
	return profile
}

func OpenMail(user string) []string {
	if !UserExists(user) {
		return nil
	}

	//keyed by time then user
	//map automatically deduplicates non-idempotent mail
	all := map[string]connections.Mail{}
	texts, redisErr := connections.Redis.LRange(connections.Ctx, "mailbox:"+user, 0, -1).Result()
	for _, text := range texts {
		var mail connections.Mail
		json.Unmarshal([]byte(text), &mail)
		all[fmt.Sprintf("%020d:%s", mail.Time, mail.User)] = mail
	}

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
				all[fmt.Sprintf("%020d:%s", newest.Time, newest.User)] = newest
			}
		}
	}

	var feed []string
	for _, k := range slices.Backward(slices.Sorted(maps.Keys(all))) {
		if len(feed) == 20 {
			break
		}
		mail := all[k]
		feed = append(feed, time.Unix(0, mail.Time).Format("15:04")+" "+mail.User+": "+mail.Tweet)
	}
	return feed
}
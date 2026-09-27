package main

import "fmt"
import "time"
import "os"
import "strconv"
import "sort"
import "math/rand"
import "context"
import "encoding/json"
import "github.com/redis/go-redis/v9"
import "github.com/cockroachdb/pebble"

var writeMode = pebble.Sync

var users *pebble.DB
var follows *pebble.DB
var tweets *pebble.DB

type Mail struct {
	User	string
	Tweet	string
	Time	int64
}
//don't spam retry when redis is clearly down
var rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379", MaxRetries: -1})
var ctx = context.Background()

var celeb_min = 1000


func CreateUser(user string) {
	users.Set([]byte(user), []byte("0"), writeMode)
}

func Follow(user, target string) {
	if(!UserExists(user) || !UserExists(target) || IsFollowing(user, target)){
		return;
	}
	follows.Set([]byte("follower:"+target+":"+user), nil, writeMode)
	follows.Set([]byte("following:"+user+":"+target), nil, writeMode)
	users.Set([]byte(target), []byte(strconv.Itoa(GetFollowerCount(target)+1)), writeMode)
}

func Unfollow(user, target string) {
	if(!IsFollowing(user, target)){
		return;
	}
	follows.Delete([]byte("follower:"+target+":"+user), writeMode)
	follows.Delete([]byte("following:"+user+":"+target), writeMode)
	users.Set([]byte(target), []byte(strconv.Itoa(GetFollowerCount(target)-1)), writeMode)
}

func Tweet(user, tweet string) {
	if(!UserExists(user)){
		return;
	}

	now := time.Now()
	if GetFollowerCount(user) < celeb_min {
		iter, _ := follows.NewIter(&pebble.IterOptions{
			LowerBound: []byte("follower:" + user + ":"),
			UpperBound: []byte("follower:" + user + ";"),
		})
		//talking to redis once instead of multiple times for each delivery to save time
		pipe := rdb.Pipeline()
		for iter.First(); iter.Valid(); iter.Next() {
			follower := string(iter.Key()[len("follower:"+user+":"):])
			mail, _ := json.Marshal(Mail{user, tweet, now.UnixNano()})
			pipe.RPush(ctx, "mailbox:"+follower, mail)
			//don't want to much cache
			pipe.LTrim(ctx, "mailbox:"+follower, -20, -1)
		}
		pipe.Exec(ctx)
		iter.Close()
	}
	tweets.Set([]byte(user+":"+fmt.Sprintf("%020d", now.UnixNano())), []byte(tweet), writeMode)
}

//getters
func PrintTime(nanos int64) {
	fmt.Print(time.Unix(0, nanos).Format("15:04"), " ")
}

func UserExists(user string) bool {
	_, closer, err := users.Get([]byte(user))
	if err != nil {
		return false
	}
	closer.Close()
	return true
}

func GetFollowerCount(user string) int {
	value, closer, err := users.Get([]byte(user))
	if err != nil {
		return 0
	}
	count, _ := strconv.Atoi(string(value))
	closer.Close()
	return count
}

func IsFollowing(user, target string) bool {
	_, closer, err := follows.Get([]byte("following:" + user + ":" + target))
	if err != nil {
		return false
	}
	closer.Close()
	return true
}

func GetFollowers(user string) {
	iter, _ := follows.NewIter(&pebble.IterOptions{
		LowerBound: []byte("follower:" + user + ":"),
		UpperBound: []byte("follower:" + user + ";"),
	})
	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Println(string(iter.Key()[len("follower:"+user+":"):]))
	}
	iter.Close()
}

func GetFollowing(user string) {
	iter, _ := follows.NewIter(&pebble.IterOptions{
		LowerBound: []byte("following:" + user + ":"),
		UpperBound: []byte("following:" + user + ";"),
	})
	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Println(string(iter.Key()[len("following:"+user+":"):]))
	}
	iter.Close()
}

func GetProfile(user string) {
	iter, _ := tweets.NewIter(&pebble.IterOptions{
		LowerBound: []byte(user + ":"),
		UpperBound: []byte(user + ";"),
	})
	for iter.First(); iter.Valid(); iter.Next() {
		nanos, _ := strconv.ParseInt(string(iter.Key()[len(user+":"):]), 10, 64)
		PrintTime(nanos)
		fmt.Println(string(iter.Value()))
	}
	iter.Close()
}

func OpenMail(user string) {
	var mailbox []Mail
	texts, redisErr := rdb.LRange(ctx, "mailbox:"+user, 0, -1).Result()
	for _, text := range texts {
		var mail Mail
		json.Unmarshal([]byte(text), &mail)
		mailbox = append(mailbox, mail)
	}

	var celebTweets []Mail
	iter, _ := follows.NewIter(&pebble.IterOptions{
		LowerBound: []byte("following:" + user + ":"),
		UpperBound: []byte("following:" + user + ";"),
	})
	//for every followee
	for iter.First(); iter.Valid(); iter.Next() {
		followee := string(iter.Key()[len("following:"+user+":"):])
		//collect the celeb's tweets as a mail struct
		//OR if redis is down collect all followee's tweets anyways
		if redisErr != nil || GetFollowerCount(followee) >= celeb_min {
			tweetIter, _ := tweets.NewIter(&pebble.IterOptions{
				LowerBound: []byte(followee + ":"),
				UpperBound: []byte(followee + ";"),
			})
			//if a tweet of theirs exists
			if tweetIter.Last() {
				nanos, _ := strconv.ParseInt(string(tweetIter.Key()[len(followee+":"):]), 10, 64)
				celebTweets = append(celebTweets, Mail{followee, string(tweetIter.Value()), nanos})
			}
			tweetIter.Close()
		}
	}
	iter.Close()
	sort.Slice(celebTweets, func(i, j int) bool { return celebTweets[i].Time < celebTweets[j].Time })

	//mergesort with two pointer
	i, j := 0, 0
	for i < len(mailbox) || j < len(celebTweets) {
		var mail Mail
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

//creates 10k users and 10 celebs. each user follows 100 randos and all celebs.
//everyone tweets once so feeds have real tweets to fetch
func Setup() {
	writeMode = pebble.NoSync

	for i := 0; i < 10000; i++ {
		CreateUser("user" + strconv.Itoa(i))
	}
	for i := 0; i < 10; i++ {
		CreateUser("celeb" + strconv.Itoa(i))
	}

	for i := 0; i < 10000; i++ {
		user := "user" + strconv.Itoa(i)
		for k := 0; k < 100; k++ {
			target := "user" + strconv.Itoa(rand.Intn(10000))
			if target != user {
				Follow(user, target)
			}
		}
		for c := 0; c < 10; c++ {
			Follow(user, "celeb"+strconv.Itoa(c))
		}
	}

	for i := 0; i < 10000; i++ {
		Tweet("user"+strconv.Itoa(i), "hello")
	}
	for i := 0; i < 10; i++ {
		Tweet("celeb"+strconv.Itoa(i), "hello")
	}

	users.Flush()
	follows.Flush()
	tweets.Flush()
	writeMode = pebble.Sync
}

func Benchmark() {
	realStdout := os.Stdout
	devNull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)

	names := []string{"hybrid", "mail to everyone", "mail to no one"}
	celeb_mins := []int{1000, 1 << 14, 0}

	for s := 0; s < 3; s++ {
		celeb_min = celeb_mins[s]
		if names[s] == "mail to no one" {
			rdb.FlushAll(ctx)
		}

		start := time.Now()
		for i := 0; i < 1000; i++ {
			Tweet("user"+strconv.Itoa(i), "hello")
		}
		fmt.Println(names[s], "normal tweet:", time.Since(start)/1000)

		start = time.Now()
		for i := 0; i < 10; i++ {
			Tweet("celeb"+strconv.Itoa(i), "hello")
		}
		fmt.Println(names[s], "celeb tweet:", time.Since(start)/10)

		os.Stdout = devNull
		start = time.Now()
		for i := 0; i < 1000; i++ {
			OpenMail("user" + strconv.Itoa(i))
		}
		os.Stdout = realStdout
		fmt.Println(names[s], "open feed:", time.Since(start)/1000)
	}
}

func main() {
	users, _ = pebble.Open("data/users", &pebble.Options{})
	follows, _ = pebble.Open("data/follows", &pebble.Options{})
	tweets, _ = pebble.Open("data/tweets", &pebble.Options{})

	//defer closes when program is finished
	defer users.Close()
	defer follows.Close()
	defer tweets.Close()

	//Setup()
	//Benchmark()
	OpenMail("user0")
}
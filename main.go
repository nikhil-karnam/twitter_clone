package main

import "fmt"
import "time"
import "os"
import "strconv"
import "sort"
import "math/rand"
import "maps"
import "slices"
import "context"
import "encoding/json"
import "github.com/redis/go-redis/v9"
import "github.com/cockroachdb/pebble"
import "google.golang.org/grpc"
import "google.golang.org/grpc/credentials/insecure"
import pb "twitter_clone/tweets"

var writeMode = pebble.Sync

var users *pebble.DB
var follows *pebble.DB
var tweets [3]pb.TweetsClient
var tweetAddrs = [3]string{"localhost:50051", "localhost:50052", "localhost:50053"}

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
	
	results := make(chan error, 3)
	for _, db := range tweets {
		go func(db pb.TweetsClient) {
			_, err := db.Put(ctx, &pb.PutRequest{
				Key: []byte(user+":"+fmt.Sprintf("%020d", now.UnixNano())), 
				Value: []byte(tweet), Sync: writeMode == pebble.Sync,
			})
			results <- err
		}(db)
	}
	passes := 0
	for i := 0; i < 3; i++ {
		//waits until result arrives
		err := <-results
		if err == nil {
			passes++
		}
	}
	//quorum failed, don't mail
	if passes < 2{
		return
	}

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
}

func Reconcile() {
	//using an in memory map to save repeated reads to the disk
	all := map[string][]byte{}
	var ram_copies [3]map[string]bool
	for i := range tweets {
		ram_copies[i] = map[string]bool{}
		stream, err := tweets[i].Scan(ctx, &pb.ScanRequest{})
		if err != nil {
			continue
		}
		for {
			kv, err := stream.Recv()
			if err != nil {
				break
			}
			all[string(kv.Key)] = kv.Value
			ram_copies[i][string(kv.Key)] = true
		}
	}
	//for each of the 3 copies we turned into maps in memory
	for i := range tweets {
		//if it's missing from the master map, batch it and add the batch to disk
		//batch saves repeated writes to the disk
		var missing []*pb.KV
		for key, value := range all {
			if !ram_copies[i][key] {
				missing = append(missing, &pb.KV{Key: []byte(key), Value: value})
			}
		}
		if len(missing) > 0 {
			tweets[i].PutBatch(ctx, &pb.PutBatchRequest{Kvs: missing})
		}
	}
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

func GetProfile(user, target string) {
	//naw screw 3 pointer merge sort. we just put everything into a map from all 3 db and sort
	all := map[string]string{}
	for _, db := range tweets {
		stream, err := db.Scan(ctx, &pb.ScanRequest{
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
	//merging the feed with the profile in case there's inconsistency
	texts, _ := rdb.LRange(ctx, "mailbox:"+user, 0, -1).Result()
	for _, text := range texts {
		var mail Mail
		json.Unmarshal([]byte(text), &mail)
		if mail.User == target {
			all[target+":"+fmt.Sprintf("%020d", mail.Time)] = mail.Tweet
		}
	}

	for _, k := range slices.Sorted(maps.Keys(all)) {
		nanos, _ := strconv.ParseInt(k[len(target+":"):], 10, 64)
		PrintTime(nanos)
		fmt.Println(all[k])
	}
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
		//collect the celeb's latest tweet as a mail struct
		//OR if redis is down collect each followee's latest tweet anyways
		if redisErr != nil || GetFollowerCount(followee) >= celeb_min {
			var newest Mail
			has_tweeted := false
			//if one db is still recovery we don't know which is latest
			//check all 3 dbs to determine latest
			for _, db := range tweets {
				reply, err := db.Last(ctx, &pb.ScanRequest{
					Lower: []byte(followee + ":"),
					Upper: []byte(followee + ";"),
				})
				if err == nil && reply.Found {
					nanos, _ := strconv.ParseInt(string(reply.Kv.Key[len(followee+":"):]), 10, 64)
					if !has_tweeted || nanos > newest.Time {
						newest = Mail{followee, string(reply.Kv.Value), nanos}
						has_tweeted = true
					}
				}
			}
			if has_tweeted {
				celebTweets = append(celebTweets, newest)
			}
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

func Setup() {
	//speed up process
	writeMode = pebble.NoSync

	//creates 10k users and 10 celebs.
	for i := 0; i < 10000; i++ {
		CreateUser("user" + strconv.Itoa(i))
	}
	for i := 0; i < 10; i++ {
		CreateUser("celeb" + strconv.Itoa(i))
	}

	//each user follows 100 randos + all celebs = 110 follows
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

	users.Flush()
	follows.Flush()
	for _, db := range tweets {
		db.Flush(ctx, &pb.FlushRequest{})
	}
	writeMode = pebble.Sync
}

func Benchmark() {
	realStdout := os.Stdout
	devNull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)

	names := []string{"hybrid", "mail to everyone", "mail to no one"}
	celeb_mins := []int{1000, 1 << 14, 0}

	for s := 0; s < 3; s++ {
		if names[s] == "mail to no one" {
			rdb.FlushAll(ctx)
		}
		
		celeb_min = celeb_mins[s]

		fmt.Println(names[s], "normal tweet:", p99(1000, func(i int) { Tweet("user"+strconv.Itoa(rand.Intn(10000)), "hello") }))
		
		fmt.Println(names[s], "celeb tweet:", p99(100, func(i int) { Tweet("celeb"+strconv.Itoa(i%10), "hello") }))
		
		os.Stdout = devNull
		t := p99(1000, func(i int) { OpenMail("user" + strconv.Itoa(rand.Intn(10000))) })
		os.Stdout = realStdout
		fmt.Println(names[s], "open feed:", t)
	}
}

func p99(n int, f func(i int)) time.Duration {
	times := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		f(i)
		times[i] = time.Since(start)
	}
	sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
	return times[(n*99+99)/100-1]
}


func main() {
	users, _ = pebble.Open("data/users", &pebble.Options{})
	follows, _ = pebble.Open("data/follows", &pebble.Options{})
	for i, addr := range tweetAddrs {
		conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		//defer closes when program is finished
		defer conn.Close()
		tweets[i] = pb.NewTweetsClient(conn)
	}
	defer users.Close()
	defer follows.Close()

	//runs repairs in the background
	/*go func() {
		for {
			Reconcile()
			time.Sleep(10 * time.Second)
		}
	}()*/

	Setup()
	Benchmark()
}
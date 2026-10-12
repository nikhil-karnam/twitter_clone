package main

import "fmt"
import "time"
import "os"
import "strconv"
import "sort"
import "math/rand"
import "maps"
import "slices"
import "sync"
import "context"
import "encoding/json"
import "github.com/redis/go-redis/v9"
import "github.com/cockroachdb/pebble"
import "google.golang.org/grpc"
import "google.golang.org/grpc/credentials/insecure"
import pb "twitter_clone/tweets"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "github.com/segmentio/kafka-go"

var writeMode = pebble.Sync

var users *pebble.DB
var follows *pebble.DB
var tweets [3]pb.TweetsClient
var mysqlDBs [3]*sql.DB
var mysqlAddrs = [3]string{"127.0.0.1:3306", "127.0.0.1:3307", "127.0.0.1:3308"}
var tweetAddrs = [3]string{"localhost:50051", "localhost:50052", "localhost:50053"}

var producer = &kafka.Writer{
	Addr:         kafka.TCP("localhost:9092"),
	Topic:        "fanout",
	//kafka waits to send in batches, default is 1 second wait. change this.
	BatchTimeout: 10 * time.Millisecond,
}

//lists to store runtime per function, benchmark converts these to p99
var pebbleTimes, mailTimes, feedTimes, signupTimes []time.Duration
//to handle race condition
var mailLock sync.Mutex

type Mail struct {
	User	string
	Tweet	string
	Time	int64
}
//don't spam retry when redis is clearly down
var rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379", MaxRetries: -1})
var ctx = context.Background()

var celebMin = 1000


func CreateUser(user string) {
	users.Set([]byte(user), []byte("0"), writeMode)
}

func Signup(user, email string) bool {
	start := time.Now()

	//give up after 2s so signups fail when 2+ nodes are down
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	//group replication sets a primary server that accepts writes while the others are read only
	//and get replicated through Paxos
	for _, db := range mysqlDBs {
		_, err := db.ExecContext(c, "INSERT INTO users (email, username) VALUES (?, ?)", email, user)
		if err == nil {
			CreateUser(user)

			signupTimes = append(signupTimes, time.Since(start))
			return true
		}
	}

	signupTimes = append(signupTimes, time.Since(start))
	return false
}

//just to see if its faster than raft
func SignupLeaderless(user, email string) bool {
	start := time.Now()

	//blind write to all 3, no check, like a Cassandra INSERT
	done := make(chan bool, 3)
	for _, db := range tweets {
		go func(db pb.TweetsClient) {
			db.Put(ctx, &pb.PutRequest{Key: []byte("email:" + email), Value: []byte(user), Sync: true})
			done <- true
		}(db)
	}
	<-done
	CreateUser(user)

	signupTimes = append(signupTimes, time.Since(start))
	return true
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
	now := time.Now()

	if(!UserExists(user)){
		return;
	}

	results := make(chan error, 3)
	for _, db := range tweets {
		//write to all 3 dbs at once
		go func(db pb.TweetsClient) {
			_, err := db.Put(ctx, &pb.PutRequest{
				Key: []byte(user+":"+fmt.Sprintf("%020d", now.UnixNano())), 
				Value: []byte(tweet), Sync: writeMode == pebble.Sync,
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

	mailLock.Lock()
	pebbleTimes = append(pebbleTimes, time.Since(now))
	mailLock.Unlock()

	if !IsCeleb(user) {
		mail, _ := json.Marshal(Mail{user, tweet, now.UnixNano()})
		producer.WriteMessages(ctx, kafka.Message{Value: mail})
	}
}

//reads tweets from kafka and delivers them to mailboxes
func Consumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "fanout",
		GroupID: "fanout-consumers",
	})
	defer reader.Close()

	for {
		//fetch works on the next mail without moving the kafka bookmark 
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return
		}

		var mail Mail
		//unmarshal just to find the followers to send the mail to
		json.Unmarshal(msg.Value, &mail)
		pipe := rdb.Pipeline()
		iter, _ := follows.NewIter(&pebble.IterOptions{
			LowerBound: []byte("follower:" + mail.User + ":"),
			UpperBound: []byte("follower:" + mail.User + ";"),
		})

		for iter.First(); iter.Valid(); iter.Next() {
			follower := string(iter.Key()[len("follower:"+mail.User+":"):])
			pipe.RPush(ctx, "mailbox:"+follower, msg.Value)
			pipe.LTrim(ctx, "mailbox:"+follower, -20, -1)
		}
		iter.Close()

		if _, err := pipe.Exec(ctx); err != nil {
			return
		}

		mailLock.Lock()
		mailTimes = append(mailTimes, time.Since(time.Unix(0, mail.Time)))
		mailLock.Unlock()

		//only move the bookmark once every mailbox got the tweet
		//this ensures a retry will occur if a worker died without completing
		reader.CommitMessages(ctx, msg)
	}
}

func Reconcile() {
	//using an in memory map to save repeated reads to the disk
	all := map[string][]byte{}
	var ramCopies [3]map[string]bool
	for i := range tweets {
		ramCopies[i] = map[string]bool{}
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
			ramCopies[i][string(kv.Key)] = true
		}
	}
	//for each of the 3 copies we turned into maps in memory
	for i := range tweets {
		//if it's missing from the master map, batch it and add the batch to disk
		//batch saves repeated writes to the disk
		var missing []*pb.KV
		for key, value := range all {
			if !ramCopies[i][key] {
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

func IsCeleb(user string) bool {
	return GetFollowerCount(user) >= celebMin
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
	if(!UserExists(user) || !UserExists(target)){
		return;
	}

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

	for _, k := range slices.Sorted(maps.Keys(all)) {
		nanos, _ := strconv.ParseInt(k[len(target+":"):], 10, 64)
		PrintTime(nanos)
		fmt.Println(all[k])
	}
}

func OpenMail(user string) {
	start := time.Now()

	if(!UserExists(user)){
		return;
	}

	var mailbox []Mail
	//hashset to remove duplicate mails in feed to handle a lack of idempotency on fan-out
	seen := map[string]bool{}
	texts, redisErr := rdb.LRange(ctx, "mailbox:"+user, 0, -1).Result()
	for _, text := range texts {
		if seen[text] {
			continue
		}
		seen[text] = true
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
		if redisErr != nil || IsCeleb(followee) {
			var newest Mail
			hasTweeted := false
			//if one db is still recovery we don't know which is latest
			//check all 3 dbs to determine latest
			for _, db := range tweets {
				reply, err := db.Last(ctx, &pb.ScanRequest{
					Lower: []byte(followee + ":"),
					Upper: []byte(followee + ";"),
				})
				if err == nil && reply.Found {
					nanos, _ := strconv.ParseInt(string(reply.Kv.Key[len(followee+":"):]), 10, 64)
					if !hasTweeted || nanos > newest.Time {
						newest = Mail{followee, string(reply.Kv.Value), nanos}
						hasTweeted = true
					}
				}
			}
			if hasTweeted {
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
	
	feedTimes = append(feedTimes, time.Since(start))
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
	writeMode = pebble.Sync
}

func BenchmarkFanOut() {
	rdb.FlushAll(ctx)

	realStdout := os.Stdout
	devNull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)

	normalTweet := func(i int) { Tweet("user"+strconv.Itoa(rand.Intn(10000)), "hello") }
	celebTweet := func(i int) { Tweet("celeb"+strconv.Itoa(i%10), "hello") }
	openFeed := func(i int) { OpenMail("user" + strconv.Itoa(rand.Intn(10000))) }

	celebMin = 1000
	fmt.Println("hybrid normal delivery:", p99(1000, normalTweet, &mailTimes))
	fmt.Println("hybrid celeb db:", p99(100, celebTweet, &pebbleTimes))
	os.Stdout = devNull
	t := p99(1000, openFeed, &feedTimes)
	os.Stdout = realStdout
	fmt.Println("hybrid open feed:", t)

	celebMin = 1 << 14
	fmt.Println("mail to everyone normal delivery:", p99(1000, normalTweet, &mailTimes))
	fmt.Println("mail to everyone celeb delivery:", p99(100, celebTweet, &mailTimes))
	os.Stdout = devNull
	t = p99(1000, openFeed, &feedTimes)
	os.Stdout = realStdout
	fmt.Println("mail to everyone open feed:", t)

	rdb.FlushAll(ctx)
	celebMin = 0
	fmt.Println("mail to no one normal db:", p99(1000, normalTweet, &pebbleTimes))
	fmt.Println("mail to no one celeb db:", p99(100, celebTweet, &pebbleTimes))
	os.Stdout = devNull
	t = p99(1000, openFeed, &feedTimes)
	os.Stdout = realStdout
	fmt.Println("mail to no one open feed:", t)
}

func BenchmarkSignups() {
	//using a time tag generates new emails each run so it doesn't collide with previous runs
	run := strconv.FormatInt(time.Now().UnixNano(), 10)
	fmt.Println("paxos signup:", p99(1000, func(i int) { Signup("new"+strconv.Itoa(i), run+"-"+strconv.Itoa(i)+"@x.com") }, &signupTimes))
	fmt.Println("leaderless signup:", p99(1000, func(i int) { SignupLeaderless("new"+strconv.Itoa(i), run+"-"+strconv.Itoa(i)+"@x.com") }, &signupTimes))
}

//runs f n times, waits until all n runtimes are recorded, then returns the p99
func p99(n int, f func(i int), times *[]time.Duration) time.Duration {
	//clear leftovers, since tweets also fill lists this run isn't reading
	mailLock.Lock()
	*times = nil
	mailLock.Unlock()

	for i := 0; i < n; i++ {
		f(i)
	}

	//consumers record mail times in the background, so wait for all n
	for {
		mailLock.Lock()
		done := len(*times) >= n
		mailLock.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	sort.Slice(*times, func(a, b int) bool { return (*times)[a] < (*times)[b] })
	return (*times)[(n*99+99)/100-1]
}

//celebs tweet with fan-out on, then the program dies mid-delivery
//run main once with this, run main again with this commented out so consumers finish tasks.
//then count unique tweets per mailbox in cli, each should hold 10.
func CrashTest() {
	rdb.FlushAll(ctx)

	celebMin = 1 << 14
	for c := 0; c < 10; c++ {
		Tweet("celeb"+strconv.Itoa(c), "test"+strconv.Itoa(c))
	}
	time.Sleep(100 * time.Millisecond)
	os.Exit(1)
}

//to test if kafka 4 consumers speed up a burst of 1000 users tweeting at once,
//where a backlog forms and consumers have work to split
func BenchmarkBurst() {
	rdb.FlushAll(ctx)

	celebMin = 1000
	burstTweet := func(i int) { go Tweet("user"+strconv.Itoa(rand.Intn(10000)), "hello") }
	fmt.Println("burst normal delivery:", p99(1000, burstTweet, &mailTimes))
}


func main() {
	users, _ = pebble.Open("data/users", &pebble.Options{})
	//defer closes when program is finished
	defer users.Close()
	
	follows, _ = pebble.Open("data/follows", &pebble.Options{})
	defer follows.Close()
	
	for i, addr := range mysqlAddrs {
		mysqlDBs[i], _ = sql.Open("mysql", "root:root@tcp("+addr+")/twitter")
		defer mysqlDBs[i].Close()
	}
	
	for i, addr := range tweetAddrs {
		conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		tweets[i] = pb.NewTweetsClient(conn)
		defer conn.Close()
	}

	//change to i < 1 and run benchmarkBurst() to test 1 kafka consumer bottleneck
	for i := 0; i < 4; i++ {
		go Consumer()
	}
	//consumers need a few seconds to start, start benchmarking timers only after this.
	time.Sleep(10 * time.Second)

	//runs repairs in the background
	/*go func() {
		for {
			Reconcile()
			time.Sleep(10 * time.Second)
		}
	}()*/

	//Setup()
	//BenchmarkFanOut()
	//BenchmarkSignups()
	//CrashTest()
	//BenchmarkBurst()
} 
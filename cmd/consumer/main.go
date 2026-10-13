package main

import (
	"encoding/json"
	"os"
	"io"
	"strings"

	"github.com/segmentio/kafka-go"
	"twitter_clone/connections"
	pb "twitter_clone/tweets"
)

//reads tweets from kafka and delivers them to mailboxes
func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(os.Getenv("KAFKA_ADDRS"), ","),
		Topic:   "fanout",
		GroupID: "fanout-consumers",
	})
	defer reader.Close()

	for {
		//fetch works on the next mail without moving the kafka checkpoint
		msg, _ := reader.FetchMessage(connections.Ctx)

		var mail connections.Mail
		//unmarshal just to find the followers to send the mail to
		json.Unmarshal(msg.Value, &mail)
		pipe := connections.Redis.Pipeline()
		stream, _ := connections.Follows.Scan(connections.Ctx, &pb.ScanRequest{
			Lower: []byte("follower:" + mail.User + ":"),
			Upper: []byte("follower:" + mail.User + ";"),
		})

		for {
			kv, err := stream.Recv()
			if err == io.EOF {
				break
			}
			follower := string(kv.Key[len("follower:"+mail.User+":"):])
			pipe.RPush(connections.Ctx, "mailbox:"+follower, msg.Value)
			pipe.LTrim(connections.Ctx, "mailbox:"+follower, -20, -1)
		}
		
		//if redis is down, take the consumer down with it!
		if _, err := pipe.Exec(connections.Ctx); err != nil {
			return
		}
		//only move the checkpoint once every mailbox got the tweet
		reader.CommitMessages(connections.Ctx, msg)
	}
}
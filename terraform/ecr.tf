resource "aws_ecr_repository" "pebble_server" {
  name         = "pebble-server"
  force_delete = true
}

resource "aws_ecr_repository" "kafka_consumer" {
  name         = "kafka-consumer"
  force_delete = true
}

resource "aws_ecr_repository" "api" {
  name         = "api"
  force_delete = true
}

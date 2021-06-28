terraform {
  required_version = ">= 0.13"
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
  profile = "your-aws-credential-profile"
}

resource "aws_sqs_queue" "queue" {
  name                      = "example-queue"
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 20
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.deadletter.arn
    maxReceiveCount     = 100
  })
}

resource "aws_sqs_queue" "deadletter" {
  name                      = "example-deadletter-queue"
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 20
}

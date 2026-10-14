resource "aws_iam_role" "node" {
  name = "twitter-clone-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "node" {
  function_name    = "twitter-clone-node"
  role             = aws_iam_role.node.arn
  runtime          = "nodejs24.x"
  handler          = "index.handler"
  filename         = "node.zip"
  source_code_hash = filebase64sha256("node.zip")
  timeout          = 900
  lifecycle {
    ignore_changes = [environment]
  }
}

resource "aws_lambda_function_url" "node" {
  function_name      = aws_lambda_function.node.function_name
  authorization_type = "NONE"
  cors {
    allow_origins = ["http://${aws_s3_bucket_website_configuration.site.website_endpoint}"]
  }
}

output "lambda_url" {
  value = aws_lambda_function_url.node.function_url
}

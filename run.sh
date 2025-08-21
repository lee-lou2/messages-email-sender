docker stop messages-email-sender;docker rm messages-email-sender;docker run --env-file .env --name=messages-email-sender -d leelou2/messages-email-sender:0.0.1

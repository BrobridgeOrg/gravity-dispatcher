FROM golang:1.24-alpine3.20 AS builder

ENV GOBIN=/

WORKDIR /
COPY . .

RUN apk add --update build-base git upx && apk upgrade --available
RUN go build -ldflags="-s -w" -o /gravity-dispatcher

RUN go install -ldflags="-s -w" github.com/BrobridgeOrg/gravity-cli@latest

# RUN upx -6 /gravity-cli /gravity-dispatcher

FROM alpine:3.20
WORKDIR /
RUN apk update && apk upgrade --available && apk add tzdata
COPY ./configs /configs
COPY --from=builder /gravity-dispatcher /gravity-dispatcher
# COPY --from=builder /gravity-cli /gravity-cli

USER 1001
ENV TZ="Asia/Taipei"

ENTRYPOINT ["/gravity-dispatcher"]

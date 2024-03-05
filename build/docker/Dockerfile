FROM golang:1.21.7-alpine3.19 AS builder
WORKDIR /
COPY . .

RUN apk add --update build-base git && apk upgrade --available
RUN go build -o /gravity-dispatcher

RUN git clone https://github.com/BrobridgeOrg/gravity-cli.git &&\
	cd gravity-cli &&\
	go build

FROM alpine:3.19.1
WORKDIR /
RUN apk update && apk upgrade --available && apk add tzdata
COPY ./configs /configs
COPY --from=builder /gravity-dispatcher /gravity-dispatcher
COPY --from=builder /gravity-cli/gravity-cli /gravity-cli

USER 1001
ENV TZ="Asia/Taipei"

ENTRYPOINT ["/gravity-dispatcher"]

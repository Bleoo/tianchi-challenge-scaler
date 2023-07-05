#!/bin/bash

# cp -R data/data_training/* /tmp/data

pkill -9 scaler

./run.sh

curl 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=117fc812-cfee-4c9e-9f54-b22afdde482e' \
   -H 'Content-Type: application/json' \
   -d '
   {
        "msgtype": "text",
        "text": {
            "content": "运行完了"
        }
   }'

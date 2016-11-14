#!/usr/bin/python
# -*- coding: utf-8 -*-

import pika
import json
import time_uuid
from time import gmtime, strftime
from datetime import datetime

EXCHANGE_NAME = 'simpl_exchange'
EXCHANGE_TYPE = 'fanout'

print(('pika version: %s') % pika.__version__)

connection   = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME,  exchange_type=EXCHANGE_TYPE, durable=True)

i = 0
while True:
  msg = { "bucketName": strftime("date_%Y_%m_%d", gmtime()),
          "eventId":    str(time_uuid.TimeUUID.with_utc(datetime.utcnow())),
          "eventName":  "SomeEventName",
          "payload":    "{\"user\":\"shekhar\"}"}

  channel.basic_publish(exchange      = EXCHANGE_NAME,
                        routing_key   = 'test',
                        body          = json.dumps(msg),
                        properties    = pika.BasicProperties(content_type='application/json'))
  i = i + 1
  print "Sent: " + str(i)
connection.close()

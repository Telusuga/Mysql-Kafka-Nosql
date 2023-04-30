from confluent_kafka import Consumer

def login_info():
    info={
        'bootstrap.servers':'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092',
        'security.protocol':'SASL_SSL',
        'sasl.mechanism':'PLAIN',
        'sasl.username':'P3FP4JOSTVZS2XWF',
        'sasl.password':'j4N3yUaVe6y6oRsie5upI3isMePZKRD3C7dozvKSu35DBe12nnIj6XUd9JUIbvZp',
        'partitioner':'consistent_random',
        'group.id':'testing',
        'auto.offset.reset':'earliest'

    }
    return info


c=Consumer(login_info())

print(c.list_topics().topics)
d=c.list_topics().topics

topic='topic_1'

c.subscribe([topic])


while True:
    try:
        msg=c.poll(5.0)
        
        if msg is None:
            print('wainting for messages!!')
            continue
        elif msg is not None:
            msg_value=msg.value().decode('utf-8')
            #print(msg_value)
            print(type(msg_value))
            print(f'The following messages are f{msg.value()} which was delivered')
            
    except KeyboardInterrupt:
        print('closing the consumer code')
        break
c.close()


    



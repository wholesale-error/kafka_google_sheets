import configparser
import sys,os,json
from confluent_kafka import Consumer

def getConfig(key):
    config = configparser.RawConfigParser()
    if os.path.isfile('kafka_config'):
        config.read('kafka_config')
    else:
        print("unable to read config file")
        return False

    try:
        config_dict=dict(config.items(key))
    except Exception as e:
        print(e)
        return False

    return config_dict


def consume(data):
    print(data)



def kafka_consumer1(topicName, count=0):
    consumer = Consumer({   
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'a-group6',
        'auto.offset.reset': 'earliest',
    })	
    consumer.subscribe([topicName])

    while True:
        try:
            msg=consumer.poll(1.0)
            if msg is None:
                print("waiting for messages/event in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                #print(msg.key(),msg.value())
                data= json.loads(msg.value())
                try:
                    consume(data)
                except Exception as e:
                    print(e)
                count+=1
                print(f"consumed {count} messages")              

        except Exception as e:
            print(e)

    

if __name__=="__main__":
    topic= sys.argv[1]
    #print(getConfig('mysql'))
    kafka_consumer1(topic)
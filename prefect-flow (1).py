# import dependencies 
import requests
import boto3
from prefect import task, flow
import time

# API url we will be retrieving from 
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/zvd6vz"
payload = requests.post(url).json()

# url we will be submitting the sqs message to 
queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/zvd6vz"
sqs = boto3.client('sqs')

# defining a delete_message function as a surprise tool that'll help us later
# (once a message is read, it will be deleted) 
def delete_message(queue_url, receipt_handle):
     try:
         response = sqs.delete_message(
             QueueUrl=queue_url,
             ReceiptHandle=receipt_handle
         )
         print(f"Response: {response}")
     except Exception as e:
         print(f"Error deleting message: {e}")
         raise e

# prefect task to fetch messages from the queue
@task(retries=3, retry_delay_seconds=5, log_prints=True)
# define function for fetching messages 
def get_message(queue_url, expected_count=21, max_attempts=20):
    # list to store our tuples
    all_messages = []
    # setting up an attempt counter so the computer knows how many times we've polled...
    # ...for a message
    attempt = 0
    while len(all_messages) < expected_count and attempt < max_attempts: 
        try: 
            # just the metadata of the response 
            response = sqs.receive_message(
                QueueUrl=queue_url,
                # "grab the whole message" 
                MessageSystemAttributeNames=['All'],
                MaxNumberOfMessages=10, # dang
                VisibilityTimeout=5, # this is where your error was! 
                MessageAttributeNames=['All'],
                WaitTimeSeconds=5
            )
            
            messages = response.get('Messages', [])

            # if we can't find a message, we'll add to the polling attempt counter
            if not messages:
                attempt += 1
                print(f"🟡 Attempt {attempt}: no messages returned, waiting...")
                time.sleep(min(2 ** attempt, 10))  # exponential backoff up to 10 sec
                continue
            
            for message in messages:
                print(f"{message}")
    
                # fetching attributes of the message
                attrs = message.get('MessageAttributes', {})
                # parsing the order_no 
                order_no = attrs.get('order_no', {}).get('StringValue')
                # turning it into an integer so we can sort it later
                order_no = int(order_no)
                # parsing the word 
                word = attrs.get('word', {}).get('StringValue')
                
                print(f"Order: {order_no}, Word: {word}")
                # store order_no and word in a tuple for later use...
                # ...and that tuple is going to be stored in our list 
                all_messages.append((order_no, word))
    
                # Delete message from the queue once processed
                receipt_handle = message['ReceiptHandle']
                delete_message(queue_url, receipt_handle)

            # set the poll attempt counter to zero if we've successfully retrieved...
            # ...a message 
            attempt = 0
            time.sleep(0.5)
                        
        # error handling
        except Exception as e:
            print(f"Error getting message: {e}")
            raise e
                            
    # (this is just so i know we've retrieved all messages) 
    print(f"✅ Retrieved total {len(all_messages)} messages.")
    print(all_messages)
    # returning our messages so that they can be passed on to our next task
    return all_messages

# prefect task for sorting the words in the correct order
@task(retries=5, retry_delay_seconds=10, log_prints=True)
# function to sort the words in the correct order...
def reassemble_messages(all_messages):
    sorted_tuples = sorted(all_messages, key = lambda n: n[0])
    print(sorted_tuples)
    # ...and reassemble them into a phrase 
    list_of_words = [i[1] + " " for i in sorted_tuples]
    phrase = "".join(list_of_words)
    # this is just for me 
    print(phrase)
    # returning our phrase so we can pass it along to the next task 
    return phrase

# prefect task for sending the solution as an sqs message 
@task(retries=5, retry_delay_seconds=10, log_prints=True)
# function to send solution 
def send_solution(queue_url, uvaid, phrase, platform):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=phrase,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        # print response so i can see that HTTP 200 response 
        print(f"Response: {response}")
        # passing on our response because not returning it makes me nervous
        return response 

    # error handling 
    except Exception as e:
        print(f"Error sending message: {e}")
        raise e

# orchestrating the whole flow 
@flow(retries=5, retry_delay_seconds=10, log_prints=True)
def orchestrate(log_prints=True):
    messages = get_message(queue_url)
    final_phrase = reassemble_messages(messages)
    uvaid = "zvd6vz"
    platform = "prefect"  
    solution = send_solution(queue_url, uvaid, final_phrase, platform)
    return solution

if __name__ == "__main__":
    result = orchestrate()
    print(result)
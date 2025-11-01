# chatgpt'ed my old code. 
# i have another deadline tonight so if i didn't go back and modify this 
# assume this is my final version. 
from datetime import datetime, timedelta
import time
import requests
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

# AWS SQS client
sqs = boto3.client('sqs', region_name='us-east-1')

# URLs
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/zvd6vz"
queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/zvd6vz"


# -----------------------------
# Helper functions
# -----------------------------
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


# -----------------------------
# Airflow tasks
# -----------------------------
def get_message(**context):
    expected_count = 21
    max_attempts = 20
    all_messages = []
    attempt = 0

    while len(all_messages) < expected_count and attempt < max_attempts:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MessageSystemAttributeNames=['All'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=5,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=5
            )

            messages = response.get('Messages', [])

            if not messages:
                attempt += 1
                print(f"🟡 Attempt {attempt}: no messages returned, waiting...")
                time.sleep(min(2 ** attempt, 10))
                continue

            for message in messages:
                attrs = message.get('MessageAttributes', {})
                order_no = int(attrs.get('order_no', {}).get('StringValue'))
                word = attrs.get('word', {}).get('StringValue')

                print(f"Order: {order_no}, Word: {word}")
                all_messages.append((order_no, word))

                # delete message from queue
                receipt_handle = message['ReceiptHandle']
                delete_message(queue_url, receipt_handle)

            attempt = 0
            time.sleep(0.5)

        except Exception as e:
            print(f"Error getting message: {e}")
            raise e

    print(f"✅ Retrieved total {len(all_messages)} messages.")
    print(all_messages)
    # Push to XCom for next task
    context['ti'].xcom_push(key='messages', value=all_messages)


def reassemble_messages(**context):
    all_messages = context['ti'].xcom_pull(key='messages', task_ids='get_messages')
    sorted_tuples = sorted(all_messages, key=lambda n: n[0])
    list_of_words = [i[1] + " " for i in sorted_tuples]
    phrase = "".join(list_of_words).strip()
    print(f"Phrase: {phrase}")
    context['ti'].xcom_push(key='final_phrase', value=phrase)


def send_solution(**context):
    phrase = context['ti'].xcom_pull(key='final_phrase', task_ids='reassemble_messages')
    uvaid = "zvd6vz"
    platform = "airflow"

    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=phrase,
            MessageAttributes={
                'uvaid': {'DataType': 'String', 'StringValue': uvaid},
                'phrase': {'DataType': 'String', 'StringValue': phrase},
                'platform': {'DataType': 'String', 'StringValue': platform},
            },
        )
        print(f"Response: {response}")
        return response
    except Exception as e:
        print(f"Error sending message: {e}")
        raise e


# -----------------------------
# Define the DAG
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='sqs_message_pipeline',
    default_args=default_args,
    description='Retrieve, sort, and send SQS messages',
    schedule=None,  # Run manually or trigger externally
    start_date=datetime(2025, 10, 31),
    catchup=False,
    tags=['aws', 'sqs', 'api']
) as dag:

    # define tasks
    get_messages = PythonOperator(
        task_id='get_messages',
        python_callable=get_message
    )

    reassemble = PythonOperator(
        task_id='reassemble_messages',
        python_callable=reassemble_messages,
    )

    send = PythonOperator(
        task_id='send_solution',
        python_callable=send_solution,
    )

    # define DAG dependencies
    get_messages >> reassemble >> send

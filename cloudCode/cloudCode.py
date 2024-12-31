import boto3
import numpy as np
import tflite_runtime.interpreter as tflite
from PIL import Image
import io
import json
from datetime import datetime, timedelta

# AWS Configuration
S3_CLIENT = boto3.client('s3')
IOT_CLIENT = boto3.client('iot-data', region_name='us-east-1')
DYNAMODB = boto3.resource('dynamodb', region_name='us-east-1')

# Resource Configuration
MODEL_BUCKET = 'petimagestorage'
MODEL_KEY = 'converted_tflite/model_unquant.tflite'
TABLE_NAME = 'FeedingTimes'
TABLE = DYNAMODB.Table(TABLE_NAME)

# Load TensorFlow Lite Model
def load_model():
    try:
        response = S3_CLIENT.get_object(Bucket=MODEL_BUCKET, Key=MODEL_KEY)
        model_data = response['Body'].read()
        interpreter = tflite.Interpreter(model_content=model_data)
        interpreter.allocate_tensors()
        return interpreter
    except Exception as e:
        print(f"Error loading model: {e}")
        raise

INTERPRETER = load_model()
INPUT_DETAILS = INTERPRETER.get_input_details()
OUTPUT_DETAILS = INTERPRETER.get_output_details()
CLASS_LABELS = ['Pet1', 'Pet2', 'NoPet']

def preprocess_image(image_bytes):
    """Preprocess image for model inference."""
    img = Image.open(io.BytesIO(image_bytes)).resize((224, 224))
    img = np.array(img) / 255.0
    return np.expand_dims(img, axis=0).astype(np.float32)

def initialize_table():
    """Check and initialize the DynamoDB table with default values."""
    response = TABLE.scan()
    existing_pets = {item['PetID'] for item in response.get('Items', [])}
    default_time = (datetime.now() - timedelta(hours=4)).isoformat()

    for pet in CLASS_LABELS:
        if pet not in existing_pets:
            TABLE.put_item(Item={
                "PetID": pet,
                "LastFedTime": default_time
            })
            print(f"Added {pet} with LastFedTime: {default_time}")
    print("Table initialized successfully.")

def get_last_fed_time(pet_name):
    """Fetch the last fed time from DynamoDB."""
    response = TABLE.get_item(Key={'PetID': pet_name})
    if 'Item' in response:
        return datetime.fromisoformat(response['Item']['LastFedTime'])
    return None

def update_last_fed_time(pet_name):
    """Update the last fed time in DynamoDB."""
    now = datetime.now().isoformat()
    TABLE.put_item(Item={'PetID': pet_name, 'LastFedTime': now})

def publish_to_iot(predicted_class_label, confidence_score, fed_status):
    """Publish inference results to IoT Core."""
    payload = {
        "recognized_label": predicted_class_label,
        "confidence_score": float(confidence_score),
        "fed": fed_status
    }
    IOT_CLIENT.publish(
        topic='pet/dispenser/command',
        qos=1,
        payload=json.dumps(payload)
    )
    print(f"Published to IoT Core: {payload}")

def run_inference(image_bytes):
    """Run inference on the given image."""
    input_data = preprocess_image(image_bytes)
    INTERPRETER.set_tensor(INPUT_DETAILS[0]['index'], input_data)
    INTERPRETER.invoke()
    predictions = INTERPRETER.get_tensor(OUTPUT_DETAILS[0]['index'])
    predicted_class_index = np.argmax(predictions)
    return CLASS_LABELS[predicted_class_index], predictions[0][predicted_class_index]

def lambda_handler(event, context):
    """AWS Lambda entry point."""
    initialize_table()
    try:
        image_data = event['Records'][0]['s3']['object']['key']
        bucket_name = event['Records'][0]['s3']['bucket']['name']

        response = S3_CLIENT.get_object(Bucket=bucket_name, Key=image_data)
        image_bytes = response['Body'].read()

        predicted_class_label, confidence_score = run_inference(image_bytes)

        threshold = 0.85
        fed_status = False
        if predicted_class_label != 'NoPet' and confidence_score > threshold:
            last_fed = get_last_fed_time(predicted_class_label)
            print(last_fed)
            if not last_fed or datetime.now() - last_fed >= timedelta(hours=4):
                update_last_fed_time(predicted_class_label)
                fed_status = True

        publish_to_iot(predicted_class_label, confidence_score, fed_status)
        return {
            "statusCode": 200,
            "body": {
                "pet_name": predicted_class_label,
                "confidence": float(confidence_score),
                "fed": fed_status
            }
        }
    except Exception as e:
        print(f"Error in Lambda function: {e}")
        return {
            "statusCode": 500,
            "body": "Error processing request"
        }

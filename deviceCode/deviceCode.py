from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import RPi.GPIO as GPIO
import time
import os
import boto3
import uuid
import json
import time

# IoT Core Configuration
MQTT_ENDPOINT = os.getenv("MQTT_ENDPOINT")
TOPIC = "pet/dispenser/image"
RESPONSE_TOPIC = 'pet/dispenser/command'
CERT_DIR = "/home/jsasid/Desktop/certificate/"
CERT_FILES = {
    "root_ca": "AmazonRootCA1.pem",
    "private_key": "private.pem.key",
    "certificate": "certificate.pem.crt"
}

# S3 Configuration
BUCKET_NAME = "petimagestorage"
REGION_NAME = "us-east-1"

# Image Capture Configuration
OUTPUT_DIR = "/home/jsasid/Desktop/pi/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

response_received = False
last_response = None

def setup_mqtt_client():
    """Setup MQTT client for AWS IoT Core."""
    client = AWSIoTMQTTClient("SmartPetDispenser")
    client.configureEndpoint(MQTT_ENDPOINT, 8883)
    client.configureCredentials(
        os.path.join(CERT_DIR, CERT_FILES["root_ca"]),
        os.path.join(CERT_DIR, CERT_FILES["private_key"]),
        os.path.join(CERT_DIR, CERT_FILES["certificate"])
    )
    client.configureOfflinePublishQueueing(-1)
    client.configureDrainingFrequency(2)
    client.configureConnectDisconnectTimeout(10)
    client.configureMQTTOperationTimeout(10)
    return client

def capture_image():
    """Capture an image using the Raspberry Pi camera."""
    timestamp = int(time.time())
    image_path = f"{OUTPUT_DIR}captured_image_{timestamp}.jpg"
    os.system(f"libcamera-jpeg -o {image_path}")
    if os.path.exists(image_path):
        return image_path
    print("Error: Camera capture failed.")
    return None

def upload_to_s3(image_path):
    """Upload the captured image to S3 and return the URL."""
    try:
        s3_client = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key= os.getenv("AWS_ACCESS_KEY_ID"),region_name=os.getenv("AWS_REGION"))
        file_name = f"{uuid.uuid4()}.jpg"
        s3_client.upload_file(image_path, BUCKET_NAME, file_name)
        s3_url = f"https://{BUCKET_NAME}.s3.{REGION_NAME}.amazonaws.com/{file_name}"
        print(f"Image uploaded successfully: {s3_url}")
        return s3_url
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None

def publish_to_iot(client, s3_url):
    """Publish the S3 URL to AWS IoT Core."""
    payload = {"image_url": s3_url}
    try:
        client.publish(topic=TOPIC, payload=json.dumps(payload), QoS=1)
        print(f"Message published to IoT Core: {payload}")
    except Exception as e:
        print(f"Error publishing to IoT Core: {e}")

def message_callback(client, userdata, message):
    """Callback to handle incoming messages."""
    global response_received, last_response
    payload = json.loads(message.payload)
    print(f"Message received from IoT Core: {payload}")
    last_response = payload
    response_received = True


def wait_for_response(timeout=30):
    """Wait for a response from IoT Core."""
    global response_received
    print("Waiting for response...")
    response_received = False
    start_time = time.time()
    while not response_received:
        if time.time() - start_time > timeout:
            print("Response timed out")
            return None
        time.sleep(0.5)  # Poll every 0.5 seconds
    return last_response

def activate_servo():

    GPIO.setmode(GPIO.BCM)
    GPIO.setup(17, GPIO.OUT) 

    pwm = GPIO.PWM(17, 50)  
    pwm.start(7.5) 
    time.sleep(3)
    try:
        pwm.ChangeDutyCycle(12.5) 
        time.sleep(1)  
        pwm.ChangeDutyCycle(7.5) 
        time.sleep(1)
        pwm.ChangeDutyCycle(0)
        print("Feeding action completed.")
    finally:
        pwm.stop()
        GPIO.cleanup()

def main():
    mqtt_client = setup_mqtt_client()
    for attempt in range(5):
        try:
            mqtt_client.connect()
            print("Connected successfully!")
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
        print("Failed to connect after multiple attempts.")
    mqtt_client.subscribe(RESPONSE_TOPIC, 1, message_callback)
    
    try:
        while True:
            # Capture image
            image_path = capture_image()
            if image_path:
                # Upload to S3 and publish the URL
                s3_url = upload_to_s3(image_path)
                if s3_url:
                    publish_to_iot(mqtt_client, s3_url)
                    # Wait for a response from AWS IoT Core
                    response = wait_for_response()
                    if response.get("fed"):
                        print("Pet has been fed. Activating servo...")
                        activate_servo()
                    else:
                        print("No action required. Pet not fed.")
            # Wait 30 seconds before capturing the next image
            print("Waiting 30 seconds before the next capture...")
            time.sleep(30)

    except KeyboardInterrupt:
        print("Exiting...")
        mqtt_client.disconnect()

if __name__ == "__main__":
    main()

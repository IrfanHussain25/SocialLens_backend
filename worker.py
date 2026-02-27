import boto3
import json
import os
import time
import uuid
import yt_dlp
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from dotenv import load_dotenv

# 1. Load the credentials from the .env file
load_dotenv()

# --- Configuration ---
QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/691210491761/ContentProcessingQueue'
BUCKET_NAME = 'social-lens-raw-intake'
REGION = os.getenv('AWS_DEFAULT_REGION')

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# --- DUMMY SERVER FOR RENDER ---
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Worker is alive and polling SQS!")

def run_dummy_server():
    # Render assigns a port dynamically via the PORT environment variable
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    print(f"Dummy health-check server running on port {port}")
    server.serve_forever()
# -------------------------------

def process_video(video_url, file_name):
    """Universal downloader for both YouTube and Instagram using yt-dlp"""
    print(f"Downloading video via yt-dlp...")
    ydl_opts = {
        'outtmpl': file_name, 
        'format': 'best[ext=mp4]/best', 
        'quiet': True,
        'no_warnings': True,
        # Uncomment the line below if YouTube throws a 403 error due to Render's datacenter IP
        # 'cookiefile': 'cookies.txt', 
        # 'extractor_args': {'youtube': ['player_client=default,-android_sdkless']} 
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

def poll_queue():
    print("Worker Server Started. Listening for URLs...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20 
            )

            if 'Messages' in response:
                message = response['Messages'][0]
                receipt_handle = message['ReceiptHandle']
                payload = json.loads(message['Body'])
                
                video_url = payload.get('videoUrl')
                job_id = payload.get('jobId', str(uuid.uuid4()))
                file_name = f"{job_id}.mp4"
                
                print(f"\n--- New Job Detected: {job_id} ---")
                print(f"URL: {video_url}")

                process_video(video_url, file_name)
                
                s3_key = f"raw-videos/{file_name}"
                print(f"Uploading to S3 bucket: {BUCKET_NAME}...")
                s3.upload_file(file_name, BUCKET_NAME, s3_key)
                print("Upload successful!")

                if os.path.exists(file_name):
                    os.remove(file_name)

                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
                print("Job Complete. Message removed from queue.\n")

        except Exception as e:
            print(f"Error processing job: {str(e)}")
            time.sleep(5)

if __name__ == "__main__":
    # 2. Start the dummy server in a background thread so it doesn't block the worker
    threading.Thread(target=run_dummy_server, daemon=True).start()
    
    # 3. Start your actual SQS polling loop
    poll_queue()
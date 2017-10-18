import io
import time
import picamera
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

def main():
  iot = create_iot_client()

  print("connecting")
  iot.connect()
  print("connected")

  try:
    counter = 0
    for frame in camera_frames():
      iot.publish("camera/frame", frame, 0)
      counter += 1
      if counter is 120:
        break
  except:
    iot.disconnect()


def camera_frames():
  with picamera.PiCamera() as camera:
    # let camera warm up
    time.sleep(2)

    stream = io.BytesIO()
    for _ in camera.capture_continuous(stream, 'jpeg', use_video_port=True):
      stream.seek(0)
      yield stream.read()

      stream.seek(0)
      stream.truncate()

def create_iot_client():
  import config
  client = AWSIoTMQTTClient(config.thing)
  client.configureEndpoint(config.server, 8883)
  client.configureCredentials(config.cert_ca, config.private_key, config.cert_public)
  client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
  client.configureDrainingFrequency(2)  # Draining: 2 Hz
  client.configureConnectDisconnectTimeout(10)  # 10 sec
  client.configureMQTTOperationTimeout(5)  # 5 sec
  return client
  


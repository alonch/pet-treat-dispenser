import io
import time
import picamera
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import traceback

from queue import Queue

def main():
  options = {}
  queue = Queue()
  
  iot = create_iot_client()
  print("connecting")
  iot.connect()
  print("connected")
  try:
    iot.subscribe("camera/activate", 1, lambda client, userdata, message: queue.put({'type':'camera/activate', 'args':{'client':client, 'userdata':userdad, 'message':message}}))

    actions = {
      'camera/activate':on_camera_activate,
      'camera/frame':camera_frame_loop
    }
    while True:
      event = queue.get(block=True)
      actions[event['type']](event['type'], event['args'], iot, options)
  except:
    iot.disconnect()
    traceback.print_exc()

def on_camera_activate(type, args, iot, options):
  
  if not args['message']:
    options['camera'] = False
    return

  if 'start_time' in options:
    print("already running")
    return

  options['camera'] = True
  queue.put({'type':'camera/frame', 'args':{}})
    


def camera_frame_loop(type, args, iot, options):
  
  if 'camera' in options:
    if not options['camera']
      print("stop camera")
      options['frames'].stop()
      del options['frames']
      del options['camera']
      del options['start_time']
      return

    print("start camera")
    options['start_time'] = time.time()
    options['frames'] = camera_frames():
    del options['camera']

  iot.publish("camera/frame", bytearray(next(options['frames'])), 0)
  queue.put({'type':'camera/frame', 'args':{}})

def camera_frames():
  with picamera.PiCamera() as camera:
    # let camera warm up
    camera.resolution = (640, 480)
    camera.framerate = 2
    time.sleep(2)
    
    stream = io.BytesIO()
    for _ in camera.capture_continuous(stream, 'jpeg', use_video_port=True, quality=35):
      stream.seek(0)
      yield stream.read()

      stream.seek(0)
      stream.truncate()

def create_iot_client():
  import config
  client = AWSIoTMQTTClient(config.thing)
  client.configureEndpoint(config.server, 8883)
  client.configureCredentials(config.ca_cert, config.private_key, config.public_cert)
  client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
  client.configureDrainingFrequency(2)  # Draining: 2 Hz
  client.configureConnectDisconnectTimeout(10)  # 10 sec
  client.configureMQTTOperationTimeout(5)  # 5 sec
  return client
  

if __name__ == '__main__':
  main()

from imports import *

# 데이터를 수신할 때 호출되는 콜백 함수
def callback(client, userdata, message):
    print(f"Received message on topic {message.topic}: {message.payload}")
    if message.topic == const.SUB_TOPIC:
        thermal_data_pub()

def thermal_data_pub():
    # S3에 열화상 이미지 파일 업로드
    file_name = execute_thermal_imgsave()
    # 8x8 열화상 데이터 읽기
    file_url = f"{const.S3_BASE_URL}{file_name}"
    # MQTT 메시지 Publish
    thermal_data = read_thermal_data()
    message = {
        "file_url": file_url,
        "sensor_data": thermal_data
    }
    mqtt_pub(message)

def execute_thermal_imgsave():
    # thermal_imgsave.py 파일 실행
    subprocess.run(["python3", "thermal_imgsave.py"])

    # pic.jpg 파일이 생성될 때까지 대기
    wait_for_file("pic.jpg", timeout=60)  # 최대 60초 대기

    # 파일 이름 생성 (현재 시간과 THING_NAME 포함)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"{timestamp}_{const.THING_NAME}.jpg"
    os.remove("pic.jpg", file_name)

    # S3에 업로드
    upload_to_s3(file_name, const.BUCKET_NAME, file_name)

    return file_name

def wait_for_file(filepath, timeout):
    start_time = time.time()
    while not os.path.exists(filepath):
        if time.time() - start_time > timeout:
            raise TimeoutError(f"{filepath} 파일 생성 시간 초과")
        time.sleep(1)  # 1초 대기

def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"File {file_path} uploaded to {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {file_path} to S3: {e}")

def read_thermal_data():
    # I2C 버스 초기화
    i2c = busio.I2C(board.SCL, board.SDA)
    amg = adafruit_amg88xx.AMG88XX(i2c)
    # 8x8 열화상 데이터 읽기
    data = []
    for row in amg.pixels:
        formatted_row = ['{0:.1f}'.format(temp) for temp in row]
        data.append(formatted_row)
        print(formatted_row)
        print("")
    print("\n")
    # 1초 대기
    time.sleep(1)
    return data

def mqtt_pub(message):    
    mqtt_client.publish(const.PUB_TOPIC, json.dumps(message), 0)  # 메시지 발행

if __name__ == "__main__":
    # AWS IoT Core 설정 및 AWS IoT MQTT 클라이언트 생성
    mqtt_client = AWSIoTMQTTClient(const.THING_NAME)
    mqtt_client.configureEndpoint(const.ENDPOINT, 8883)
    mqtt_client.configureCredentials(const.CAROOTPATH, const.KEYPATH, const.CERTPATH)

    # MQTT 클라이언트 설정
    mqtt_client.configureOfflinePublishQueueing(-1)  # 무제한 오프라인 큐잉
    mqtt_client.configureDrainingFrequency(2)  # 2Hz 드레이닝 속도
    mqtt_client.configureConnectDisconnectTimeout(10)  # 10초 연결/해제 타임아웃
    mqtt_client.configureMQTTOperationTimeout(5)  # 5초 MQTT 작업 타임아웃

    mqtt_client.connect()

    time.sleep(2)  # 2초 대기
    print('Connected to AWS IoT Core')

    # 구독할 주제에 대해 메시지를 수신하기 위해 콜백 함수 등록
    mqtt_client.subscribe(const.SUB_TOPIC, 1, callback)

    # 메시지 수신을 계속해서 처리하기 위해 대기
    while True:
        time.sleep(1)

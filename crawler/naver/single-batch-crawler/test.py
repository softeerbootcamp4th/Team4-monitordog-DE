import boto3, json, logging, datetime, time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    keyword = event['keyword']
    target_prefix = event['target_prefix']
    start_number = int(event['start_number'])
    start_datetime = datetime.datetime.fromisoformat(event['start_datetime'])
    max_days = int(event['max_days'])
    bucket_name = event['bucket_name']

    file_name = f"{keyword}.jsonl"
    file_path = f"/tmp/{file_name}"

    dummies = [
        {"title": "아반떼N 진공펌프 교환", "content": "주행중 경고등 점등되어 견인 입고\n진단시 진공펌프 회로 이상 표출합니다\n회로도 참고하여 퓨즈 점검시 퓨즈 단선이라\n퓨즈 교체후 확인시 바로 나가버립니다\n진공펌프 커넥터 탈거후 확인시 괜찮아 진공펌프 교환\n미션 뒤에 숨어 있네여.\n고장난 펌프 저항입니다.\n정상 펌프 저항 교환후 이상없이 작동 합니다.", "author": "꽃곰탱", "created_at": "2024.07.05. 21:54", "viewed": "조회 186", "num_of_comments": "14", "liked": "0", "comments": [{"author": "후크선장", "content": "수고하셨습니다 ~^^", "created_at": "2024.07.05. 22:05", "num_of_comments": 0, "children": []}, {"author": "마창대교", "content": "수고하셨습니다.", "created_at": "2024.07.06. 00:17", "num_of_comments": 0, "children": []}, {"author": "현대티지모터스", "content": "A/S인가요~?\n수고하셨습니다 ~", "created_at": "2024.07.06. 06:59", "num_of_comments": 0, "children": []}, {"author": "가족을위해", "content": "수고하셨습니다", "created_at": "2024.07.06. 07:12", "num_of_comments": 0, "children": []}, {"author": "하이카프라자", "content": "수고하셨습니다", "created_at": "2024.07.06. 07:23", "num_of_comments": 0, "children": []}, {"author": "한얼스", "content": "수고하셨습니다", "created_at": "2024.07.06. 07:35", "num_of_comments": 0, "children": []}, {"author": "목련꽃", "content": "수고하셨습니다", "created_at": "2024.07.06. 08:30", "num_of_comments": 0, "children": []}, {"author": "좌청룡 우백호", "content": "정보 감사합니다", "created_at": "2024.07.06. 08:34", "num_of_comments": 0, "children": []}, {"author": "홀로 카", "content": "수고하셨습니다.", "created_at": "2024.07.06. 08:40", "num_of_comments": 0, "children": []}, {"author": "이달의소녀", "content": "수고하셨습니다", "created_at": "2024.07.06. 09:52", "num_of_comments": 0, "children": []}, {"author": "서진서카이", "content": "수고하셨습니다", "created_at": "2024.07.06. 10:27", "num_of_comments": 0, "children": []}, {"author": "향유고래", "content": "", "created_at": "2024.07.06. 11:01", "num_of_comments": 0, "children": []}, {"author": "카리페어샵", "content": "수고 하셨습니다.", "created_at": "2024.07.06. 11:04", "num_of_comments": 0, "children": []}, {"author": "쪼부장", "content": "벌써? 빨르네여", "created_at": "2024.07.07. 23:16", "num_of_comments": 0, "children": []}]},
        {"title": "※ 2024 코나 가격표 다운로드", "content": "가솔린, 하이브리드, EV 연식변경 가격표 업데이트 되었습니다.\n첨부파일\nthe-all-new-kona-2024-price\n.pdf\nnull\n파일 다운로드\n첨부파일\nthe-all-new-kona-hybrid-2024-price\n.pdf\nnull\n파일 다운로드\n첨부파일\nkona-electric-24-price\n.pdf\nnull\n파일 다운로드", "author": "패밀리ll매니저", "created_at": "2024.02.06. 09:16", "viewed": "조회 2,394", "num_of_comments": "5", "liked": "8", "comments": [{"author": "대전ll아삭", "content": "감사합니다:)", "created_at": "2024.02.06. 09:25", "num_of_comments": 0, "children": []}, {"author": "경기화성ll거북이", "content": "감사합니다", "created_at": "2024.02.06. 11:03", "num_of_comments": 0, "children": []}, {"author": "부산lll코놔내놔", "content": "감사합니당", "created_at": "2024.02.27. 22:40", "num_of_comments": 0, "children": []}, {"author": "화성ll전차전차", "content": "감사합니다", "created_at": "2024.04.07. 18:31", "num_of_comments": 0, "children": []}, {"author": "청주ll셀레네", "content": "오 감사합니다~", "created_at": "2024.04.21. 20:37", "num_of_comments": 0, "children": []}]}
    ]



    for dummy in dummies:
        with open(file_path, "a", encoding='utf8') as target_file:
            json.dump(dummy, target_file, ensure_ascii=False)
            target_file.write('\n')
    
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, file_name)
    return {
            'statusCode': 200,
            'body': json.dumps(f'File({keyword}) uploaded successfully!')
    }
import uuid
import pymongo
import datetime as dt
import json
import random

from midtermDAG import DAG
from midtermOperator import BashOperator
from midtermOperator import PythonOperator

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct

# Hoàn thành việc kết nối với MongoDB và Qdrant
qdrant_client = QdrantClient(host="qdrant_db", port=6333)
mongo_client = pymongo.MongoClient("mongodb://admin:admin@mongo_db:27017")
database_mongo = mongo_client["midterm"]
# tạo collection có tên là mssv của bạn trong MongoDB và Qdrant
# ví dụ mssv của bạn là 17101691 thi tên collection sẽ là "17101691"
collection_mongo = "20078291"
name_collection_qdrant = "20078291"


def create_collection_qdrant():
    try:
        # lấy tên tất cả collection hiện có trong Qdrant
        

        # cấu hình vector params cho collection bao gồm size = 1536 và distance = cosine
        vectorParams = {
            'size': 1536, 'distance':Distance.COSINE
        }
        ### YOUR CODE HERE ###
        # kiểm tra nếu collection chưa tồn tại thì tạo mới
        cls = qdrant_client.get_collections()
        names = [c.name for c in cls.collections]
        if name_collection_qdrant not in names:
            qdrant_client.recreate_collection(
                collection_name="fit-iuh-news",
                vectors_config=VectorParams(vectorParams['size'], distance=vectorParams['distance'])
            )
        # Sử dụng **vectorParams để unpack dict thành các keyword arguments
        
        

        return {
            "status": "success",
            "collection": name_collection_qdrant,
            "vectorParams": str(vectorParams)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def insert_data_mongoDB():
    try:
        # đọc dữ liệu từ file data_iuh_new.json và chọn ngẫu nhiên điểm dữ liệu gán vào biến data
        message = ""
        import json
        with open('data_iuh_new.json', 'r', encoding='utf-8') as file:
            json_data = json.load(file)
            data = json_data[0]
        db = mongo_client["fit-iuh"]
        news = db["news"]
        if news.find_one({"title": data['title']}):
            message = "Data already exists"
        else:
            data['status'] = 'new'
            news.insert_one(data)
            message = "Data inserted"
        # kiểm tra title của điểm dữ liệu đã tồn tại trong MongoDB chưa
        # nếu đã tồn tại thì gán "Data already exists" cho biến message
        # nếu chưa thì thêm trường "status": "new" vào data và insert vào MongoDB 
        # sau đó gán "Data inserted" cho biến message

        title = data["title"]
        return {
            "status": "success",
            "data": title,
            "message": message
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def insert_data_qdrant():
    try:
        # lấy tất cả các điểm dữ liệu có "status":"new" từ MongoDB
        # insert các điểm dữ liệu này vào Qdrant (lưu ý: không insert trường "_id" và "embedding" của MongoDB vào Qdrant)
        # sau khi insert thành công thì cập nhật trường "status":"indexed" cho các điểm dữ liệu đã insert trong MongoDB
        db = mongo_client["fit-iuh"]
        news = db["news"]
        data = news.find({"status": "new"})
        for item in data:
            item.pop("_id")
            item.pop("embedding")
            item["status"] = "indexed"
            qdrant_client.insert_point(collection_name=name_collection_qdrant, point=PointStruct(**item))
            news.update_one({"title": item['title']}, {"$set": {"status": "indexed"}})

        return {
            "status": "success",
            "message": "Data inserted to Vector DB successfully"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def count_data():
    try:
        count = collection_mongo.count_documents({})
        count_indexed = collection_mongo.count_documents({"status": "indexed"})
        count_new = collection_mongo.count_documents({"status": "new"})
        return {
            "status": "success",
            "indexed": count_indexed,
            "new": count_new,
            "mongoDB": count,
            "vectorDB": qdrant_client.get_collection(collection_name=name_collection_qdrant).points_count
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def search_by_vector():
    try:
        # tạo ngẫu nhiên một vector có size = 1536 và sử dụng Qdrant để tìm kiếm 1 điểm gần nhất
        import numpy as np
        result = qdrant_client.search(
            collection_name=name_collection_qdrant,
            query_vector=np.random.rand(1536).tolist(),
            top=1
        )

        result_json = result[0].model_dump()
        return {
            "status": "success",
            "result": result_json
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# đặt owner là mssv của bạn, Thử lại 1 lần nếu thất bại, thời gian chờ giữa các lần thử là 1 phút
default_args = {
    'owner': "20078291",
    'start_date': dt.datetime.now() - dt.timedelta(minutes=19),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),

}


# khởi tạo DAG với tên là mssv của bạn và cài đặt mỗi 5 phút chạy 1 lần

with DAG(
        '20078291',
        default_args=default_args,
        tags=['midterm'],
        schedule_interval="*/5 * * * *",
    ) as dag:

    # khởi tạo pipeline sử dụng BashOperator và PythonOperator như sau:
    # task 1: sử dụng BashOperator để in ra "Midterm exam started" với task_id là mssv của bạn (ví dụ: task_id='17101691')
    start = BashOperator(
        task_id='20078291',
        bash_command='echo "Midterm exam started"'
    )
    # task 2: sử dụng PythonOperator để tạo collection trong Qdrant với task_id là 4 chữ số đầu của mssv của bạn (ví dụ: task_id='1710')
    create_qdrant = PythonOperator(
        task_id='2007',
        python_callable=create_collection_qdrant
    )
    # task 3: sử dụng PythonOperator để insert data vào MongoDB với task_id là 3 chữ số cuối của task2 và số kế tiếp trong mssv (ví dụ: task_id='7101')
    data_mongo = PythonOperator(
        task_id='0078',
        python_callable=insert_data_mongoDB
    )
    # task 4: sử dụng PythonOperator để insert data vào Qdrant với task_id là 3 chữ số cuối của task3 và số kế tiếp trong mssv (ví dụ: task_id='1016')
    data_qdrant = PythonOperator(
        task_id='0782',
        python_callable=insert_data_qdrant
    )
    # task 5: sử dụng PythonOperator để thực hiện hàm count_data với task_id là 3 chữ số cuối của task4 và số kế tiếp trong mssv (ví dụ: task_id='0169')
    data_count = PythonOperator(
        task_id='7829',
        python_callable=count_data
    )
    # task 6: sử dụng PythonOperator để thực hiện search bằng vector với task_id là 3 chữ số cuối của task5 và số kế tiếp trong mssv (ví dụ: task_id='1691')
    vector_search = PythonOperator(
        task_id='829',
        python_callable=search_by_vector
    )
    # task 7: sử dụng BashOperator để in ra "Midterm exam ended" với task_id là 2 số đầu và 2 số cuối của mssv (ví dụ: task_id='1791')
    end = BashOperator(
        task_id='2081',
        bash_command='echo "Midterm exam ended"'
    )
    

import os
import json
import time
import signal
import sys
import boto3
import redis
import structlog
from dotenv import load_dotenv

# Executor 통합 모듈 import
from executor import TaskExecutor, TaskMessage

# --- Setup ---
load_dotenv()

# 로깅 설정
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger()

class NanoAgent:
    def __init__(self):
        logger.info("🤖 NanoGrid Agent Starting...")
        
        # 환경 변수 로드 -> Config 딕셔너리 생성
        self.config = {k: v for k, v in os.environ.items()}
        
        # AWS & Redis Clients
        self.sqs = boto3.client('sqs', region_name=self.config["AWS_REGION"])
        self.redis_client = redis.Redis(
            host=self.config["REDIS_HOST"],
            port=int(self.config["REDIS_PORT"]),
            password=self.config.get("REDIS_PASSWORD"),
            decode_responses=True
        )
        
        # 실행 엔진 초기화
        self.executor = TaskExecutor(self.config)
        self.running = True

        # 시그널 핸들링
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame):
        logger.info("🛑 Shutdown signal received")
        self.running = False

    def run(self):
        queue_url = self.config["SQS_QUEUE_URL"]
        logger.info("📡 Listening for tasks", queue=queue_url)

        while self.running:
            try:
                # 1. SQS Long Polling
                resp = self.sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )

                if "Messages" not in resp:
                    continue

                for msg in resp["Messages"]:
                    self._process_message(queue_url, msg)

            except Exception as e:
                logger.error("Polling loop error", error=str(e))
                time.sleep(1)
        
        logger.info("👋 Agent stopped cleanly")

    def _process_message(self, queue_url, msg):
        try:
            body = json.loads(msg["Body"])
            task = TaskMessage(
                request_id=body["requestId"],
                function_id=body.get("functionId", "unknown"),
                runtime=body.get("runtime", "python"),
                s3_key=body["s3Key"],
                memory_mb=body.get("memoryMb", 128)
            )
            
            logger.info("🚀 Processing Task", id=task.request_id, runtime=task.runtime)

            # 2. 작업 실행 (Executor에게 위임)
            result = self.executor.run(task)

            # 3. 결과 Redis 발행
            channel = f"{self.config['REDIS_RESULT_PREFIX']}{task.request_id}"
            self.redis_client.publish(channel, json.dumps(result.to_dict()))
            
            # 4. SQS 메시지 삭제 (성공 시)
            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            
            logger.info("✅ Task Completed", id=task.request_id, ms=result.duration_ms)

        except json.JSONDecodeError:
            logger.error("Invalid JSON format", body=msg["Body"])
            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        except Exception as e:
            logger.error("Task processing failed", error=str(e))
            # 에러 시 SQS 메시지를 지우지 않음 -> VisibilityTimeout 후 재시도 (DLQ 활용 권장)

if __name__ == "__main__":
    agent = NanoAgent()
    agent.run()
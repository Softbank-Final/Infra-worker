import boto3
import os
import shutil
import structlog
from typing import List, Optional

logger = structlog.get_logger()

class OutputUploader:
    def __init__(self, bucket_name: str, region: str = "ap-northeast-2"):
        self.bucket = bucket_name
        self.region = region
        self.s3 = boto3.client('s3', region_name=region)

    def upload_outputs(self, job_id: str, source_dir: str) -> List[str]:
        """
        source_dir 폴더의 모든 파일을 S3에 업로드하고 URL 리스트를 반환합니다.
        """
        uploaded_urls = []
        
        if not os.path.exists(source_dir):
            logger.warning("Output directory not found", job_id=job_id, path=source_dir)
            return uploaded_urls

        try:
            for filename in os.listdir(source_dir):
                filepath = os.path.join(source_dir, filename)
                
                # 디렉토리는 무시 (재귀적 업로드 필요 시 확장 가능)
                if os.path.isfile(filepath):
                    s3_key = f'outputs/{job_id}/{filename}'
                    
                    logger.info("Uploading output file", job_id=job_id, file=filename)
                    self.s3.upload_file(filepath, self.bucket, s3_key)
                    
                    # URL 생성 (Public Read 가정 또는 Presigned URL 필요 시 변경)
                    # 여기서는 표준 Public URL 형식 사용
                    url = f'https://{self.bucket}.s3.{self.region}.amazonaws.com/{s3_key}'
                    uploaded_urls.append(url)
                    
        except Exception as e:
            logger.error("Failed to upload outputs", job_id=job_id, error=str(e))
            
        return uploaded_urls

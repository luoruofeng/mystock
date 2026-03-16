import uvicorn
from dotenv import load_dotenv
import os

# 加载 .env 文件中的环境变量
load_dotenv()

# 确保代理环境变量被设置
if not os.getenv("HTTPS_PROXY"):
    os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7897"
if not os.getenv("HTTP_PROXY"):
    os.environ["HTTP_PROXY"] = "http://127.0.0.1:7897"

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

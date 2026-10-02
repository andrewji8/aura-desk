"""
全量兼容版 LLM 连通性测试脚本
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from aura_gateway.core.config import get_settings
import httpx


async def test_llm_connection():
    settings = get_settings()
    
    print("=" * 60)
    print("🔍 开始全量兼容测试 LLM 连接...")
    print(f"📍 Base URL : {settings.LLM_BASE_URL}")
    print(f"🤖 Model   : {settings.LLM_MODEL}")
    
    api_key = settings.LLM_API_KEY.get_secret_value().strip()
    if api_key and len(api_key) > 8:
        print(f"🔑 API Key : 已读取 (前6位: {api_key[:6]}...{api_key[-4:]})")
    else:
        print("⚠️  警告: API Key 为空或过短！请检查 .env 文件")
        return
    print("-" * 60)

    url = f"{settings.LLM_BASE_URL}/chat/completions"
    
    # 核心修复：同时提供所有可能的 Token Header 格式，确保网关一定能识别
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
        "x-api-key": api_key,
        "api-key": api_key,
    }
    
    print("📤 正在发送请求 (已注入多种 Token Header 格式)...")

    payload = {
        "model": settings.LLM_MODEL,
        "messages": [{"role": "user", "content": "请仅回复'连接成功'四个字。"}],
        "max_tokens": 10,
        "temperature": 0.1
    }
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.post(url, json=payload, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                print("=" * 60)
                print("✅ 连接成功！(网络与 API 均正常)")
                print(f"🤖 模型回复 : {content}")
                print("=" * 60)
            else:
                print("=" * 60)
                print(f"❌ HTTP 状态错误: {response.status_code}")
                print(f"📄 服务端返回内容: {response.text}")
                print("💡 提示: 如果依然报 Token not provided，请确认 API Key 是否有效。")
                print("=" * 60)
                
        except Exception as e:
            print("=" * 60)
            print(f"❌ 请求失败: {type(e).__name__} - {e}")
            print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_llm_connection())
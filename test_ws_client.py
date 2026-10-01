import asyncio
import websockets
import json

async def test_virtual_human_pipeline():
    session_id = "test-session-001"
    uri = f"ws://127.0.0.1:8000/ws/virtual-human/{session_id}"
    
    print(f"🔗 正在连接到 {uri} ...")
    async with websockets.connect(uri) as ws:
        print("✅ 连接成功！")
        
        # 1. 模拟用户输入文本
        print(f"\n🗣️ [Client] 发送文本 (session_id={session_id}): '你好，请介绍一下你自己'")
        await ws.send(json.dumps({
            "type": "text",
            "session_id": session_id,
            "sequence": 1,
            "text": {
                "text": "你好，请介绍一下你自己",
                "final": False
            }
        }))
        
        # 2. 接收流式响应
        print("\n👂 [Client] 开始接收流式响应:")
        try:
            async with asyncio.timeout(6.0): # 稍微延长超时，确保能收到完整 Mock 回复
                async for message in ws:
                    # 【关键修复】：区分二进制帧和文本帧
                    if isinstance(message, bytes):
                        # 这是原始音频数据，绝对不能 json.loads
                        print(f" 🎵[Audio:{len(message)}B]", end="", flush=True)
                        continue
                    
                    # 这是文本帧，安全解析 JSON
                    data = json.loads(message)
                    msg_type = data.get("type")
                    
                    if msg_type == "text_delta":
                        text = data.get("text", {}).get("text", "")
                        print(f"  📝[Delta]{text}", end="", flush=True)
                    elif msg_type == "status":
                        state = data.get("status", {}).get("state", "unknown")
                        detail = data.get("status", {}).get("detail", "")
                        detail_str = f" ({detail})" if detail else ""
                        print(f"\n  🔄[Status] {state}{detail_str}", end="", flush=True)
                    elif msg_type == "avatar_action":
                        action = data.get("avatar", {}).get("action", "unknown")
                        print(f"  🎭[Avatar] {action}", end="", flush=True)
                    elif msg_type == "error":
                        error_msg = data.get("error", {}).get("message", "Unknown error")
                        print(f"\n  ❌[Error] {error_msg}", flush=True)
                        
        except asyncio.TimeoutError:
            print("\n\n⚠️ [Client] 接收超时 (Mock 正常结束)")

        # 3. 测试打断机制 (Barge-in)
        print("\n\n🛑 [Client] 模拟用户突然插话，发送 interrupt 信号！")
        await ws.send(json.dumps({
            "type": "interrupt",
            "session_id": session_id,
            "sequence": 2,
            "interrupt": {
                "reason": "user_speech",
                "keep_context": False
            }
        }))
        
        # 4. 验证打断是否生效
        print("👂 [Client] 监听打断后的响应 (预期: 只有 status=interrupted，绝无 text_delta):")
        try:
            async with asyncio.timeout(2.0):
                async for message in ws:
                    if isinstance(message, bytes):
                        continue # 忽略打断后可能残留的极少量音频二进制帧
                    
                    data = json.loads(message)
                    if data.get("type") == "text_delta":
                        print("  ❌ [FAIL] 打断失败！依然收到了 text_delta")
                    elif data.get("type") == "status":
                        state = data.get("status", {}).get("state", "unknown")
                        print(f"  ✅ [OK] 收到状态更新: {state}")
                        if state == "interrupted":
                            print("  🎉 [SUCCESS] 打断机制完美生效！")
                            break # 确认打断成功后即可退出
        except asyncio.TimeoutError:
            print("  ✅ [OK] 超时，未收到任何违规的 text_delta，打断成功！")

if __name__ == "__main__":
    asyncio.run(test_virtual_human_pipeline())
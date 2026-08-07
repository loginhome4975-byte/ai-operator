"""Twilio media-stream oqimini tunnel orqali test qiladi (asl qo'ng'iroq simulyatsiyasi)."""
import asyncio, json, sys

try:
    import websockets
except ImportError:
    print("websockets kutubxonasi yo'q — pip install websockets")
    sys.exit(1)


async def main():
    url = "wss://sip.traffix.uz/media-stream"
    print(f"Ulanish: {url}")
    try:
        async with websockets.connect(url, open_timeout=15) as ws:
            print("✅ WebSocket ulandi (tunnel orqali)")
            # Twilio 'start' eventini yuboramiz
            start = {
                "event": "start",
                "sequence": 0,
                "start": {
                    "accountSid": "AC123",
                    "streamSid": "MZ_TEST_STREAM_001",
                    "callSid": "CA_TEST_CALL_001",
                    "tracks": ["inbound"],
                    "customParameters": {"language": "uz"},
                    "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": 8000, "channels": 1},
                },
                "streamSid": "MZ_TEST_STREAM_001",
                "callSid": "CA_TEST_CALL_001",
            }
            await ws.send(json.dumps(start))
            print("→ 'start' event yuborildi (callSid=CA_TEST_CALL_001)")
            # 3 soniya ichida javob kelyaptimi
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=3)
                print(f"← Serverdan javob: {msg[:300]}")
            except asyncio.TimeoutError:
                print("(serverdan media kelmadi — kutish normal, DTMF kutilmoqda)")
            # stop event
            stop = {"event": "stop", "streamSid": "MZ_TEST_STREAM_001", "callSid": "CA_TEST_CALL_001"}
            await ws.send(json.dumps(stop))
            print("→ 'stop' event yuborildi, yopilmoqda")
    except Exception as e:
        print(f"❌ XATO: {type(e).__name__}: {e}")
        sys.exit(1)


asyncio.run(main())

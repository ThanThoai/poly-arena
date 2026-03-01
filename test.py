import asyncio
import json
import websockets


# Hàm chạy ngầm để gửi PING mỗi 10 giây
async def keep_alive(websocket):
    """Gửi tin nhắn 'PING' mỗi 10 giây để giữ kết nối không bị đóng."""
    while True:
        try:
            await asyncio.sleep(10)
            await websocket.send("PING")
            # print("Đã gửi: PING") # Bỏ comment nếu bạn muốn xem log gửi ping
        except websockets.ConnectionClosed:
            print("Kết nối đã đóng, dừng tác vụ PING.")
            break
        except Exception as e:
            print(f"Lỗi khi gửi PING: {e}")
            break


async def get_market_data():
    # Endpoint Market Channel của Polymarket
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Bạn cần thay thế dãy số này bằng Token ID (Asset ID) thực tế của thị trường bạn muốn theo dõi.
    # ID dưới đây chỉ là ví dụ (một token YES/NO mẫu trên Polymarket)
    target_asset_ids = [
        "35881796829857164781110438942460103244092752420895875768964203175989990102958"
    ]

    # Kết nối tới WebSocket
    async with websockets.connect(uri) as websocket:
        print("✅ Đã kết nối thành công tới Polymarket Market Channel!")

        # 1. Khởi tạo và gửi Payload đăng ký
        subscribe_payload = {
            "assets_ids": target_asset_ids,
            "type": "market",
            "custom_feature_enabled": True,  # Kích hoạt để nhận các sự kiện như best_bid_ask, new_market,...
        }

        await websocket.send(json.dumps(subscribe_payload))
        print(
            f"📡 Đã gửi yêu cầu đăng ký (Subscribe) cho token: {target_asset_ids[0][:10]}..."
        )

        # 2. Bắt đầu tác vụ Heartbeat (PING) chạy song song
        asyncio.create_task(keep_alive(websocket))

        # 3. Lắng nghe và xử lý luồng dữ liệu trả về
        try:
            while True:
                response = await websocket.recv()

                # Bỏ qua việc in ra các tin nhắn PONG (phản hồi PING từ server) để log đỡ rối
                if response == "PONG":
                    continue

                # Chuyển đổi chuỗi JSON thành Dictionary của Python và in ra
                data = json.loads(response)

                # In định dạng JSON đẹp mắt
                print("\n📩 Dữ liệu mới nhận:")
                print(json.dumps(data, indent=2))

        except websockets.ConnectionClosed as e:
            print(f"❌ Kết nối WebSocket đã bị đóng: {e}")
        except json.JSONDecodeError:
            print(f"Nhận được thông điệp không phải JSON: {response}")


if __name__ == "__main__":
    # Chạy vòng lặp sự kiện bất đồng bộ
    try:
        asyncio.run(get_market_data())
    except KeyboardInterrupt:
        print("\nĐã dừng chương trình thủ công.")

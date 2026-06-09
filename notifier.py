import logging
import requests
from datetime import datetime
from typing import Any, Dict, List, Optional
from config import Config
from utils import setup_logger


class Notifier:
    """Quản lý việc gửi tin nhắn cảnh báo và báo cáo qua Telegram."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or setup_logger("Notifier")
        self.telegram_token = Config.TELEGRAM_BOT_TOKEN
        self.telegram_chat_id = Config.TELEGRAM_CHAT_ID

    def _send_telegram(self, text: str) -> None:
        """Gửi tin nhắn qua Telegram Bot API."""
        if not self.telegram_token or not self.telegram_chat_id:
            return

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            response = requests.post(url, json=payload, timeout=Config.NETWORK_TIMEOUT)
            response.raise_for_status()
            self.logger.info("📲 [Telegram] Gửi tin nhắn thành công.")
        except Exception as e:
            self.logger.error(f"❌ [Telegram] Gửi tin nhắn thất bại: {e}")

    def send_message(self, text: str) -> None:
        """Gửi tin nhắn qua các kênh được cấu hình."""
        # Chạy bọc trong try-except để không bao giờ làm sập pipeline chính
        try:
            self._send_telegram(text)
        except Exception as e:
            self.logger.error(f"❌ [Notifier] Lỗi không mong muốn trong quá trình gửi thông báo: {e}")

    def send_alert(self, subject: str, message: str) -> None:
        """Gửi tin nhắn cảnh báo lỗi nghiêm trọng."""
        formatted_msg = (
            f"🚨 <b>CẢNH BÁO PIPELINE: {subject}</b>\n\n"
            f"📍 <b>Chi tiết:</b>\n"
            f"<code>{message}</code>\n\n"
            f"⏰ <i>Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
        )
        self.send_message(formatted_msg)


    def send_summary(
        self,
        date_str: str,
        total_processed: int,
        is_eod: bool,
        missing_dates: List[Any],
        reloaded_symbols: List[str],
        failed_reloads: List[str],
    ) -> None:
        """Gửi tin nhắn báo cáo kết quả chạy thành công hàng ngày."""
        status_icon = "✅" if not failed_reloads else "⚠️"
        eod_status = "Đã chốt EOD 🔒" if is_eod else "Chưa chốt EOD 🔓"
        
        missing_dates_str = ", ".join([str(d) for d in missing_dates]) if missing_dates else "Không có"
        reloaded_str = ", ".join(reloaded_symbols) if reloaded_symbols else "Không có"
        failed_str = ", ".join(failed_reloads) if failed_reloads else "Không có"

        formatted_msg = (
            f"{status_icon} <b>BÁO CÁO PIPELINE DAILY VNSTOCK</b>\n"
            f"📅 <b>Ngày giao dịch:</b> {date_str}\n"
            f"📊 <b>Trạng thái:</b> {eod_status}\n\n"
            f"📈 <b>Dữ liệu T0 cập nhật:</b> {total_processed:,} dòng\n"
            f"🔄 <b>Ngày thiếu đã backfill:</b> {missing_dates_str}\n"
            f"🔄 <b>Reload lịch sử điều chỉnh:</b> {reloaded_str}\n"
        )

        if failed_reloads:
            formatted_msg += f"❌ <b>Mã lỗi reload:</b> <code>{failed_str}</code>\n"
        else:
            formatted_msg += f"✨ <b>Kết quả:</b> Hoàn thành xuất sắc, không có lỗi.\n"

        formatted_msg += f"\n⏰ <i>Thời gian chạy: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"

        self.send_message(formatted_msg)

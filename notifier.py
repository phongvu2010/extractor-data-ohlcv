import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import requests
from config import Config
from utils import setup_logger


class Notifier:
    """Quản lý việc gửi tin nhắn cảnh báo và báo cáo kết quả chạy qua Telegram."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """Khởi tạo đối tượng Notifier.

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình. Nếu None, hệ thống tự tạo mới.
        """
        self.logger: logging.Logger = logger or setup_logger("Notifier")
        self.telegram_token: str = Config.TELEGRAM_BOT_TOKEN
        self.telegram_chat_id: str = Config.TELEGRAM_CHAT_ID

    def _send_telegram(self, text: str) -> None:
        """Gửi nội dung tin nhắn dạng HTML qua API Telegram Bot với cơ chế thử lại.

        Args:
            text: Nội dung thông điệp cần gửi, hỗ trợ các thẻ định dạng HTML.

        Raises:
            requests.exceptions.RequestException: Phát sinh khi tất cả các lần thử gửi đều thất bại.
        """
        if not self.telegram_token or not self.telegram_chat_id:
            return

        url: str = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload: Dict[str, Union[str, bool]] = {
            "chat_id": self.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        max_retries: int = 3
        backoff_delay: float = 2.0
        for attempt in range(1, max_retries + 1):
            try:
                response: requests.Response = requests.post(url, json=payload, timeout=Config.NETWORK_TIMEOUT)
                response.raise_for_status()
                self.logger.info("📲 [Telegram] Gửi tin nhắn thành công.")
                return
            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"⚠️ [Telegram] Gửi tin nhắn thất bại (lần thử {attempt}/{max_retries}): {e}"
                )
                if attempt == max_retries:
                    raise e
                time.sleep(backoff_delay * attempt)

    def send_message(self, text: str) -> None:
        """Bọc ngoài cơ chế gửi tin nhắn để đảm bảo lỗi mạng Telegram không làm sập pipeline chính.

        Args:
            text: Nội dung thông điệp cần gửi.
        """
        try:
            self._send_telegram(text)
        except Exception as e:
            self.logger.error(f"❌ [Notifier] Lỗi không mong muốn trong quá trình gửi thông báo: {e}")

    def send_alert(self, subject: str, message: str) -> None:
        """Gửi cảnh báo khẩn cấp khi hệ thống gặp lỗi nghiêm trọng (Critical Error).

        Args:
            subject: Tiêu đề hoặc nguồn gốc phát sinh lỗi cảnh báo.
            message: Chi tiết lỗi hoặc stack trace của ngoại lệ.
        """
        formatted_msg: str = (
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
        """Gửi báo cáo tổng hợp chi tiết sau khi kết thúc phiên chạy hàng ngày thành công.

        Args:
            date_str: Ngày giao dịch của phiên chạy.
            total_processed: Tổng số dòng dữ liệu thô T0 đã được ghi nhận.
            is_eod: Trạng thái chốt phiên cuối ngày EOD (True/False).
            missing_dates: Danh sách các ngày giao dịch còn thiếu đã chạy backfill.
            reloaded_symbols: Danh sách các mã cổ phiếu được tải lại giá điều chỉnh thành công.
            failed_reloads: Danh sách các mã gặp sự cố khi tải lại lịch sử giá điều chỉnh.
        """
        def _format_symbols(symbols: List[str], max_show: int = 15) -> str:
            if not symbols:
                return "Không có"
            if len(symbols) <= max_show:
                return ", ".join(symbols)
            return ", ".join(symbols[:max_show]) + f" (+{len(symbols) - max_show} mã khác)"

        status_icon: str = "✅" if not failed_reloads else "⚠️"
        eod_status: str = "Đã chốt EOD 🔒" if is_eod else "Chưa chốt EOD 🔓"

        missing_dates_str: str = ", ".join([str(d) for d in missing_dates]) if missing_dates else "Không có"
        reloaded_str: str = _format_symbols(reloaded_symbols)
        failed_str: str = _format_symbols(failed_reloads)

        formatted_msg: str = (
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
            formatted_msg += "✨ <b>Kết quả:</b> Hoàn thành xuất sắc, không có lỗi.\n"

        formatted_msg += f"\n⏰ <i>Thời gian chạy: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"

        self.send_message(formatted_msg)

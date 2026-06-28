"""Module hỗ trợ gửi tin nhắn thông báo, báo cáo và cảnh báo qua Telegram."""

from __future__ import annotations

from datetime import datetime
import html
import logging
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from config import Config
from utils import setup_logger


class Notifier:
    """Quản lý việc gửi tin nhắn cảnh báo và báo cáo kết quả chạy qua Telegram."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Khởi tạo đối tượng Notifier.

        Args:
            logger (logging.Logger | None): Đối tượng Logger dùng để ghi nhận
                tiến trình. Nếu None, hệ thống tự tạo mới.
        """
        self.logger: logging.Logger = logger or setup_logger("Notifier")
        self.telegram_token: str = Config.TELEGRAM_BOT_TOKEN
        self.telegram_chat_id: str = Config.TELEGRAM_CHAT_ID
        self.session: requests.Session = requests.Session()

        # Cấu hình tự động retry 3 lần cho các lỗi HTTP server hoặc timeout
        retries: Retry = Retry(
            total=3,
            backoff_factor=2,  # Khoảng giãn cách giữa các lần thử lại là 2s, 4s, 8s
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False  # Để ta tự bắt lỗi HTTPError và xử lý logic 4xx
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def close(self) -> None:
        """Đóng session giải phóng connection pool."""
        if hasattr(self, "session") and self.session:
            self.session.close()

    def __enter__(self) -> Notifier:
        """Hỗ trợ cơ chế quản lý ngữ cảnh (Context Manager)."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Tự động giải phóng tài nguyên kết nối khi thoát khối lệnh with."""
        self.close()

    def __del__(self) -> None:
        """Giải phóng tài nguyên kết nối an toàn khi đối tượng bị hủy."""
        try:
            self.close()
        except Exception:
            pass

    def _send_telegram(self, text: str) -> None:
        """Gửi nội dung tin nhắn dạng HTML qua API Telegram Bot.

        Args:
            text (str): Nội dung thông điệp cần gửi (hỗ trợ HTML).

        Raises:
            requests.exceptions.RequestException: Khi gửi thất bại.
        """
        if not self.telegram_token or not self.telegram_chat_id:
            self.logger.warning(
                "⚠️ [Telegram] Thiếu cấu hình TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID. Bỏ qua gửi thông báo."
            )
            return

        url: str = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": self.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "link_preview_options": {"is_disabled": True},
        }

        telegram_timeout: int = min(8, Config.NETWORK_TIMEOUT)
        try:
            response: requests.Response = self.session.post(
                url, json=payload, timeout=telegram_timeout
            )
            response.raise_for_status()
            self.logger.info("📲 [Telegram] Gửi tin nhắn thành công.")
        except requests.exceptions.RequestException as e:
            # Nếu là lỗi HTTP phía client (ví dụ 400 Bad Request, 401 Unauthorized do token/chat_id sai)
            if (
                isinstance(e, requests.exceptions.HTTPError)
                and e.response is not None
            ):
                if 400 <= e.response.status_code < 500:
                    self.logger.error(
                        f"❌ [Telegram] Lỗi client nghiêm trọng (HTTP {e.response.status_code}). Chi tiết: {e.response.text}"
                    )
            self.logger.error(f"❌ [Telegram] Gửi tin nhắn thất bại: {e}")
            raise e

    def send_message(self, text: str) -> None:
        """Bọc ngoài cơ chế gửi tin nhắn để tránh làm sập pipeline chính.

        Args:
            text (str): Nội dung thông điệp cần gửi.
        """
        try:
            self._send_telegram(text)
        except Exception as e:
            self.logger.error(
                f"❌ [Notifier] Lỗi không mong muốn trong quá trình gửi thông báo: {e}"
            )

    def send_alert(self, subject: str, message: str) -> None:
        """Gửi cảnh báo khẩn cấp khi hệ thống gặp lỗi nghiêm trọng (Critical Error).

        Args:
            subject (str): Tiêu đề hoặc nguồn gốc phát sinh lỗi cảnh báo.
            message (str): Chi tiết lỗi hoặc stack trace của ngoại lệ.
        """
        escaped_subject: str = html.escape(subject)
        escaped_message: str = html.escape(message)
        formatted_msg: str = (
            f"🚨 <b>CẢNH BÁO PIPELINE: {escaped_subject}</b>\n\n"
            f"📍 <b>Chi tiết:</b>\n"
            f"<code>{escaped_message}</code>\n\n"
            f"⏰ <i>Thời gian: {datetime.now(Config.VN_TZ).strftime('%Y-%m-%d %H:%M:%S')}</i>"
        )
        self.send_message(formatted_msg)

    def send_summary(
        self,
        date_str: str,
        total_processed: int,
        is_eod: bool,
        missing_dates: list[Any],
        reloaded_symbols: list[str],
        failed_reloads: list[str],
    ) -> None:
        """Gửi báo cáo tổng hợp chi tiết sau phiên chạy thành công.

        Args:
            date_str (str): Ngày giao dịch.
            total_processed (int): Tổng số dòng dữ liệu thô T0.
            is_eod (bool): Trạng thái chốt phiên cuối ngày EOD.
            missing_dates (list[Any]): Các ngày giao dịch còn thiếu.
            reloaded_symbols (list[str]): Các mã đã tải lại thành công.
            failed_reloads (list[str]): Các mã lỗi reload.
        """

        def _format_symbols(symbols: list[str], max_show: int = 15) -> str:
            if not symbols:
                return "Không có"
            if len(symbols) <= max_show:
                return ", ".join(symbols)
            return (
                ", ".join(symbols[:max_show]) + f" (+{len(symbols) - max_show} mã khác)"
            )

        status_icon: str = "✅" if not failed_reloads else "⚠️"
        eod_status: str = "Đã chốt EOD 🔒" if is_eod else "Chưa chốt EOD 🔓"

        missing_dates_str: str = (
            ", ".join([str(d) for d in missing_dates]) if missing_dates else "Không có"
        )
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

        formatted_msg += f"\n⏰ <i>Thời gian chạy: {datetime.now(Config.VN_TZ).strftime('%Y-%m-%d %H:%M:%S')}</i>"

        self.send_message(formatted_msg)

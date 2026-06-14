from datetime import datetime
import logging
import os

from config import Config
from extractors.extractor_vnstock import VnstockExtractorETL
from notifier import Notifier
from utils import setup_logger


def main() -> None:
    """Hàm khởi động chính của Vnstock Daily Pipeline.

    Hàm thực hiện cấu hình Logger, áp dụng các chốt chặn cuối tuần và ngày lễ nghỉ giao dịch
    của thị trường chứng khoán Việt Nam, sau đó kích hoạt luồng chạy daily tự động.
    """
    logger: logging.Logger = setup_logger(Config.DEFAULT_LOGGER_NAME)
    logger.info("[bold cyan]🚀 === KHỞI ĐỘNG PIPELINE (DAILY MODE) ===[/bold cyan]")

    if "K_SERVICE" in os.environ and "K_JOB" not in os.environ:
        logger.warning(
            "[yellow]⚠️ CẢNH BÁO: Phát hiện đang chạy dưới dạng Cloud Run Service. "
            "Cơ chế sleep của Rate Limiter có thể bị ảnh hưởng bởi việc throttle CPU. "
            "Khuyến nghị triển khai dưới dạng Cloud Run Job.[/yellow]"
        )

    try:
        # Chốt chặn thời gian: Bỏ qua chạy tự động vào các ngày cuối tuần và ngày lễ
        today_date: datetime = datetime.now(Config.VN_TZ)

        # 1. Chặn Thứ 7 và Chủ Nhật theo hàm weekday()
        if today_date.weekday() >= 5 and not Config.FORCE_RUN:
            skip_msg: str = (
                f"⏸️ Bỏ qua chạy DAILY: Hôm nay là cuối tuần ({today_date.strftime('%A')}). "
                "Thị trường đóng cửa."
            )
            logger.warning(f"[yellow]{skip_msg}[/yellow]")
            return

        # 2. Chặn các ngày nghỉ lễ của Việt Nam theo lịch cấu hình
        today_date_str: str = today_date.strftime("%Y-%m-%d")
        if today_date_str in Config.get_vn_holiday_dates() and not Config.FORCE_RUN:
            skip_msg = (
                f"⏸️ Bỏ qua chạy DAILY: Hôm nay ({today_date_str}) là ngày nghỉ Lễ Quốc gia. "
                "Thị trường đóng cửa."
            )
            logger.warning(f"[yellow]{skip_msg}[/yellow]")
            return

        extractor: VnstockExtractorETL = VnstockExtractorETL()
        extractor.run()

    except Exception as e:
        logger.error(f"🚨 Sự cố nghiêm trọng sập hệ thống toàn cục: {e}", exc_info=True)
        try:
            Notifier(logger).send_alert(
                "Sập Hệ thống Toàn cục (main.py)", f"{type(e).__name__}: {str(e)}"
            )
        except Exception as notify_err:
            logger.error(f"❌ Không thể gửi thông báo lỗi: {notify_err}")


if __name__ == "__main__":
    main()

import logging
from datetime import datetime
from config import Config
from extractors.extractor_vnstock import VnstockExtractorETL
from utils import setup_logger
from notifier import Notifier


def main() -> None:
    """Hàm khởi động chính của Vnstock Daily Pipeline.

    Hàm thực hiện cấu hình Logger, áp dụng các chốt chặn cuối tuần và ngày lễ nghỉ giao dịch
    của thị trường chứng khoán Việt Nam, sau đó kích hoạt luồng chạy daily tự động.
    """
    logger: logging.Logger = setup_logger(Config.DEFAULT_LOGGER_NAME)
    logger.info("[bold cyan]🚀 === KHỞI ĐỘNG PIPELINE (DAILY MODE) ===[/bold cyan]")

    try:
        # Chốt chặn thời gian: Bỏ qua chạy tự động vào các ngày cuối tuần và ngày lễ
        today_date: datetime = datetime.now(Config.VN_TZ)

        # 1. Chặn Thứ 7 và Chủ Nhật theo hàm weekday()
        if today_date.weekday() >= 5 and not Config.FORCE_RUN_WEEKEND:
            skip_msg: str = (
                f"⏸️ Bỏ qua chạy DAILY: Hôm nay là cuối tuần ({today_date.strftime('%A')}). "
                "Thị trường đóng cửa."
            )
            logger.warning(f"[yellow]{skip_msg}[/yellow]")
            return

        # 2. Chặn các ngày nghỉ lễ của Việt Nam theo lịch cấu hình
        today_date_str: str = today_date.strftime("%Y-%m-%d")
        if today_date_str in Config.VN_HOLIDAY_DATES and not Config.FORCE_RUN_WEEKEND:
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

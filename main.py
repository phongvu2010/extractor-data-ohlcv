from datetime import datetime
from config import Config
from extractors import VnstockExtractorETL
from utils import setup_logger
from notifier import Notifier


def main() -> None:
    # 1. Setup hệ thống ghi chú (Logging)
    logger = setup_logger(Config.DEFAULT_LOGGER_NAME)
    logger.info(f"[bold cyan]🚀 === KHỞI ĐỘNG PIPELINE (DAILY MODE) ===[/bold cyan]")

    try:
        # ---------------------------------------------------------
        # 🛡️ CHỐT CHẶN (GUARD CLAUSE): BỎ QUA CUỐI TUẦN & NGÀY LỄ
        # ---------------------------------------------------------
        # Chỉ áp dụng chặn cho luồng DAILY. Luồng HISTORICAL thường được
        # chạy thủ công (chạy bù) nên vẫn cho phép chạy vào cuối tuần.
        today_date = datetime.now(Config.VN_TZ)

        # 1. Chặn Thứ 7 (5) và Chủ Nhật (6) theo hàm weekday()
        if today_date.weekday() >= 5 and not Config.FORCE_RUN_WEEKEND:
            skip_msg = f"⏸️ Bỏ qua chạy DAILY: Hôm nay là cuối tuần ({today_date.strftime('%A')}). Thị trường đóng cửa."
            logger.warning(f"[yellow]{skip_msg}[/yellow]")
            return

        # 2. Chặn các ngày nghỉ Lễ của Việt Nam
        today_date_str = today_date.strftime("%Y-%m-%d")
        if today_date_str in Config.VN_HOLIDAY_DATES and not Config.FORCE_RUN_WEEKEND:
            skip_msg = f"⏸️ Bỏ qua chạy DAILY: Hôm nay ({today_date_str}) là ngày nghỉ Lễ Quốc gia. Thị trường đóng cửa."
            logger.warning(f"[yellow]{skip_msg}[/yellow]")
            return

        extractor = VnstockExtractorETL()
        extractor.run()

    except Exception as e:
        logger.error(f"🚨 Sự cố nghiêm trọng sập hệ thống toàn cục: {e}", exc_info=True)
        try:
            Notifier(logger).send_alert("Sập Hệ thống Toàn cục (main.py)", f"{type(e).__name__}: {str(e)}")
        except Exception as notify_err:
            logger.error(f"❌ Không thể gửi thông báo lỗi: {notify_err}")



if __name__ == "__main__":
    main()

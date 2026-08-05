import os
import time
import base64
import logging
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger("security_utils")

# FAIL-FAST SECURITY: AES kaliti environmentdan keladi, default YO'Q.
# Oldin shu yerda hardcoded fallback bor edi — CRITICAL zaiflik edi
# (GitHub'da kodni ko'rgan hujumchi tarmoqdagi trafikni decrypt qila olardi).
# Endi env berilmasa import-time da xato beradi — bu STRICT_SECURITY ga
# bog'liq emas, chunki AES kaliti har doim talab qilinadi.
SHARED_SECRET_KEY = os.getenv("AES_256_KEY")
if not SHARED_SECRET_KEY:
    raise RuntimeError(
        "AES_256_KEY environment o'zgaruvchisi o'rnatilmagan. "
        "Tizim xavfsizlik sababli ishga tushmaydi. "
        "Yangi kalit generatsiya qilish: "
        "python3 -c \"import secrets; print(secrets.token_bytes(32).hex())\""
    )
# Mask format pin — versioned, so future migrations (v2, v3) remain
# self-describing and historical audit rows can still be parsed correctly.
# Format: "<version>:<prefix><truncated_sha256_hex>"
# V1: "v1:" + "kh:" + sha256[:12]
MASK_VERSION = "v1"
MASK_PREFIX = "kh:"
MASK_TRUNCATE_BYTES = 12
# Paketning maksimal yoshi (sekundlarda) — replay attack ga qarshi himoya
MAX_NONCE_AGE_SECONDS = int(os.getenv("MAX_NONCE_AGE_SECONDS", "300"))
# Oxirgi ishlatilgan nonce'lar (in-memory, TTL bilan)
_seen_nonces: dict[str, float] = {}


def get_key_bytes():
    """
    AES-256 kalitini 32 baytga normalizatsiya qiladi.

    Production'da AES_256_KEY environment orqali kiritilishi SHART (import-time
    RuntimeError bor). Bu funksiya ishlasa, demak kalit to'g'ri o'rnatilgan.
    Kalit entropiyasi < 256 bit bo'lsa ham xavfsizlik kuchsizlanadi — shuning
    uchun random 32-byte hex yoki base64 token_urlsafe(48) tavsiya qilamiz.
    """
    key = SHARED_SECRET_KEY.encode('utf-8')
    if len(key) < 32:
        key = key.ljust(32, b'0')
    elif len(key) > 32:
        key = key[:32]
    return key


def _purge_old_nonces(now: float):
    """#7 fix: Vaqtga asoslangan tozalash — har doim eski nonce'larni o'chiradi,
    faqat >5000 bo'lganda emas. Bu memory growth'ni oldini oladi."""
    cutoff = now - MAX_NONCE_AGE_SECONDS
    # Har safar tozalaymiz, faqat katta bo'lganda emas
    expired = [n for n, t in _seen_nonces.items() if t < cutoff]
    for n in expired:
        del _seen_nonces[n]
    # Safety net: agar 10,000 dan oshib ketsa (DDOS yoki vaqt drifti),
    # eng eski yarmini agressiv tozalash
    if len(_seen_nonces) > 10000:
        sorted_items = sorted(_seen_nonces.items(), key=lambda x: x[1])
        for n, _ in sorted_items[:len(sorted_items) // 2]:
            _seen_nonces.pop(n, None)
        log.warning(f"Nonce cache agressiv tozalandi: {len(_seen_nonces)} qoldi")


def encrypt_payload(data: bytes) -> str:
    """Ma'lumotni AES-256-GCM orqali shifrlaydi + timestamp qo'shib replay himoya.

    Format: base64( nonce(12) || timestamp(8) || ciphertext )
    """
    key = get_key_bytes()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ts = int(time.time()).to_bytes(8, "big")
    # Associated data (AAD) ga nonce o'zi va vaqtni bog'laydi
    ct = aesgcm.encrypt(nonce, data, ts)
    return base64.b64encode(nonce + ts + ct).decode('utf-8')


def decrypt_payload(encrypted_str: str) -> bytes:
    """Shifrlangan payload'ni ochadi, vaqt va replay tekshiruvi bilan."""
    key = get_key_bytes()
    aesgcm = AESGCM(key)
    encrypted_bytes = base64.b64decode(encrypted_str)
    if len(encrypted_bytes) < 12 + 8 + 16:  # nonce + ts + min tag
        raise ValueError("Shifrlangan payload juda qisqa")
    nonce = encrypted_bytes[:12]
    ts_bytes = encrypted_bytes[12:20]
    ts = int.from_bytes(ts_bytes, "big")
    ct = encrypted_bytes[20:]
    # Replay himoya — eski nonce'ni qayta ishlatmaslik
    now = time.time()
    if now - ts > MAX_NONCE_AGE_SECONDS or ts - now > 30:  # kelajakda ham cheklangan
        raise ValueError("Payload muddati o'tgan yoki vaqti noto'g'ri")
    nonce_key = nonce.hex()
    # Replay detection (in-memory)
    last_seen = _seen_nonces.get(nonce_key, 0)
    if last_seen and now - last_seen < MAX_NONCE_AGE_SECONDS:
        raise ValueError("Replay aniqlandi: nonce qayta ishlatilgan")
    _seen_nonces[nonce_key] = now
    _purge_old_nonces(now)
    # AAD = ts_bytes
    return aesgcm.decrypt(nonce, ct, ts_bytes)

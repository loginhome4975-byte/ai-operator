from typing import Dict, Any, Optional
import logging
from .base import BaseCRM

class DummySQLCRM(BaseCRM):
    """
    Beta sinovlari uchun simulyatsiya qilingan (Mock) Ma'lumotlar Bazasi.
    Haqiqiy tizim (masalan, PostgreSQL) ulanganda faqat shu klassning ichki logikasi o'zgaradi.
    """
    def __init__(self):
        # Vaqtincha xotirada (in-memory) mijozlar bazasi
        self.mock_db = {
            "998901234567": {
                "name": "Ali Valiyev",
                "tariff": "Tezkor-100",
                "balance": -15000.0,
                "status": "blocked",
                "address": "Toshkent sh, Yunusobod 4-kvartal, 15-uy"
            },
            "998991112233": {
                "name": "Zuhra Karimova",
                "tariff": "Premium-500",
                "balance": 120000.0,
                "status": "active",
                "address": "Samarqand sh, Registon ko'chasi, 8-uy"
            }
        }
        self.tickets = []
        logging.info("[CRM] DummySQL CRM tizimi ishga tushdi (Beta mode).")

    def get_client_info(self, phone_number: str) -> Optional[Dict[str, Any]]:
        logging.info(f"[CRM] Mijoz ma'lumotlari qidirilmoqda: {phone_number}")
        # Raqamdan ortiqcha belgilarni olib tashlaymiz
        clean_number = "".join(filter(str.isdigit, phone_number))
        if clean_number.startswith("998") and len(clean_number) == 12:
            pass # OK
        elif len(clean_number) == 9:
            clean_number = "998" + clean_number
            
        return self.mock_db.get(clean_number)

    def get_balance(self, phone_number: str) -> Optional[float]:
        client = self.get_client_info(phone_number)
        if client:
            return client.get("balance")
        return None

    def create_ticket(self, phone_number: str, issue_description: str, priority: str = "normal") -> bool:
        client = self.get_client_info(phone_number)
        if not client:
            logging.warning(f"[CRM] Ticket ochish bekor qilindi. Baza topilmadi: {phone_number}")
            return False
            
        ticket = {
            "phone": phone_number,
            "issue": issue_description,
            "priority": priority,
            "status": "open"
        }
        self.tickets.append(ticket)
        logging.info(f"[CRM] Yangi Ticket ochildi: {ticket}")
        return True

    def update_profile(self, phone_number: str, update_data: Dict[str, Any]) -> bool:
        client = self.get_client_info(phone_number)
        if not client:
            return False
        
        # Faqat ruxsat etilgan maydonlarni yangilaymiz (Xavfsizlik)
        allowed_keys = ["address", "email"]
        for k, v in update_data.items():
            if k in allowed_keys:
                # Real bazada SQL UPDATE ... WHERE phone = phone_number
                clean_number = "".join(filter(str.isdigit, phone_number))
                if len(clean_number) == 9: clean_number = "998" + clean_number
                self.mock_db[clean_number][k] = v
                
        logging.info(f"[CRM] Profil yangilandi: {phone_number} -> {update_data}")
        return True

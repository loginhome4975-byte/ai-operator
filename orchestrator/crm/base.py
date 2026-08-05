import abc
from typing import Dict, Any, Optional

class BaseCRM(abc.ABC):
    """
    Barcha CRM va Ma'lumotlar bazalari uchun umumiy interfeys (Shablon).
    Har qanday yangi haqiqiy tizim (AmoCRM, Bitrix, Custom SQL) shu klassdan meros olishi shart.
    """
    
    @abc.abstractmethod
    def get_client_info(self, phone_number: str) -> Optional[Dict[str, Any]]:
        """Mijozning telefon raqami orqali uning bazadagi barcha ma'lumotlarini tortib kelish."""
        pass

    @abc.abstractmethod
    def get_balance(self, phone_number: str) -> Optional[float]:
        """Mijozning hisobidagi mablag'ni tekshirish."""
        pass
        
    @abc.abstractmethod
    def create_ticket(self, phone_number: str, issue_description: str, priority: str = "normal") -> bool:
        """Texnik muammo bo'yicha zayavka (ticket) ochish."""
        pass

    @abc.abstractmethod
    def update_profile(self, phone_number: str, update_data: Dict[str, Any]) -> bool:
        """Mijozning ruxsat etilgan ma'lumotlarini yangilash."""
        pass

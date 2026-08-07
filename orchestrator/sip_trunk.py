import asyncio
import socket
import hashlib
import re
import time
import uuid
import logging

class SIPTrunkConfig:
    def __init__(self, username, password, domain, local_ip, local_port):
        self.username = username
        self.password = password
        self.domain = domain
        self.local_ip = local_ip
        self.local_port = local_port

def get_digest_auth_qop(username, password, realm, nonce, uri, cnonce, nc="00000001", qop="auth", method="REGISTER"):
    ha1 = hashlib.md5(f"{username}:{realm}:{password}".encode()).hexdigest()
    ha2 = hashlib.md5(f"{method}:{uri}".encode()).hexdigest()
    response = hashlib.md5(f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}".encode()).hexdigest()
    return response

async def register_loop(config, transport):
    # Audit fix: Call-ID registratsiyalar orasida BARQAROR bo'lishi kerak —
    # ba'zi SIP server'lar har re-register'da yangi Call-ID ni rad etadi.
    call_id = f"reg-{uuid.uuid4().hex[:12]}"
    cseq = 1
    # DNS cache — har registratsiyada qayta lookup qilmaslik
    _cached_ip = None
    _cache_ts = 0
    DNS_CACHE_TTL = 600  # 10 daqiqa

    def _resolve_sync():
        """Sinxron DNS resolver — asyncio.to_thread orqali ishga tushiriladi."""
        nonlocal _cached_ip, _cache_ts
        now = time.time()
        if _cached_ip and (now - _cache_ts) < DNS_CACHE_TTL:
            return _cached_ip
        _cached_ip = socket.gethostbyname(config.domain)
        _cache_ts = now
        return _cached_ip

    while True:
        try:
            sip_ip = await asyncio.to_thread(_resolve_sync)
            reg_msg1 = f"""REGISTER sip:{config.domain} SIP/2.0\r
Via: SIP/2.0/UDP {config.local_ip}:{config.local_port};branch=z9hG4bK-reg1-{cseq}\r
Max-Forwards: 70\r
From: <sip:{config.username}@{config.domain}>;tag=tag1\r
To: <sip:{config.username}@{config.domain}>\r
Call-ID: {call_id}\r
CSeq: {cseq} REGISTER\r
Contact: <sip:{config.username}@{config.local_ip}:{config.local_port}>\r
Expires: 300\r
Content-Length: 0\r\n\r\n"""
            
            addr = (sip_ip, 5060)
            transport.sendto(reg_msg1.encode(), addr)
            
            cseq += 1
            await asyncio.sleep(290)
        except Exception as e:
            logging.error(f"[TRUNK] Registration error: {e}")
            await asyncio.sleep(30)

def handle_401(data, config, transport, call_id, cseq, addr):
    resp = data.decode('utf-8', errors='ignore')
    nonce_match = re.search(r'nonce="([^"]+)"', resp)
    realm_match = re.search(r'realm="([^"]+)"', resp)
    opaque_match = re.search(r'opaque="([^"]+)"', resp)
    
    if nonce_match and realm_match:
        nonce = nonce_match.group(1)
        realm = realm_match.group(1)
        opaque = opaque_match.group(1) if opaque_match else ""
        
        cnonce = uuid.uuid4().hex[:16]
        nc = "00000001"
        qop = "auth"
        
        auth_response = get_digest_auth_qop(
            config.username, config.password, realm, nonce, f"sip:{config.domain}", cnonce, nc, qop
        )
        
        auth_header = f'Digest username="{config.username}", realm="{realm}", nonce="{nonce}", uri="sip:{config.domain}", response="{auth_response}", algorithm=MD5, cnonce="{cnonce}", nc={nc}, qop={qop}'
        if opaque:
            auth_header += f', opaque="{opaque}"'
            
        reg_msg2 = f"""REGISTER sip:{config.domain} SIP/2.0\r
Via: SIP/2.0/UDP {config.local_ip}:{config.local_port};branch=z9hG4bK-reg2-{cseq}\r
Max-Forwards: 70\r
From: <sip:{config.username}@{config.domain}>;tag=tag1\r
To: <sip:{config.username}@{config.domain}>\r
Call-ID: {call_id}\r
CSeq: {cseq} REGISTER\r
Contact: <sip:{config.username}@{config.local_ip}:{config.local_port}>\r
Authorization: {auth_header}\r
Expires: 300\r
Content-Length: 0\r\n\r\n"""

        transport.sendto(reg_msg2.encode(), addr)

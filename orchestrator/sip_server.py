import asyncio
import socket
import logging
import re
import os
import time
import struct

try:
    import audioop
except ImportError:
    pass

from orchestrator.stream_controller import stream_controller
from orchestrator.sip_trunk import SIPTrunkConfig, register_loop, handle_401

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

import random as _sip_random

class SSRCGenerator:
    """Har bir qo'ng'iroq uchun unikal tasodifiy SSRC yaratadi."""
    @staticmethod
    def generate() -> int:
        return _sip_random.randint(1, 0xFFFFFFFF)


class RTPProtocol(asyncio.DatagramProtocol):
    def __init__(self, call_id, stream_controller_callback, dtmf_callback, timeout_callback):
        self.call_id = call_id
        self.callback = stream_controller_callback
        self.dtmf_callback = dtmf_callback
        self.timeout_callback = timeout_callback
        self.transport = None
        self.remote_addr = None
        self.sequence_number = 0
        self.timestamp = 0
        self.play_task = None
        self.dtmf_buffer = set()
        self._ssrc = SSRCGenerator.generate()
        self.call_state = "MENU"
        self._marker_next = True   # RTP marker bit faqat birinchi paketda bo'ladi

    def connection_made(self, transport):
        self.transport = transport
        logging.info(f"[RTP] 🟢 Call {self.call_id} uchun RTP port ochildi: {transport.get_extra_info('sockname')}")

    def datagram_received(self, data, addr):
        if not self.remote_addr:
            self.remote_addr = addr
            logging.info(f"[RTP] 🔗 Mijoz bilan ulanish o'rnatildi: {addr}")
            if self.call_state == "MENU":
                self.play_file('orchestrator/menu.wav', max_loops=3)
        
        if len(data) >= 12:
            pt = data[1] & 0x7F
            payload = data[12:]
            
            if pt == 101:
                if len(payload) >= 4:
                    event = payload[0]
                    digit_map = {0:'0', 1:'1', 2:'2', 3:'3', 4:'4', 5:'5', 6:'6', 7:'7', 8:'8', 9:'9', 10:'*', 11:'#'}
                    if event in digit_map:
                        digit = digit_map[event]
                        if digit not in self.dtmf_buffer:
                            self.dtmf_buffer.add(digit)
                            if self.dtmf_callback:
                                self.dtmf_callback(self.call_id, digit)
                return

            if self.call_state == "ACTIVE":
                try:
                    pcm_data = audioop.ulaw2lin(payload, 2)
                    if self.callback:
                        self.callback(self.call_id, pcm_data)
                except Exception as e:
                    logging.warning(f"[RTP] Audio dekodlash xatolik call={self.call_id}: {e}")

    def play_file(self, filename, max_loops=0):
        self.stop_playback()
        self._marker_next = True
        self.play_task = asyncio.create_task(self._play_loop(filename, max_loops))

    def stop_playback(self):
        if self.play_task and not self.play_task.done():
            self.play_task.cancel()

    async def _play_loop(self, filename, max_loops):
        try:
            import wave
            loops = 0
            while True:
                if not os.path.exists(filename):
                    break
                with wave.open(filename, 'rb') as wf:
                    pcm_data = wf.readframes(wf.getnframes())
                
                chunk_size = 320
                for i in range(0, len(pcm_data), chunk_size):
                    chunk = pcm_data[i:i+chunk_size]
                    if len(chunk) < chunk_size:
                        continue
                    self.send_audio(chunk)
                    await asyncio.sleep(0.02)
                
                loops += 1
                if max_loops > 0 and loops >= max_loops:
                    break
            
            if self.call_state == "MENU" and self.timeout_callback:
                self.timeout_callback(self.call_id)
                
        except asyncio.CancelledError:
            pass

    def send_audio(self, pcm_data):
        if self.transport and self.remote_addr:
            try:
                ulaw_data = audioop.lin2ulaw(pcm_data, 2)
                self.sequence_number = (self.sequence_number + 1) % 65536
                self.timestamp = (self.timestamp + len(ulaw_data)) & 0xFFFFFFFF
                # Audit fix: marker bit (0x80) faqat birinchi paketda bo'ladi
                marker = 0x80 if self._marker_next else 0x00
                self._marker_next = False
                header = struct.pack("!BBHII", 0x80 | marker, 0x00, self.sequence_number, self.timestamp, self._ssrc)
                self.transport.sendto(header + ulaw_data, self.remote_addr)
            except Exception as e:
                logging.warning(f"[RTP] Audio jo'natishda xatolik call={self.call_id}: {e}")


class SIPLogic:
    def __init__(self, local_ip=None, local_port=None, trunk_config=None):
        self.local_ip = local_ip if local_ip is not None else os.environ.get("SERVER_IP", "0.0.0.0")
        self.local_port = int(local_port if local_port is not None else os.environ.get("SIP_LOCAL_PORT", "8081"))
        self.trunk_config = trunk_config
        self.active_calls = {}
        self.rtp_port_counter = 10000

    def handle_sip_message(self, data, addr, transport, is_tcp=False):
        try:
            msg = data.decode('utf-8', errors='ignore')
        except Exception:
            return
            
        lines = msg.split('\r\n')
        if not lines:
            return
        
        first_line = lines[0]
        headers = {}
        for line in lines[1:]:
            if ':' in line:
                key, val = line.split(':', 1)
                headers[key.strip().lower()] = val.strip()

        call_id = headers.get('call-id')
        if not call_id:
            return

        if first_line.startswith('SIP/2.0 401 Unauthorized') and self.trunk_config:
            cseq = headers.get('cseq', '1').split()[0]
            handle_401(data, self.trunk_config, transport, call_id, int(cseq), addr)
            return

        if first_line.startswith('SIP/2.0 200 Registration successful'):
            logging.info("[TRUNK] 🟢 Muvaffaqiyatli ro'yxatdan o'tildi (Linphone.org).")
            return

        if first_line.startswith('INVITE'):
            logging.info(f"[SIP] 📞 Yangi qo'ng'iroq (INVITE): {call_id} | {addr}")
            asyncio.create_task(self.handle_invite(msg, headers, addr, call_id, transport, is_tcp))
            
        elif first_line.startswith('CANCEL'):
            # Audit fix: CANCEL ishlanmay qolmasin — 487 qaytaramiz va call tozalanadi
            logging.info(f"[SIP] ❌ Qo'ng'iroq bekor qilindi (CANCEL): {call_id}")
            self.handle_cancel(headers, addr, call_id, transport, is_tcp)
            
        elif first_line.startswith('BYE'):
            logging.info(f"[SIP] 🔴 Qo'ng'iroq yakunlandi (BYE): {call_id}")
            self.handle_bye(msg, headers, addr, call_id, transport, is_tcp)
            
        elif first_line.startswith('ACK'):
            pass

    def send_response(self, transport, response, addr, is_tcp):
        if is_tcp:
            transport.write(response.encode('utf-8'))
        else:
            transport.sendto(response.encode('utf-8'), addr)

    async def handle_invite(self, raw_msg, headers, addr, call_id, transport, is_tcp):
        local_rtp_port = self.rtp_port_counter
        # RTP port counter overflow protection: 10000-65534 range, cycle back
        self.rtp_port_counter += 2
        if self.rtp_port_counter >= 65530:
            self.rtp_port_counter = 10000
            logging.info("[SIP] RTP port counter cycled back to 10000") 
        
        loop = asyncio.get_running_loop()
        try:
            rtp_transport, rtp_protocol = await loop.create_datagram_endpoint(
                lambda: RTPProtocol(call_id, self.on_audio_stream, self.on_dtmf, self.on_menu_timeout),
                local_addr=('0.0.0.0', local_rtp_port)
            )
            self.active_calls[call_id] = {
                'rtp_transport': rtp_transport, 
                'protocol': rtp_protocol,
                'remote_addr': addr,
                'headers': headers,
                'sip_transport': transport,
                'is_tcp': is_tcp
            }
        except Exception as e:
            logging.error(f"[SIP] RTP xatolik: {e}")
            return
            
        via = headers.get('via', '')
        fro = headers.get('from', '')
        to = headers.get('to', '')
        cseq = headers.get('cseq', '')
        
        if ";tag=" not in to:
            to += f";tag=ai-operator-{int(time.time())}"
            self.active_calls[call_id]['to_with_tag'] = to

        sdp_body = (
            "v=0\r\n"
            f"o=AI-Operator {int(time.time())} IN IP4 {self.local_ip}\r\n"
            "s=AI Call\r\n"
            f"c=IN IP4 {self.local_ip}\r\n"
            "t=0 0\r\n"
            f"m=audio {local_rtp_port} RTP/AVP 0 101\r\n"
            "a=rtpmap:0 PCMU/8000\r\n"
            "a=sendrecv\r\n"
        )
        
        transport_param = ";transport=TCP" if is_tcp else ""
        response = (
            f"SIP/2.0 200 OK\r\n"
            f"Via: {via}\r\n"
            f"From: {fro}\r\n"
            f"To: {to}\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {cseq}\r\n"
            f"Contact: <sip:ai@{self.local_ip}:{self.local_port}{transport_param}>\r\n"
            f"Content-Type: application/sdp\r\n"
            f"Content-Length: {len(sdp_body)}\r\n"
            "\r\n"
            f"{sdp_body}"
        )
        
        self.send_response(transport, response, addr, is_tcp)

    def on_dtmf(self, call_id, digit):
        if call_id not in self.active_calls: return
        proto = self.active_calls[call_id]['protocol']
        
        if proto.call_state == "MENU":
            lang = "uz"
            if digit == '2': lang = "ru"
            elif digit == '3': lang = "en"
            
            proto.call_state = "WAITING"
            proto.dtmf_buffer.clear()  # audit fix: yangi call holatiga o'tishda tozalanadi
            proto._marker_next = True
            proto.play_file('orchestrator/wait.wav', max_loops=0)
            asyncio.create_task(self.init_llm_and_start(call_id, lang, proto))

    def on_menu_timeout(self, call_id):
        if call_id not in self.active_calls: return
        
        call_info = self.active_calls[call_id]
        addr = call_info['remote_addr']
        headers = call_info['headers']
        sip_trans = call_info['sip_transport']
        is_tcp = call_info['is_tcp']
        
        via = headers.get('via', '')
        fro = headers.get('from', '')
        to = call_info.get('to_with_tag', headers.get('to', ''))
        
        cseq_num = 1
        try:
            cseq_num = int(headers.get('cseq', '1 INVITE').split()[0]) + 1
        except Exception:
            pass

        bye_request = (
            f"BYE sip:client@{addr[0]}:{addr[1]} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {self.local_ip}:{self.local_port};branch=z9hG4bK-ai-{int(time.time())}\r\n"
            f"From: {to}\r\n"
            f"To: {fro}\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {cseq_num} BYE\r\n"
            f"Max-Forwards: 70\r\n"
            f"Content-Length: 0\r\n"
            "\r\n"
        )
        self.send_response(sip_trans, bye_request, addr, is_tcp)
        
        call_info['rtp_transport'].close()
        del self.active_calls[call_id]
        stream_controller.end_call(call_id)

    async def init_llm_and_start(self, call_id, lang, proto):
        try:
            session = stream_controller.get_or_create_session(call_id)
            session.language = lang
            await asyncio.sleep(2)
            proto.stop_playback()
            proto.call_state = "ACTIVE"
            stream_controller.trigger_greeting(call_id, proto.send_audio)
        except Exception as e:
            logging.error(f"[SIP] LLM/Stream init xatolik call={call_id}: {e}")
            # Recovery: MENU ga qaytish. Nested try — recovery o'zi xato qilsa ham
            # call abadiy WAITING da qolmasligi uchun.
            try:
                proto.stop_playback()
                proto.call_state = "MENU"
                proto.play_file('orchestrator/menu.wav', max_loops=3)
            except Exception as recovery_error:
                logging.critical(
                    f"[SIP] Recovery ham xato berdi call={call_id}: {recovery_error}. "
                    f"Call'ni majburiy yopamiz."
                )
                # To'g'ridan-to'g'ri tozalash — handle_bye ga bormaymiz
                if call_id in self.active_calls:
                    try:
                        self.active_calls[call_id]['rtp_transport'].close()
                    except Exception:
                        pass
                    del self.active_calls[call_id]
                    stream_controller.end_call(call_id)

    def handle_cancel(self, headers, addr, call_id, transport, is_tcp):
        """CANCEL: 487 Request Terminated qaytaramiz, dialoglarni tozalaymiz."""
        if call_id in self.active_calls:
            try:
                self.active_calls[call_id]['rtp_transport'].close()
            except Exception:
                pass
            del self.active_calls[call_id]
            stream_controller.end_call(call_id)
        via = headers.get('via', '')
        fro = headers.get('from', '')
        to = headers.get('to', '')
        cseq = headers.get('cseq', '')
        response = (
            f"SIP/2.0 487 Request Terminated\r\n"
            f"Via: {via}\r\n"
            f"From: {fro}\r\n"
            f"To: {to}\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {cseq}\r\n"
            "Content-Length: 0\r\n\r\n"
        )
        self.send_response(transport, response, addr, is_tcp)

    def handle_bye(self, raw_msg, headers, addr, call_id, transport, is_tcp):
        if call_id in self.active_calls:
            self.active_calls[call_id]['rtp_transport'].close()
            del self.active_calls[call_id]
            stream_controller.end_call(call_id)
            
        via = headers.get('via', '')
        fro = headers.get('from', '')
        to = headers.get('to', '')
        cseq = headers.get('cseq', '')
        
        response = (
            f"SIP/2.0 200 OK\r\n"
            f"Via: {via}\r\n"
            f"From: {fro}\r\n"
            f"To: {to}\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {cseq}\r\n"
            "Content-Length: 0\r\n\r\n"
        )
        self.send_response(transport, response, addr, is_tcp)

    def on_audio_stream(self, call_id, pcm_chunk):
        if call_id in self.active_calls:
            stream_controller.on_audio_chunk(call_id, pcm_chunk, self.active_calls[call_id]['protocol'].send_audio)

class SIPUDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, logic):
        self.logic = logic
    def connection_made(self, transport):
        self.transport = transport
        logging.info(f"[SIP] 🟢 UDP Server ishga tushdi: {self.logic.local_port}")
    def datagram_received(self, data, addr):
        self.logic.handle_sip_message(data, addr, self.transport, is_tcp=False)

class SIPTCPProtocol(asyncio.Protocol):
    def __init__(self, logic):
        self.logic = logic
        self.buffer = b""
    def connection_made(self, transport):
        self.transport = transport
        self.addr = transport.get_extra_info('peername')
    def data_received(self, data):
        self.buffer += data
        # Audit fix: bitta TCP paketda 2+ SIP xabari bo'lsa ham qayta ishlanadi
        while True:
            idx = self.buffer.find(b"\r\n\r\n")
            if idx < 0:
                break
            msg = self.buffer[:idx + 4]
            self.buffer = self.buffer[idx + 4:]
            try:
                self.logic.handle_sip_message(msg, self.addr, self.transport, is_tcp=True)
            except Exception as e:
                logging.warning(f"[SIP] TCP message xatosi: {e}")

def _detect_ip():
    """SERVER_IP berilmagan bo'lsa, tashqi ulanish orqali server IP'ni aniqlaydi.
    SDP'da 0.0.0.0 yuborilsa mijoz RTP jo'nata olmaydi — shu sababli autodetect kerak."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("0.0.0.0"):
            return ip
    except Exception:
        pass
    return "0.0.0.0"


async def main():
    loop = asyncio.get_running_loop()
    
    # Hardcoded SIP creds/IP olib tashlandi - env orqali o'qiladi.
    _u = os.environ.get("SIP_USERNAME", "")
    _p = os.environ.get("SIP_PASSWORD", "")
    if not _u or not _p:
        raise RuntimeError(
            "SIP_USERNAME va SIP_PASSWORD env o'zgaruvchilari talab qilinadi "
            "(linphone.org akkaunt ma'lumotlari)."
        )
    _sip_port = int(os.environ.get("SIP_LOCAL_PORT", "5060"))
    _server_ip = os.environ.get("SERVER_IP") or _detect_ip()
    trunk_config = SIPTrunkConfig(
        username=_u,
        password=_p,
        domain=os.environ.get("SIP_DOMAIN", "sip.linphone.org"),
        local_ip=_server_ip,
        local_port=_sip_port
    )
    
    logic = SIPLogic(local_ip=_server_ip, local_port=_sip_port, trunk_config=trunk_config)
    
    transport_udp, protocol_udp = await loop.create_datagram_endpoint(
        lambda: SIPUDPProtocol(logic),
        local_addr=('0.0.0.0', _sip_port)
    )
    
    server_tcp = await loop.create_server(
        lambda: SIPTCPProtocol(logic),
        '0.0.0.0', _sip_port
    )
    
    logging.info(f"[SIP] 🟢 TCP Server ishga tushdi: {_sip_port}")
    
    asyncio.create_task(register_loop(trunk_config, transport_udp))
    
    try:
        await asyncio.sleep(3600*24)
    except KeyboardInterrupt:
        pass
    finally:
        transport_udp.close()
        server_tcp.close()

if __name__ == "__main__":
    asyncio.run(main())

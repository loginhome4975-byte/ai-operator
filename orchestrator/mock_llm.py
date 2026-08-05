import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from orchestrator.security_utils import encrypt_payload, decrypt_payload

logging.basicConfig(level=logging.INFO)

class MockLLMHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/v1/chat/completions':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            data = json.loads(post_data.decode('utf-8'))
            enc_payload = data.get("encrypted_payload")
            
            if not enc_payload:
                self.send_response(400)
                self.end_headers()
                return
                
            try:
                dec_payload = decrypt_payload(enc_payload).decode('utf-8')
                chat_data = json.loads(dec_payload)
            except Exception:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"error": "decrypt failed"}')
                return
                
            messages = chat_data.get("messages", [])
            last_msg = messages[-1]["content"] if messages and messages[-1].get("content") else ""
            role = messages[-1].get("role", "") if messages else ""
            
            response_data = {"response": "Mock javob"}
            
            if role == "tool":
                response_data = {"response": f"Sizning so'rovingiz bo'yicha ma'lumot oldim: {messages[-1].get('content')}"}
            elif last_msg and "balans" in last_msg.lower():
                response_data = {
                    "tool_calls": [
                        {
                            "id": "call_mock123",
                            "type": "function",
                            "function": {
                                "name": "get_client_info",
                                "arguments": json.dumps({"phone_number": "+998901234567"})
                            }
                        }
                    ]
                }
            elif last_msg == "ping":
                response_data = {"response": "pong"}
            else:
                response_data = {"response": "Bu Mock LLM serveridan kelgan oddiy javob."}
                
            enc_resp = encrypt_payload(json.dumps(response_data).encode('utf-8'))
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"encrypted_payload": enc_resp}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

def run(server_class=HTTPServer, handler_class=MockLLMHandler, port=9000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info(f'Mock LLM Starting on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()

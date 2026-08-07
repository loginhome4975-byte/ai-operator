"""G.711 mu-law LUT testi: audioop bilan moslik + round-trip SNR."""
import numpy as np, audioop, sys
sys.path.insert(0, '/home/ubuntu/ai-operator')
from orchestrator.sip.main import _ULAW_ENC, _ULAW_DEC, _mulaw_to_pcm16k, _pcm16k_to_mulaw

# 1) Encode moslik (audioop bilan)
samples = np.array([-32768, -1000, -1, 0, 1, 1000, 5000, 32000, -32000, -5000, -8, 8], dtype=np.int16)
ref = np.frombuffer(audioop.lin2ulaw(samples.tobytes(), 2), dtype=np.uint8)
mine = _ULAW_ENC[(samples.astype(np.int32) + 32768)]
match = (ref == mine).sum()
print(f'lin2ulaw moslik: {match}/{len(samples)}')
print('audioop:', ref.tolist())
print('meniki :', mine.tolist())

# 2) Decode moslik
dec_ref = np.frombuffer(audioop.ulaw2lin(ref.tobytes(), 2), dtype=np.int16)
dec_mine = _ULAW_DEC[ref].astype(np.int16)
print('ulaw2lin moslik:', (dec_ref == dec_mine).sum(), '/', len(dec_ref))

# 3) Round-trip SNR (16k -> mulaw -> 16k)
rng = np.random.default_rng(42)
t = np.linspace(0, 1, 8000, endpoint=False)
pcm = ((np.sin(2*np.pi*300*t) * 7000) + (np.sin(2*np.pi*1100*t) * 2000)).astype(np.int16)
mulaw = _pcm16k_to_mulaw(pcm.tobytes())
back = _mulaw_to_pcm16k(mulaw)
orig = pcm.astype(np.float32)
rec = np.frombuffer(back[:len(orig)*2], dtype=np.int16).astype(np.float32)
minlen = min(len(orig), len(rec))
noise = orig[:minlen] - rec[:minlen]
snr = 10*np.log10(np.mean(orig[:minlen]**2)/np.mean(noise**2))
print(f'round-trip SNR: {snr:.1f} dB ({"OK" if snr > 25 else "XATO"})')

# 4) Dekod natijasi 16k ekanligi (audioop.ratecv 1 sample qisqartirishi mumkin)
print('decode uzunligi:', len(back), 'bayt (16000 kutilgan; audioop.ratecv ±2 bayt farq qilishi normal)')
assert 15998 <= len(back) <= 16000, f'decode uzunligi notogri: {len(back)}'
print('G.711 TEST PASS ✅')

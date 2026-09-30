from datetime import datetime, timezone
import asyncio,json,os,sqlite3,time,ipaddress,socket,re,base64,hashlib,struct,secrets
from pathlib import Path
from fastapi import FastAPI,HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel,Field
import httpx
import csv
import io
from collections import deque
from fastapi.responses import StreamingResponse

DATA=Path(os.getenv('DATA_DIR','/data')); DATA.mkdir(parents=True,exist_ok=True)
DB=DATA/'optimizer.db'; app=FastAPI(title='Bitaxe Optimizer'); miners={}; tasks={}; watchdog_tasks={}
DEFAULT={
 'asic_temp_target':55.0,'vr_temp_target':65.0,'fan_target':65.0,'fan_tolerance':5.0,'fan_ceiling':75.0,'efficiency_strategy':'band_efficiency','vr_cooling':'shared',
 'min_frequency':400.0,'max_frequency':600.0,'min_voltage':1000.0,'max_voltage':1150.0,
 'op_step':0.05,'max_asic_temp':65.0,'max_vr_temp':75.0,'max_error_pct':3.0,'max_reject_pct':1.0,
 'settle_seconds':30,'temp_deadband':0.5,'vr_temp_deadband':1.0,'fan_deadband':2.0,'recovery_seconds':90,
 'stability_voltage_step':15.0,'stability_probe_seconds':60,'stability_trim_decay':5.0,'lock_fv':False,'reject_min_shares':10,'trim_decay_error_fraction':0.5,'max_positive_trim':60.0
}
class MinerIn(BaseModel): name:str; host:str
class Settings(BaseModel):
 asic_temp_target:float=Field(55,ge=30,le=85); vr_temp_target:float=Field(65,ge=30,le=110); fan_target:float=Field(65,ge=0,le=100)
 fan_tolerance:float=Field(5,ge=0,le=30); fan_ceiling:float=Field(75,ge=1,le=100); efficiency_strategy:str='band_efficiency'
 vr_cooling:str='shared'; min_frequency:float=Field(400,gt=0); max_frequency:float=Field(600,gt=0)
 min_voltage:float=Field(1000,gt=0); max_voltage:float=Field(1150,gt=0); op_step:float=Field(.05,gt=0,le=.25)
 max_asic_temp:float=Field(65,ge=30,le=100); max_vr_temp:float=Field(75,ge=30,le=120)
 max_error_pct:float=Field(3,ge=0,le=100); max_reject_pct:float=Field(1,ge=0,le=100)
 settle_seconds:int=Field(30,ge=10,le=600); recovery_seconds:int=Field(90,ge=15,le=1800)
 temp_deadband:float=Field(.5,ge=0,le=10); vr_temp_deadband:float=Field(1,ge=0,le=15); fan_deadband:float=Field(2,ge=0,le=30)
 stability_voltage_step:float=Field(15,ge=1,le=100); stability_probe_seconds:int=Field(60,ge=15,le=900); stability_trim_decay:float=Field(5,ge=1,le=50); lock_fv:bool=False
 reject_min_shares:int=Field(10,ge=1,le=1000); trim_decay_error_fraction:float=Field(.5,ge=0,le=1); max_positive_trim:float=Field(60,ge=0,le=250)

def merged_settings(raw):
 raw=dict(raw or {})
 d=dict(DEFAULT); d.update(raw)
 if 'efficiency_strategy' not in raw:
  d['efficiency_strategy']='performance' if raw.get('headroom_priority')=='performance' else 'band_efficiency'
 return d
def con(): c=sqlite3.connect(DB);c.row_factory=sqlite3.Row;return c
def init():
 c=con();c.execute('CREATE TABLE IF NOT EXISTS miners(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT,host TEXT,settings TEXT)')
 c.execute('CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY AUTOINCREMENT,miner_id INTEGER,ts REAL,payload TEXT)')
 for r in c.execute('SELECT * FROM miners'):
  s=merged_settings(json.loads(r['settings'])); miners[r['id']]={'id':r['id'],'name':r['name'],'host':r['host'],'settings':s,'online':False,'telemetry':{},'reason':'Idle','mode':'paused','recovery_since':None,'last_shares':None,'stability_trim':0.0,'effsearch':{},'stable_since':None}
  c.execute('UPDATE miners SET settings=? WHERE id=?',(json.dumps(s),r['id']))
 c.commit();c.close()
def base(host):
 h=host.rstrip('/');return h if h.startswith(('http://','https://')) else 'http://'+h
async def getj(host,path):
 async with httpx.AsyncClient(timeout=6) as x: r=await x.get(base(host)+path);r.raise_for_status();return r.json()
async def patch(host,data):
 async with httpx.AsyncClient(timeout=6) as x: r=await x.patch(base(host)+'/api/system',json=data);r.raise_for_status();return r.json() if r.content else {}
def num(d,*ks):
 for k in ks:
  v=d.get(k)
  if isinstance(v,(int,float)): return float(v)
def norm(d,m=None):
 p=num(d,'power')
 # AxeOS reports hashRate/hashRate_1m in GH/s; normalize to TH/s.
 h_ghs=num(d,'hashRate_1m','hashRate')
 h=(h_ghs/1000.0) if h_ghs is not None else None
 a=num(d,'sharesAccepted');r=num(d,'sharesRejected')
 reject=0.0; reject_delta=0; accepted_delta=0; share_counter_reset=False
 if m is not None and a is not None and r is not None:
  prev=m.get('last_shares'); m['last_shares']=(a,r)
  if prev:
   if a < prev[0] or r < prev[1]:
    share_counter_reset=True; reject_samples=0
   else:
    accepted_delta=int(a-prev[0]); reject_delta=int(r-prev[1])
    reject_samples=accepted_delta+reject_delta
    reject=100*reject_delta/reject_samples if reject_samples>0 else 0.0
  else: reject_samples=0
 elif a is not None and r is not None and a+r>0:
  reject_samples=a+r
 else: reject_samples=0
 return {'temp':num(d,'temp'),'fan':num(d,'fanspeed'),'fan_rpm':num(d,'fanrpm'),'fan2_rpm':num(d,'fan2rpm'),
 'hashrate':h,'power':p,'jth':p/h if p and h and h>0 else None,'vr_temp':num(d,'vrTemp'),
 'error_pct':num(d,'errorPercentage') or 0.0,'accepted':a,'rejected':r,'accepted_delta':accepted_delta,'reject_delta':reject_delta,'reject_pct':reject,'reject_samples':reject_samples,'share_counter_reset':share_counter_reset,'asic_count':int(num(d,'asicCount') or 1),
 'frequency':num(d,'frequency'),'voltage':num(d,'coreVoltage'),'autofan':num(d,'autofanspeed')}
def op_to_fv(x,s):
 x=max(0,min(1,x)); return round(s['min_frequency']+x*(s['max_frequency']-s['min_frequency'])),round(s['min_voltage']+x*(s['max_voltage']-s['min_voltage']))
def fv_to_op(f,v,s):
 # Frequency defines the coupled baseline position. Voltage may intentionally
 # live above/below that baseline as an independently learned efficiency trim.
 if f is not None and s['max_frequency']>s['min_frequency']:
  return max(0,min(1,(f-s['min_frequency'])/(s['max_frequency']-s['min_frequency'])))
 return 0
def uses_reject_only(t):
 return int(t.get('asic_count') or 1) >= 4
def stable(t,s):
 if uses_reject_only(t):
  return int(t.get('reject_delta') or 0) == 0
 reject_ok=t.get('reject_samples',0)<s.get('reject_min_shares',10) or t.get('reject_pct',0)<=s['max_reject_pct']
 return t.get('error_pct',0)<=s['max_error_pct'] and reject_ok
def hard_hot(t,s): return (t.get('temp') is not None and t['temp']>=s['max_asic_temp']) or (t.get('vr_temp') is not None and t['vr_temp']>=s['max_vr_temp'])
def demands(t,s):
 asic=t.get('temp') is not None and t['temp']>s['asic_temp_target']+s['temp_deadband']
 vr=t.get('vr_temp') is not None and t['vr_temp']>s['vr_temp_target']+s['vr_temp_deadband']
 fan=t.get('fan')
 strategy=s.get('efficiency_strategy','band_efficiency')
 lower=max(0.0,s['fan_target']-s.get('fan_tolerance',5.0))
 upper=min(100.0,s['fan_target']+s.get('fan_tolerance',5.0))
 fan_limit=s.get('fan_ceiling',75.0) if strategy=='max_efficiency' else min(upper,s.get('fan_ceiling',75.0))
 fan_high=fan is not None and fan>fan_limit
 cool_asic=t.get('temp') is not None and t['temp']<s['asic_temp_target']-s['temp_deadband']
 cool_vr=t.get('vr_temp') is None or t['vr_temp']<s['vr_temp_target']-s['vr_temp_deadband']
 fan_low=fan is not None and fan<lower and strategy in ('band_efficiency','performance')
 return asic,vr,fan_high,cool_asic,cool_vr,fan_low
optimizer_history=deque(maxlen=20000)

def record_optimizer_snapshot(m):
 t=m.get('telemetry') or {}; st=m.get('settings') or {}
 optimizer_history.append({
  'timestamp':datetime.now(timezone.utc).isoformat(),
  'miner_name':m.get('name',''),'host':m.get('host',''),'reason':m.get('reason',''),
  'asic_temp_c':t.get('temp'),'vrm_temp_c':t.get('vr_temp'),'fan_pct':t.get('fan'),
  'asic_count':t.get('asic_count'),'accepted_total':t.get('accepted'),'rejected_total':t.get('rejected'),
  'accepted_delta':t.get('accepted_delta'),'rejected_delta':t.get('reject_delta'),
  'hashrate_ths':t.get('hashrate'),'power_w':t.get('power'),'j_th':t.get('jth'),
  'hw_error_pct':t.get('error_pct'),'reject_pct':t.get('reject_pct'),
  'frequency_mhz':t.get('frequency'),'core_voltage_mv':t.get('voltage'),
  'asic_target_c':st.get('asic_temp_target'),'vrm_target_c':st.get('vr_temp_target'),
  'fan_target_pct':st.get('fan_target'),'fan_tolerance_pct':st.get('fan_tolerance'),'fan_ceiling_pct':st.get('fan_ceiling'),'efficiency_strategy':st.get('efficiency_strategy')
 })

@app.get('/api/optimizer-log.csv')
async def optimizer_log_csv():
 fields=['timestamp','miner_name','host','reason','asic_temp_c','vrm_temp_c','fan_pct','asic_count','accepted_total','rejected_total','accepted_delta','rejected_delta',
 'hashrate_ths','power_w','j_th','hw_error_pct','reject_pct','frequency_mhz','core_voltage_mv',
 'asic_target_c','vrm_target_c','fan_target_pct','fan_tolerance_pct','fan_ceiling_pct','efficiency_strategy']
 out=io.StringIO()
 writer=csv.DictWriter(out,fieldnames=fields); writer.writeheader()
 writer.writerows(list(optimizer_history))
 return StreamingResponse(iter([out.getvalue()]),media_type='text/csv',
  headers={'Content-Disposition':'attachment; filename="bitaxe-optimizer-log.csv"'})

def target_score(t,s):
 # ASIC temperature and fan utilization are equal optimization objectives.
 # Values inside their configured deadbands count as on-target (zero penalty).
 vals=[]
 if t.get('temp') is not None:
  e=max(0,abs(t['temp']-s['asic_temp_target'])-s['temp_deadband'])
  vals.append(e/max(1,s['asic_temp_target']*.10))
 if t.get('fan') is not None:
  e=max(0,abs(t['fan']-s['fan_target'])-s['fan_deadband'])
  vals.append(e/20.0)
 return sum(vals)/len(vals) if vals else 1e9
async def set_fv(m,f,v):
 s=m['settings'];f=round(max(s['min_frequency'],min(s['max_frequency'],f)));v=round(max(s['min_voltage'],min(s['max_voltage'],v)))
 await patch(m['host'],{'overclockEnabled':1,'frequency':f,'coreVoltage':v});return f,v
async def set_op(m,x):
 # Coupled baseline move (used for fan/thermal demand) while preserving the
 # learned signed voltage delta. This prevents later moves from erasing an
 # efficient undervolt.
 f,v=op_to_fv(x,m['settings']);trim=m.get('stability_trim',0)
 return await set_fv(m,f,v+trim)
async def auto_fan(m,on=True): await patch(m['host'],{'autofanspeed':1 if on else 0})

# OctAxe individual-chip watchdog.
# Deliberately isolated from /api/system/info polling and optimizer logic.
# AxeOS realtime logs are streamed at ws://<miner>/api/ws.
ASIC_WATCH_THRESHOLD=0.70
ASIC_WATCH_SAMPLES=3
ASIC_WATCH_RECOVERY_HOLD=300
OCTAXE_RECOVERY_FREQUENCY=600
OCTAXE_RECOVERY_VOLTAGE=1150

def _host_only(host):
 h=host.strip().replace('http://','').replace('https://','')
 return h.split('/')[0].split(':')[0]

async def _ws_send_pong(writer,payload=b''):
 # WebSocket client frames must be masked.
 mask=secrets.token_bytes(4)
 n=len(payload)
 head=bytes([0x8A])
 if n<126: head+=bytes([0x80|n])
 elif n<65536: head+=bytes([0x80|126])+struct.pack('!H',n)
 else: head+=bytes([0x80|127])+struct.pack('!Q',n)
 masked=bytes(b ^ mask[i%4] for i,b in enumerate(payload))
 writer.write(head+mask+masked);await writer.drain()

async def _ws_read_frame(reader,writer):
 h=await reader.readexactly(2);opcode=h[0]&0x0f;masked=bool(h[1]&0x80);n=h[1]&0x7f
 if n==126:n=struct.unpack('!H',await reader.readexactly(2))[0]
 elif n==127:n=struct.unpack('!Q',await reader.readexactly(8))[0]
 mask=await reader.readexactly(4) if masked else None
 payload=await reader.readexactly(n) if n else b''
 if mask:payload=bytes(b ^ mask[i%4] for i,b in enumerate(payload))
 if opcode==9:
  await _ws_send_pong(writer,payload);return None
 if opcode==8:raise ConnectionError('WebSocket closed')
 if opcode in (1,2,0):return payload
 return None

async def _open_log_ws(host):
 ip=_host_only(host)
 reader,writer=await asyncio.wait_for(asyncio.open_connection(ip,80),timeout=6)
 key=base64.b64encode(secrets.token_bytes(16)).decode()
 request=(f'GET /api/ws HTTP/1.1\r\nHost: {ip}\r\nOrigin: http://{ip}\r\n'
          f'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n'
          'Sec-WebSocket-Version: 13\r\n\r\n')
 writer.write(request.encode());await writer.drain()
 status=(await asyncio.wait_for(reader.readline(),timeout=6)).decode(errors='ignore')
 headers={}
 while True:
  line=await asyncio.wait_for(reader.readline(),timeout=6)
  if line in (b'\r\n',b'\n',b''):break
  text=line.decode(errors='ignore').strip()
  if ':' in text:
   k,v=text.split(':',1);headers[k.lower().strip()]=v.strip()
 if '101' not in status:
  writer.close();await writer.wait_closed();raise ConnectionError(f'WebSocket handshake failed: {status.strip()}')
 expected=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
 if headers.get('sec-websocket-accept')!=expected:
  writer.close();await writer.wait_closed();raise ConnectionError('WebSocket accept mismatch')
 return reader,writer

def _chip_rates_from_log(text):
 # Example:
 # hashrate_monitor: chip hashrates: 1075.46GH/s / ... / 1081.47GH/s  (total: ...)
 if 'hashrate_monitor:' not in text or 'chip hashrates:' not in text:return None
 tail=text.split('chip hashrates:',1)[1].split('(total:',1)[0]
 vals=[float(x) for x in re.findall(r'([0-9]+(?:\.[0-9]+)?)\s*GH/s',tail)]
 return vals if len(vals)==8 else None

async def chip_watchdog(mid):
 bad=[0]*8
 while mid in miners:
  m=miners[mid];writer=None
  try:
   reader,writer=await _open_log_ws(m['host'])
   m['chip_watchdog']={'connected':True,'threshold_pct':70,'required_samples':3,'rates':[],'status':'Monitoring'}
   while mid in miners:
    payload=await asyncio.wait_for(_ws_read_frame(reader,writer),timeout=90)
    if payload is None:continue
    text=payload.decode(errors='ignore')
    rates=_chip_rates_from_log(text)
    if rates is None:continue

    wd=m.setdefault('chip_watchdog',{})
    wd.update({'connected':True,'threshold_pct':70,'required_samples':3,'rates':rates,'last_update':time.time()})

    # Suppress fault decisions during a post-restart stabilization hold.
    if time.time()<wd.get('hold_until',0):
     bad=[0]*8;wd['status']='Post-restart stabilization hold';continue

    fault=None
    for i,h in enumerate(rates):
     peers=rates[:i]+rates[i+1:]
     peer_avg=sum(peers)/len(peers)
     ratio=h/peer_avg if peer_avg>0 else 1.0
     if ratio<ASIC_WATCH_THRESHOLD:bad[i]+=1
     else:bad[i]=0
     if bad[i]>=ASIC_WATCH_SAMPLES:
      fault=(i,h,peer_avg,ratio);break

    if fault:
     i,h,peer_avg,ratio=fault
     # Remember the operating point that produced the chip fault so efficiency
     # search cannot immediately walk back into it after recovery.
     tf=m.get('telemetry') or {}
     bad_f=tf.get('frequency');bad_v=tf.get('voltage')
     pf=m.setdefault('probe_fault',{})
     if bad_v is not None and bad_v < OCTAXE_RECOVERY_VOLTAGE:
      pf['unsafe_below_v']=bad_v
      pf['safe_min_v']=min(m['settings']['max_voltage'],bad_v+float(m['settings'].get('probe_fault_guard_mv',15)))
     if bad_f is not None and bad_f < OCTAXE_RECOVERY_FREQUENCY:
      pf['unsafe_below_f']=bad_f
      pf['safe_min_f']=min(m['settings']['max_frequency'],bad_f+float(m['settings'].get('probe_fault_guard_mhz',12.5)))
     wd.update({'status':'Restarting','fault_chip':i+1,'fault_rate':h,'peer_average':peer_avg,'fault_ratio_pct':ratio*100,
                'fault_time':time.time(),'unsafe_frequency':bad_f,'unsafe_voltage':bad_v,'recovery_pending':True})
     m['effsearch']={};m['efficiency_lock']=False
     m['reason']=f'ASIC #{i+1} fault — {h:.1f} GH/s vs {peer_avg:.1f} peer avg ({ratio*100:.0f}%); restarting AxeOS for stock recovery'
     async with httpx.AsyncClient(timeout=10) as c:
      r=await c.post(base(m['host'])+'/api/system/restart');r.raise_for_status()
     wd['hold_until']=time.time()+ASIC_WATCH_RECOVERY_HOLD
     bad=[0]*8
     # Restart will normally close this socket; reconnect cleanly either way.
     try:writer.close();await writer.wait_closed()
     except Exception:pass
     await asyncio.sleep(15)
     break
  except asyncio.CancelledError:
   if writer:
    try:writer.close();await writer.wait_closed()
    except Exception:pass
   raise
  except Exception as e:
   wd=m.setdefault('chip_watchdog',{})
   wd.update({'connected':False,'status':'Reconnecting','last_error':str(e)})
   # Crucially: watchdog failure never changes miner online state or normal telemetry.
   await asyncio.sleep(5)
  finally:
   if writer:
    try:writer.close();await writer.wait_closed()
    except Exception:pass

async def poll(mid):
 while mid in miners:
  m=miners[mid]
  try:
   t=norm(await getj(m['host'],'/api/system/info'),m);m['telemetry']=t;m['online']=True
   if uses_reject_only(t) and int(t.get('reject_delta') or 0)>0:
    m['reject_event_seq']=m.get('reject_event_seq',0)+int(t.get('reject_delta') or 0)
   wd=m.get('chip_watchdog') or {}
   if wd.get('recovery_pending'):
    # AxeOS is reachable again after the watchdog restart. Force the confirmed
    # NerdOCTAXE stock point before optimizer/search logic is allowed to continue.
    try:
     rs=m['settings']
     rf=round(rs['max_frequency']) if rs.get('lock_fv') else OCTAXE_RECOVERY_FREQUENCY
     rv=round(rs['max_voltage']) if rs.get('lock_fv') else OCTAXE_RECOVERY_VOLTAGE
     await patch(m['host'],{'overclockEnabled':1,'frequency':rf,'coreVoltage':rv})
     m['stability_trim']=0.0;m['effsearch']={};m['efficiency_lock']=False
     wd['recovery_pending']=False;wd['stock_recovery_applied']=time.time()
     wd['hold_until']=time.time()+ASIC_WATCH_RECOVERY_HOLD
     m['reason']=f'OctAxe recovered at stock {OCTAXE_RECOVERY_FREQUENCY} MHz / {OCTAXE_RECOVERY_VOLTAGE} mV — 5 minute stabilization hold'
    except Exception:
     # Leave recovery_pending set so the next normal poll retries safely.
     pass
   record_optimizer_snapshot(m)
   c=con();c.execute('INSERT INTO samples(miner_id,ts,payload) VALUES(?,?,?)',(mid,time.time(),json.dumps(t)));c.commit();c.close()
  except Exception:m['online']=False
  await asyncio.sleep(5)
# v3 optimizer: one decision per cycle, strict priority order.
# Safety > stability > cooling constraints > strategy > efficiency probing.
def _v3_state(m):
 return m.setdefault('engine',{'phase':'observe','stable_since':None,'action_until':0,'last_reject_seq':m.get('reject_event_seq',0),'probe':None,'fan_anomaly_since':None,'v_blocked':False,'search_cooldown_until':0})

def _actual_x(t,s):
 f=t.get('frequency')
 if f is None or s['max_frequency']<=s['min_frequency']: return 0.0
 return max(0.0,min(1.0,(f-s['min_frequency'])/(s['max_frequency']-s['min_frequency'])))

def _fan_bounds(s):
 return max(0.0,s['fan_target']-s.get('fan_tolerance',5.0)), min(100.0,s['fan_target']+s.get('fan_tolerance',5.0))

def _instability_event(m,t,s,st):
 if uses_reject_only(t):
  seq=m.get('reject_event_seq',0)
  if seq>st.get('last_reject_seq',0):
   st['last_reject_seq']=seq; return True,'rejected share'
  return False,''
 if t.get('error_pct',0)>s['max_error_pct']: return True,f'HW error {t.get("error_pct",0):.2f}%'
 return False,''

async def _v3_coupled(m,x,reason):
 s=m['settings']; st=_v3_state(m); x=max(0.0,min(1.0,x))
 f,v=op_to_fv(x,s); trim=float(m.get('stability_trim',0.0)); v+=trim
 f,v=await set_fv(m,f,v)
 st['action_until']=time.time()+s['settle_seconds'];st['stable_since']=None;st['probe']=None
 m['reason']=reason+f' — {f:.0f} MHz / {v:.0f} mV'
 return f,v

async def optimize(mid):
 m=miners[mid];m['mode']='optimizing';m['recovery_since']=None
 st=_v3_state(m)
 while mid in miners:
  s=m['settings'];t=m.get('telemetry',{});now=time.time()
  if not m.get('online') or not t:
   m['reason']='Waiting for telemetry';await asyncio.sleep(5);continue

  # Lock means F/V only. Never touch fan settings.
  if s.get('lock_fv'):
   lf=round(s['max_frequency']);lv=round(s['max_voltage'])
   if t.get('frequency') is None or t.get('voltage') is None or abs(t['frequency']-lf)>.5 or abs(t['voltage']-lv)>.5:
    try: await patch(m['host'],{'overclockEnabled':1,'frequency':lf,'coreVoltage':lv})
    except Exception: pass
   m['stability_trim']=0.0;st['probe']=None;st['stable_since']=None
   m['reason']=f'F/V locked at {lf} MHz / {lv} mV — fan unchanged';await asyncio.sleep(5);continue

  wd=m.get('chip_watchdog') or {}
  if wd.get('recovery_pending') or now<wd.get('hold_until',0):
   m['reason']='OctAxe recovery hold';await asyncio.sleep(5);continue

  x=_actual_x(t,s); temp=t.get('temp');vr=t.get('vr_temp');fan=t.get('fan');lower,upper=_fan_bounds(s)
  strategy=s.get('efficiency_strategy','band_efficiency')

  # 1) HARD SAFETY. Temperature is authoritative. Fan percentage alone is never a hard-safety trigger.
  if hard_hot(t,s):
   await _v3_coupled(m,0,'HARD thermal limit — minimum F/V')
   m['mode']='thermal_guard';await asyncio.sleep(s['settle_seconds']);continue

  # Do not make another F/V decision until the previous one has had time to settle.
  if now<st.get('action_until',0):
   m['reason']=f'Settling after adjustment — {max(0,st["action_until"]-now):.0f}s';await asyncio.sleep(min(5,max(1,st['action_until']-now)));continue

  # 2) STABILITY. 4+ ASICs use rejected-counter events only; smaller miners use HW error %.
  unstable,why=_instability_event(m,t,s,st)
  if unstable:
   st['probe']=None;st['stable_since']=None
   f=t.get('frequency') or op_to_fv(x,s)[0];v=t.get('voltage') or op_to_fv(x,s)[1]
   base_v=op_to_fv(x,s)[1];max_trim=float(s.get('max_positive_trim',60));allowed=min(s['max_voltage'],base_v+max_trim)
   if v+1 < allowed:
    nv=min(allowed,v+s['stability_voltage_step']);await set_fv(m,f,nv);m['stability_trim']=nv-base_v
    st['action_until']=time.time()+s['settle_seconds'];m['reason']=f'Stability: {why} — hold frequency, voltage ↑ to {nv:.0f} mV'
   else:
    nx=max(0,x-s['op_step']);m['stability_trim']=0.0;await _v3_coupled(m,nx,f'Stability: {why}; voltage headroom exhausted, F/V ↓')
   await asyncio.sleep(s['settle_seconds']);continue

  # Establish continuous stable time after every adjustment/settings change.
  if st.get('stable_since') is None: st['stable_since']=now

  # 3) COOLING. ASIC/VRM temperature is primary; fan is secondary cooling effort.
  asic_high=temp is not None and temp>s['asic_temp_target']+s['temp_deadband']
  asic_low=temp is not None and temp<s['asic_temp_target']-s['temp_deadband']
  vr_high=vr is not None and vr>s['vr_temp_target']+s['vr_temp_deadband']
  fan_limit=s['fan_ceiling'] if strategy=='max_efficiency' else min(upper,s['fan_ceiling'])
  fan_high=fan is not None and fan>fan_limit

  # Implausible PID state: very cool ASIC but high fan. Hold instead of throttling F/V.
  fan_anomaly=fan_high and temp is not None and temp<s['asic_temp_target']-max(2.0,s['temp_deadband']*2)
  if fan_anomaly:
   if st.get('fan_anomaly_since') is None: st['fan_anomaly_since']=now
   st['probe']=None
   m['reason']=f'Cooling observation — ASIC {temp:.1f}°C below target but fan {fan:.0f}% > {fan_limit:.0f}%; holding F/V'
   await asyncio.sleep(5);continue
  st['fan_anomaly_since']=None

  # Temperature above target always retreats. Fan-only retreat requires sustained high fan and ASIC not cool.
  if asic_high or vr_high or fan_high:
   nx=max(0,x-s['op_step']);st['probe']=None
   why=[]
   if asic_high: why.append(f'ASIC {temp:.1f}>{s["asic_temp_target"]+s["temp_deadband"]:.1f}°C')
   if vr_high: why.append(f'VRM {vr:.1f}>{s["vr_temp_target"]+s["vr_temp_deadband"]:.1f}°C')
   if fan_high: why.append(f'fan {fan:.0f}>{fan_limit:.0f}%')
   if nx<x-.0001:
    await _v3_coupled(m,nx,'Cooling: '+', '.join(why)+', coupled F/V ↓');await asyncio.sleep(s['settle_seconds']);continue
   m['reason']='Cooling demand at configured minimum F/V — holding minimum';await asyncio.sleep(5);continue

  # 4) STRATEGY. Never increase load merely to chase fan if ASIC is at/above target.
  if strategy in ('band_efficiency','performance') and fan is not None and fan<lower and asic_low and not vr_high:
   nx=min(1,x+s['op_step'])
   if nx>x+.0001:
    await _v3_coupled(m,nx,f'{"Performance" if strategy=="performance" else "Target band"}: cool ASIC + fan {fan:.0f}%<{lower:.0f}%, coupled F/V ↑')
    await asyncio.sleep(s['settle_seconds']);continue

  # Performance mode stops here when inside constraints.
  if strategy=='performance':
   m['reason']='Performance target balanced — holding current F/V';await asyncio.sleep(5);continue

  # 5) EFFICIENCY. Simple, reversible voltage-only probe. No competing search branches.
  probe=st.get('probe')
  f=t.get('frequency');v=t.get('voltage');j=t.get('jth')
  stable_for=now-st.get('stable_since',now)
  if probe:
   # Evaluate only after settling and enough fresh telemetry time.
   if now>=probe['evaluate_at'] and j is not None and f is not None and v is not None:
    good=(not asic_high and not vr_high and not fan_high)
    improved=good and j < probe['base_j']*0.995
    if improved:
     bx=_actual_x(t,s);m['stability_trim']=v-op_to_fv(bx,s)[1]
     if probe.get('kind')=='frequency': st['v_blocked']=False
     st['probe']=None;st['stable_since']=now
     m['reason']=f'Efficiency {probe.get("kind","voltage")} probe accepted — {probe["base_j"]:.2f}→{j:.2f} J/TH'
    else:
     await set_fv(m,probe['base_f'],probe['base_v']);m['stability_trim']=probe['base_trim']
     if probe.get('kind')=='voltage': st['v_blocked']=True
     else: st['search_cooldown_until']=now+max(120,s['stability_probe_seconds']*2)
     st['probe']=None;st['action_until']=time.time()+s['settle_seconds'];st['stable_since']=time.time()
     m['reason']=f'Efficiency {probe.get("kind","voltage")} probe rejected — restored {probe["base_f"]:.0f} MHz / {probe["base_v"]:.0f} mV'
    await asyncio.sleep(5);continue
  elif j is not None and f is not None and v is not None and stable_for>=s['stability_probe_seconds'] and now>=st.get('search_cooldown_until',0):
   step=max(5.0,min(15.0,float(s.get('stability_trim_decay',5))*2))
   nv=max(s['min_voltage'],v-step)
   if (not st.get('v_blocked')) and nv<v-.5:
    st['probe']={'kind':'voltage','base_f':f,'base_v':v,'base_j':j,'base_trim':m.get('stability_trim',0.0),'evaluate_at':now+s['settle_seconds']}
    await set_fv(m,f,nv);st['action_until']=now+s['settle_seconds']
    m['reason']=f'Efficiency probe — voltage ↓ {v:.0f}→{nv:.0f} mV at fixed {f:.0f} MHz'
    await asyncio.sleep(s['settle_seconds']);continue
   # Once voltage-down no longer helps, test a small frequency reduction at the known-stable voltage.
   fstep=max(6.25,(s['max_frequency']-s['min_frequency'])*min(.05,s['op_step']))
   nf=max(s['min_frequency'],f-fstep)
   if nf<f-.1:
    st['probe']={'kind':'frequency','base_f':f,'base_v':v,'base_j':j,'base_trim':m.get('stability_trim',0.0),'evaluate_at':now+s['settle_seconds']}
    await set_fv(m,nf,v);st['action_until']=now+s['settle_seconds']
    m['reason']=f'Efficiency probe — frequency ↓ {f:.0f}→{nf:.0f} MHz at fixed {v:.0f} mV'
    await asyncio.sleep(s['settle_seconds']);continue

  m['reason']='Stable inside constraints — holding current F/V'
  await asyncio.sleep(5)

def clean_host(host):
 return host.replace('http://','').replace('https://','').split('/')[0].split(':')[0]
def discovered_name(d,ip):
 for k in ('hostname','hostName','deviceName','name'):
  v=d.get(k)
  if isinstance(v,str) and v.strip(): return v.strip()
 model=d.get('deviceModel') or d.get('boardVersion') or d.get('ASICModel')
 return f'{model} ({ip})' if model else f'Bitaxe {ip.split(".")[-1]}'
async def probe_ip(ip,sem):
 async with sem:
  try:
   async with httpx.AsyncClient(timeout=httpx.Timeout(1.2,connect=.45)) as x:
    r=await x.get(f'http://{ip}/api/system/info')
    if r.status_code!=200:return None
    d=r.json()
    # Require AxeOS-like telemetry so random web devices are not offered.
    if not any(k in d for k in ('hashRate','hashRate_1m','ASICModel','frequency','coreVoltage')):return None
    t=norm(d)
    return {'host':ip,'name':discovered_name(d,ip),'model':d.get('deviceModel') or d.get('ASICModel') or d.get('boardVersion'),
            'hashrate':t.get('hashrate'),'temp':t.get('temp'),'power':t.get('power')}
  except Exception:return None

@app.on_event('startup')
async def startup():
 init()
 for mid in list(miners):
  asyncio.create_task(poll(mid));watchdog_tasks[mid]=asyncio.create_task(chip_watchdog(mid))
@app.get('/')
async def index():return FileResponse(Path(__file__).parent/'static/index.html')

def local_scan_networks():
 """Return likely LAN /24s. AxeOS probing decides which candidate is actually useful."""
 nets=[]
 def add(raw):
  try:
   n=ipaddress.ip_network(raw,strict=False)
   if n.version==4 and n.is_private and n.prefixlen==24 and n not in nets:nets.append(n)
  except Exception: pass

 # Try Linux routes first.
 try:
  import subprocess
  out=subprocess.check_output(['ip','-4','route'],text=True,timeout=2)
  for line in out.splitlines():
   p=line.split()
   if 'src' in p:
    ip=p[p.index('src')+1]
    if not ip.startswith(('10.21.','10.42.','172.17.','172.18.','172.19.','172.20.')):
     add(f'{ip}/24')
 except Exception: pass

 # Try hostname/interface addresses.
 try:
  for info in socket.getaddrinfo(socket.gethostname(),None,socket.AF_INET,socket.SOCK_STREAM):
   ip=info[4][0]
   if ipaddress.ip_address(ip).is_private and not ip.startswith(('10.21.','10.42.','172.17.','172.18.','172.19.','172.20.')):
    add(f'{ip}/24')
 except Exception: pass

 # Common home LANs. These are verified by AxeOS probes before being reported.
 # 192.168.1.x is intentionally first because it is the most common and is
 # confirmed working with this app's Advanced Scan.
 for raw in ('192.168.1.0/24','192.168.0.0/24','192.168.50.0/24','192.168.68.0/24','192.168.86.0/24'):
  add(raw)
 return nets

async def scan_networks(nets):
 sem=asyncio.Semaphore(48);found=[]
 for net in nets:
  batch=await asyncio.gather(*(probe_ip(str(ip),sem) for ip in net.hosts()))
  found.extend(d for d in batch if d)
 existing={clean_host(m['host']) for m in miners.values()};seen=set();out=[]
 for d in found:
  if d['host'] in seen:continue
  seen.add(d['host']);out.append({**d,'added':clean_host(d['host']) in existing})
 return out

@app.post('/api/scan')
async def scan():
 nets=local_scan_networks()
 for n in nets:
  devices=await scan_networks([n])
  if devices:
   return {'networks':[str(n)],'devices':devices}
 return {'networks':[str(n) for n in nets],'devices':[]}

class ManualScanIn(BaseModel): subnet:str

@app.post('/api/scan-manual')
async def scan_manual(x:ManualScanIn):
 try:
  raw=x.subnet.strip()
  if raw.count('.')==2 and '/' not in raw: raw += '.0/24'
  elif '/' not in raw:
   ip=ipaddress.ip_address(raw);raw=str(ipaddress.ip_network(f'{ip}/24',strict=False))
  n=ipaddress.ip_network(raw,strict=False)
  if n.version!=4 or not n.is_private or n.prefixlen<24: raise ValueError()
 except Exception: raise HTTPException(400,'Enter a private IPv4 /24, e.g. 192.168.1.0/24')
 return {'networks':[str(n)],'devices':await scan_networks([n])}

@app.get('/api/miners')
async def ls():return list(miners.values())
@app.post('/api/miners')
async def add(x:MinerIn):
 c=con();q=c.execute('INSERT INTO miners(name,host,settings) VALUES(?,?,?)',(x.name,x.host,json.dumps(DEFAULT)));mid=q.lastrowid;c.commit();c.close()
 miners[mid]={'id':mid,'name':x.name,'host':x.host,'settings':dict(DEFAULT),'online':False,'telemetry':{},'reason':'Starting telemetry','mode':'paused','recovery_since':None,'last_shares':None,'stability_trim':0.0,'effsearch':{},'stable_since':None};asyncio.create_task(poll(mid));watchdog_tasks[mid]=asyncio.create_task(chip_watchdog(mid));return miners[mid]
@app.delete('/api/miners/{mid}')
async def rem(mid:int):
 if mid not in miners:raise HTTPException(404)
 q=tasks.pop(mid,None)
 if q:q.cancel()
 w=watchdog_tasks.pop(mid,None)
 if w:w.cancel()
 c=con();c.execute('DELETE FROM samples WHERE miner_id=?',(mid,));c.execute('DELETE FROM miners WHERE id=?',(mid,));c.commit();c.close();miners.pop(mid);return {'ok':True}
@app.get('/api/miners/{mid}/settings')
async def gs(mid:int):
 if mid not in miners:raise HTTPException(404)
 return miners[mid]['settings']
@app.put('/api/miners/{mid}/settings')
async def ss(mid:int,x:Settings):
 if mid not in miners:raise HTTPException(404)
 d=x.model_dump()
 if d['vr_cooling'] not in ('shared','separate','passive'):raise HTTPException(400,'Invalid VRM cooling mode')
 if d['efficiency_strategy'] not in ('max_efficiency','band_efficiency','performance'):raise HTTPException(400,'Invalid efficiency strategy')
 if d['fan_target']-d['fan_tolerance']<0 or d['fan_target']+d['fan_tolerance']>100:raise HTTPException(400,'Fan target ± tolerance must stay within 0–100%')
 if d['fan_ceiling']<d['fan_target']+d['fan_tolerance']:raise HTTPException(400,'Fan ceiling must be at or above the top of the target band')
 if d['max_frequency']<=d['min_frequency'] or d['max_voltage']<=d['min_voltage']:raise HTTPException(400,'Maximums must exceed minimums')
 if d['max_asic_temp']<=d['asic_temp_target'] or d['max_vr_temp']<=d['vr_temp_target']:raise HTTPException(400,'Hard thermal maximums must exceed targets')
 was_locked=bool(miners[mid]['settings'].get('lock_fv'))
 old_settings=dict(miners[mid]['settings'])
 miners[mid]['settings']=d
 # v3: settings changes invalidate transient optimizer state; learned watchdog boundaries remain intact.
 miners[mid]['engine']={'phase':'observe','stable_since':None,'action_until':0,'last_reject_seq':miners[mid].get('reject_event_seq',0),'probe':None,'fan_anomaly_since':None,'v_blocked':False,'search_cooldown_until':0}
 miners[mid]['effsearch']={};miners[mid]['efficiency_lock']=False
 if d.get('lock_fv'):
  miners[mid]['effsearch']={};miners[mid]['efficiency_lock']=False;miners[mid]['stability_trim']=0.0
  await patch(miners[mid]['host'],{'overclockEnabled':1,'frequency':round(d['max_frequency']),'coreVoltage':round(d['max_voltage'])})
  miners[mid]['reason']=f'F/V locked at {d["max_frequency"]:.0f} MHz / {d["max_voltage"]:.0f} mV — fan control unchanged'
 elif was_locked:
  miners[mid]['effsearch']={};miners[mid]['efficiency_lock']=False;miners[mid]['stable_since']=time.time()
  miners[mid]['reason']='F/V lock released — optimizer control restored; fan control unchanged'
 c=con();c.execute('UPDATE miners SET settings=? WHERE id=?',(json.dumps(d),mid));c.commit();c.close();return d
@app.post('/api/miners/{mid}/optimize')
async def go(mid:int):
 if mid not in miners:raise HTTPException(404)
 if mid not in tasks or tasks[mid].done():tasks[mid]=asyncio.create_task(optimize(mid))
 return {'running':True}
@app.post('/api/miners/{mid}/stop')
async def stop(mid:int):
 q=tasks.pop(mid,None)
 if q:q.cancel()
 if mid in miners:miners[mid]['mode']='paused';miners[mid]['reason']='Optimizer paused'
 return {'running':False}

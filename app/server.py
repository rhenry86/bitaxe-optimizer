from datetime import datetime, timezone
import asyncio,json,os,sqlite3,time,ipaddress,socket
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
DB=DATA/'optimizer.db'; app=FastAPI(title='Bitaxe Optimizer'); miners={}; tasks={}
DEFAULT={
 'asic_temp_target':55.0,'vr_temp_target':65.0,'fan_target':65.0,'vr_cooling':'shared',
 'min_frequency':400.0,'max_frequency':600.0,'min_voltage':1000.0,'max_voltage':1150.0,
 'op_step':0.05,'max_asic_temp':65.0,'max_vr_temp':75.0,'max_error_pct':3.0,'max_reject_pct':1.0,
 'settle_seconds':30,'temp_deadband':0.5,'vr_temp_deadband':1.0,'fan_deadband':2.0,'recovery_seconds':90,
 'stability_voltage_step':15.0,'stability_probe_seconds':60,'stability_trim_decay':5.0
}
class MinerIn(BaseModel): name:str; host:str
class Settings(BaseModel):
 asic_temp_target:float=Field(55,ge=30,le=85); vr_temp_target:float=Field(65,ge=30,le=110); fan_target:float=Field(65,ge=0,le=100)
 vr_cooling:str='shared'; min_frequency:float=Field(400,gt=0); max_frequency:float=Field(600,gt=0)
 min_voltage:float=Field(1000,gt=0); max_voltage:float=Field(1150,gt=0); op_step:float=Field(.05,gt=0,le=.25)
 max_asic_temp:float=Field(65,ge=30,le=100); max_vr_temp:float=Field(75,ge=30,le=120)
 max_error_pct:float=Field(3,ge=0,le=100); max_reject_pct:float=Field(1,ge=0,le=100)
 settle_seconds:int=Field(30,ge=10,le=600); recovery_seconds:int=Field(90,ge=15,le=1800)
 temp_deadband:float=Field(.5,ge=0,le=10); vr_temp_deadband:float=Field(1,ge=0,le=15); fan_deadband:float=Field(2,ge=0,le=30)
 stability_voltage_step:float=Field(15,ge=1,le=100); stability_probe_seconds:int=Field(60,ge=15,le=900); stability_trim_decay:float=Field(5,ge=1,le=50)

def merged_settings(raw):
 d=dict(DEFAULT); d.update(raw or {}); return d
def con(): c=sqlite3.connect(DB);c.row_factory=sqlite3.Row;return c
def init():
 c=con();c.execute('CREATE TABLE IF NOT EXISTS miners(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT,host TEXT,settings TEXT)')
 c.execute('CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY AUTOINCREMENT,miner_id INTEGER,ts REAL,payload TEXT)')
 for r in c.execute('SELECT * FROM miners'):
  s=merged_settings(json.loads(r['settings'])); miners[r['id']]={'id':r['id'],'name':r['name'],'host':r['host'],'settings':s,'online':False,'telemetry':{},'reason':'Idle','mode':'paused','recovery_since':None,'last_shares':None,'stability_trim':0.0,'stable_since':None}
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
 reject=0.0
 if m is not None and a is not None and r is not None:
  prev=m.get('last_shares'); m['last_shares']=(a,r)
  if prev:
   da=max(0,a-prev[0]); dr=max(0,r-prev[1]); reject=100*dr/(da+dr) if da+dr>0 else 0.0
 elif a is not None and r is not None and a+r>0: reject=100*r/(a+r)
 return {'temp':num(d,'temp'),'fan':num(d,'fanspeed'),'fan_rpm':num(d,'fanrpm'),'fan2_rpm':num(d,'fan2rpm'),
 'hashrate':h,'power':p,'jth':p/h if p and h and h>0 else None,'vr_temp':num(d,'vrTemp'),
 'error_pct':num(d,'errorPercentage') or 0.0,'accepted':a,'rejected':r,'reject_pct':reject,
 'frequency':num(d,'frequency'),'voltage':num(d,'coreVoltage'),'autofan':num(d,'autofanspeed')}
def op_to_fv(x,s):
 x=max(0,min(1,x)); return round(s['min_frequency']+x*(s['max_frequency']-s['min_frequency'])),round(s['min_voltage']+x*(s['max_voltage']-s['min_voltage']))
def fv_to_op(f,v,s):
 # Frequency defines the coupled baseline position. Voltage may intentionally
 # live above/below that baseline as an independently learned efficiency trim.
 if f is not None and s['max_frequency']>s['min_frequency']:
  return max(0,min(1,(f-s['min_frequency'])/(s['max_frequency']-s['min_frequency'])))
 return 0
def stable(t,s): return t.get('error_pct',0)<=s['max_error_pct'] and t.get('reject_pct',0)<=s['max_reject_pct']
def hard_hot(t,s): return (t.get('temp') is not None and t['temp']>=s['max_asic_temp']) or (t.get('vr_temp') is not None and t['vr_temp']>=s['max_vr_temp'])
def demands(t,s):
 asic=t.get('temp') is not None and t['temp']>s['asic_temp_target']+s['temp_deadband']
 vr=t.get('vr_temp') is not None and t['vr_temp']>s['vr_temp_target']+s['vr_temp_deadband']
 fan=t.get('fan') is not None and t['fan']>s['fan_target']+s['fan_deadband']
 cool_asic=t.get('temp') is not None and t['temp']<s['asic_temp_target']-s['temp_deadband']
 cool_vr=t.get('vr_temp') is None or t['vr_temp']<s['vr_temp_target']-s['vr_temp_deadband']
 fan_low=t.get('fan') is not None and t['fan']<s['fan_target']-s['fan_deadband']
 return asic,vr,fan,cool_asic,cool_vr,fan_low
optimizer_history=deque(maxlen=20000)

def record_optimizer_snapshot(m):
 t=m.get('telemetry') or {}; st=m.get('settings') or {}
 optimizer_history.append({
  'timestamp':datetime.now(timezone.utc).isoformat(),
  'miner_name':m.get('name',''),'host':m.get('host',''),'reason':m.get('reason',''),
  'asic_temp_c':t.get('temp'),'vrm_temp_c':t.get('vr_temp'),'fan_pct':t.get('fan'),
  'hashrate_ths':t.get('hashrate'),'power_w':t.get('power'),'j_th':t.get('jth'),
  'hw_error_pct':t.get('error_pct'),'reject_pct':t.get('reject_pct'),
  'frequency_mhz':t.get('frequency'),'core_voltage_mv':t.get('voltage'),
  'asic_target_c':st.get('asic_temp_target'),'vrm_target_c':st.get('vr_temp_target'),
  'fan_target_pct':st.get('fan_target')
 })

@app.get('/api/optimizer-log.csv')
async def optimizer_log_csv():
 fields=['timestamp','miner_name','host','reason','asic_temp_c','vrm_temp_c','fan_pct',
 'hashrate_ths','power_w','j_th','hw_error_pct','reject_pct','frequency_mhz','core_voltage_mv',
 'asic_target_c','vrm_target_c','fan_target_pct']
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
async def poll(mid):
 while mid in miners:
  m=miners[mid]
  try:
   t=norm(await getj(m['host'],'/api/system/info'),m);m['telemetry']=t;m['online']=True;record_optimizer_snapshot(m)
   c=con();c.execute('INSERT INTO samples(miner_id,ts,payload) VALUES(?,?,?)',(mid,time.time(),json.dumps(t)));c.commit();c.close()
  except Exception:m['online']=False
  await asyncio.sleep(5)
async def optimize(mid):
 m=miners[mid];m['mode']='optimizing';m['recovery_since']=None
 while mid in miners:
  s=m['settings'];t=m.get('telemetry',{})
  if not m.get('online') or not t:m['reason']='Waiting for telemetry';await asyncio.sleep(5);continue
  x=fv_to_op(t.get('frequency'),t.get('voltage'),s);asic_hot,vr_hot,fan_high,cool_asic,cool_vr,fan_low=demands(t,s)
  if hard_hot(t,s):
   await set_op(m,0);await auto_fan(m,True);m['mode']='autofan';m['recovery_since']=None;m['reason']='Hard thermal limit — minimum F/V + AxeOS Auto Fan';await asyncio.sleep(s['settle_seconds']);continue
  if not stable(t,s):
   # Stability faults are usually insufficient voltage for the selected frequency.
   # Hold frequency and add voltage first; only reduce frequency when max voltage is reached.
   f=t.get('frequency') if t.get('frequency') is not None else op_to_fv(x,s)[0]
   v=t.get('voltage') if t.get('voltage') is not None else op_to_fv(x,s)[1]
   step=s['stability_voltage_step'];m['stable_since']=None;m['recovery_since']=None
   if v < s['max_voltage']-0.5:
    nv=min(s['max_voltage'],v+step);m['stability_trim']=nv-op_to_fv(x,s)[1]
    await set_fv(m,f,nv);m['reason']=f'Stability correction — holding {f:.0f} MHz, voltage ↑ to {nv:.0f} mV (trim {m["stability_trim"]:+.0f} mV; HW {t.get("error_pct",0):.2f}% / reject {t.get("reject_pct",0):.2f}%)';await asyncio.sleep(s['settle_seconds']);continue
   nx=max(0,x-s['op_step']);nf,bv=op_to_fv(nx,s);m['stability_trim']=s['max_voltage']-bv;await set_fv(m,nf,s['max_voltage']);m['reason']=f'Max voltage still unstable — frequency ↓ to {nf:.0f} MHz, holding {s["max_voltage"]:.0f} mV';await asyncio.sleep(s['settle_seconds']);continue
  # Stable operation: independently probe voltage DOWN while frequency stays fixed.
  # This searches for efficiency and is allowed to create a negative trim.
  if m.get('stable_since') is None:m['stable_since']=time.time()
  if time.time()-m['stable_since']>=s['stability_probe_seconds'] and t.get('error_pct',0)<s['max_error_pct'] and t.get('reject_pct',0)<s['max_reject_pct']:
   f=t.get('frequency') if t.get('frequency') is not None else op_to_fv(x,s)[0]
   v=t.get('voltage') if t.get('voltage') is not None else op_to_fv(x,s)[1]+m.get('stability_trim',0)
   nv=max(s['min_voltage'],v-s['stability_trim_decay'])
   if nv<v-0.5:
    m['stability_trim']=nv-op_to_fv(x,s)[1]
    await set_fv(m,f,nv);m['stable_since']=time.time();m['reason']=f'Efficiency probe — holding {f:.0f} MHz, voltage ↓ {v:.0f}→{nv:.0f} mV (trim {m["stability_trim"]:+.0f} mV; HW {t.get("error_pct",0):.2f}%/{s["max_error_pct"]:.2f}%)';await asyncio.sleep(s['settle_seconds']);continue
  # Shared cooling: either ASIC or VRM can trigger AxeOS fan takeover at minimum. Separate/passive VRM cannot be independently commanded by current AxeOS API.
  at_min=x<=0.01
  need_takeover=at_min and (asic_hot or (vr_hot and s['vr_cooling']=='shared'))
  if need_takeover:
   await set_op(m,0);await auto_fan(m,True);m['mode']='autofan';m['recovery_since']=None;m['reason']='Minimum F/V; shared thermal demand handed to AxeOS Auto Fan';await asyncio.sleep(s['settle_seconds']);continue
  if at_min and vr_hot and s['vr_cooling'] in ('separate','passive'):
   m['mode']='vr_guard';m['reason']='VRM above target at minimum F/V — holding minimum; independent VR fan control is not exposed by AxeOS';await asyncio.sleep(s['settle_seconds']);continue
  if m['mode'] in ('autofan','vr_guard'):
   recovered=cool_asic and cool_vr and (fan_low if s['vr_cooling']=='shared' else True)
   if not recovered:
    m['recovery_since']=None;m['reason']='Thermal recovery — holding minimum F/V';await asyncio.sleep(s['settle_seconds']);continue
   if m.get('recovery_since') is None:m['recovery_since']=time.time()
   elapsed=time.time()-m['recovery_since']
   if elapsed<s['recovery_seconds']:
    m['reason']=f'Stable recovery {elapsed:.0f}/{s["recovery_seconds"]}s — holding minimum F/V';await asyncio.sleep(min(s['settle_seconds'],15));continue
   m['mode']='optimizing';m['recovery_since']=None;m['reason']='Thermally recovered — resuming slow coupled F/V optimization'
  # Any target demand retreats F/V. Fan target matters in normal operation; VR target always protects VRM.
  if asic_hot or vr_hot or fan_high:
   nx=max(0,x-s['op_step']);await set_op(m,nx);m['stable_since']=time.time();m['reason']=f'Thermal/fan demand — coupled F/V ↓ to {nx*100:.0f}% while preserving voltage trim {m.get("stability_trim",0):+.0f} mV';await asyncio.sleep(s['settle_seconds']);continue
  if (not asic_hot) and (not vr_hot) and fan_low:
   nx=min(1,x+s['op_step']);await set_op(m,nx);m['stable_since']=time.time();m['reason']=f'Fan headroom — coupled F/V ↑ to {nx*100:.0f}% with learned voltage trim {m.get("stability_trim",0):+.0f} mV (fan {t.get("fan",0):.0f}% / {s["fan_target"]:.0f}% target)';await asyncio.sleep(s['settle_seconds']);continue
  m['reason']=f'Holding frequency; voltage trim {m.get("stability_trim",0):+.0f} mV — waiting for next efficiency probe'
  await asyncio.sleep(s['settle_seconds'])

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
 for mid in list(miners):asyncio.create_task(poll(mid))
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
 miners[mid]={'id':mid,'name':x.name,'host':x.host,'settings':dict(DEFAULT),'online':False,'telemetry':{},'reason':'Starting telemetry','mode':'paused','recovery_since':None,'last_shares':None,'stability_trim':0.0,'stable_since':None};asyncio.create_task(poll(mid));return miners[mid]
@app.delete('/api/miners/{mid}')
async def rem(mid:int):
 if mid not in miners:raise HTTPException(404)
 q=tasks.pop(mid,None)
 if q:q.cancel()
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
 if d['max_frequency']<=d['min_frequency'] or d['max_voltage']<=d['min_voltage']:raise HTTPException(400,'Maximums must exceed minimums')
 if d['max_asic_temp']<=d['asic_temp_target'] or d['max_vr_temp']<=d['vr_temp_target']:raise HTTPException(400,'Hard thermal maximums must exceed targets')
 miners[mid]['settings']=d;c=con();c.execute('UPDATE miners SET settings=? WHERE id=?',(json.dumps(d),mid));c.commit();c.close();return d
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

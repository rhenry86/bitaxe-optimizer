import asyncio,json,os,sqlite3,time
from pathlib import Path
from fastapi import FastAPI,HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel,Field
import httpx

DATA=Path(os.getenv("DATA_DIR","/data")); DATA.mkdir(parents=True,exist_ok=True)
DB=DATA/"optimizer.db"; app=FastAPI(title="Bitaxe Optimizer"); miners={}; tasks={}
DEFAULT={"asic_temp_target":60.0,"fan_target":40.0,"min_frequency":400.0,"max_frequency":600.0,
"min_voltage":1000.0,"max_voltage":1150.0,"op_step":0.05,"max_asic_temp":75.0,"max_vr_temp":85.0,
"max_error_pct":1.0,"max_reject_pct":1.0,"settle_seconds":30,"temp_deadband":0.5,"fan_deadband":2.0}

class MinerIn(BaseModel): name:str; host:str
class Settings(BaseModel):
 asic_temp_target:float=Field(60,ge=30,le=85); fan_target:float=Field(40,ge=0,le=100)
 min_frequency:float=Field(400,gt=0); max_frequency:float=Field(600,gt=0)
 min_voltage:float=Field(1000,gt=0); max_voltage:float=Field(1150,gt=0)
 op_step:float=Field(.05,gt=0,le=.25); max_asic_temp:float=Field(75,ge=30,le=100)
 max_vr_temp:float=Field(85,ge=30,le=120); max_error_pct:float=Field(1,ge=0,le=100)
 max_reject_pct:float=Field(1,ge=0,le=100); settle_seconds:int=Field(30,ge=10,le=600)
 temp_deadband:float=Field(.5,ge=0); fan_deadband:float=Field(2,ge=0)

def con(): c=sqlite3.connect(DB);c.row_factory=sqlite3.Row;return c
def init():
 c=con()
 c.execute("CREATE TABLE IF NOT EXISTS miners(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT,host TEXT,settings TEXT)")
 c.execute("CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY AUTOINCREMENT,miner_id INTEGER,ts REAL,payload TEXT)")
 for r in c.execute("SELECT * FROM miners"):
  miners[r["id"]]={"id":r["id"],"name":r["name"],"host":r["host"],"settings":json.loads(r["settings"]),"online":False,"telemetry":{},"reason":"Idle","mode":"paused"}
 c.commit();c.close()

def base(host):
 h=host.rstrip("/");return h if h.startswith(("http://","https://")) else "http://"+h
async def getj(host,path):
 async with httpx.AsyncClient(timeout=6) as x:
  r=await x.get(base(host)+path);r.raise_for_status();return r.json()
async def patch(host,data):
 async with httpx.AsyncClient(timeout=6) as x:
  r=await x.patch(base(host)+"/api/system",json=data);r.raise_for_status();return r.json() if r.content else {}
def num(d,*ks):
 for k in ks:
  v=d.get(k)
  if isinstance(v,(int,float)):return float(v)
def norm(d):
 p=num(d,"power");h=num(d,"hashRate","hashRate_1m");a=num(d,"sharesAccepted");r=num(d,"sharesRejected")
 reject=(100*r/(a+r)) if a is not None and r is not None and a+r>0 else 0.0
 return {"temp":num(d,"temp"),"fan":num(d,"fanspeed"),"hashrate":h,"power":p,"jth":p/h if p and h and h>0 else None,
 "vr_temp":num(d,"vrTemp"),"error_pct":num(d,"errorPercentage") or 0.0,"accepted":a,"rejected":r,"reject_pct":reject,
 "frequency":num(d,"frequency"),"voltage":num(d,"coreVoltage"),"autofan":num(d,"autofanspeed")}

def op_to_fv(x,s):
 x=max(0,min(1,x))
 f=s["min_frequency"]+x*(s["max_frequency"]-s["min_frequency"])
 v=s["min_voltage"]+x*(s["max_voltage"]-s["min_voltage"])
 return round(f),round(v)
def fv_to_op(f,v,s):
 spans=[s["max_frequency"]-s["min_frequency"],s["max_voltage"]-s["min_voltage"]]
 vals=[]
 if f is not None and spans[0]>0: vals.append((f-s["min_frequency"])/spans[0])
 if v is not None and spans[1]>0: vals.append((v-s["min_voltage"])/spans[1])
 return max(0,min(1,sum(vals)/len(vals))) if vals else 0
def stable(t,s):
 return (t.get("error_pct",0)<=s["max_error_pct"] and t.get("reject_pct",0)<=s["max_reject_pct"])
def hard_hot(t,s):
 return ((t.get("temp") is not None and t["temp"]>=s["max_asic_temp"]) or
         (t.get("vr_temp") is not None and t["vr_temp"]>=s["max_vr_temp"]))
def target_score(t,s):
 if t.get("temp") is None or t.get("fan") is None:return 1e9
 te=abs(t["temp"]-s["asic_temp_target"])/max(1.0,s["asic_temp_target"]*.10)
 fe=abs(t["fan"]-s["fan_target"])/20.0
 return .5*te+.5*fe

async def set_op(m,x):
 f,v=op_to_fv(x,m["settings"])
 await patch(m["host"],{"overclockEnabled":1,"frequency":f,"coreVoltage":v})
 return f,v
async def auto_fan(m,on=True):
 await patch(m["host"],{"autofanspeed":1 if on else 0})

async def poll(mid):
 while mid in miners:
  m=miners[mid]
  try:
   t=norm(await getj(m["host"],"/api/system/info"));m["telemetry"]=t;m["online"]=True
   c=con();c.execute("INSERT INTO samples(miner_id,ts,payload) VALUES(?,?,?)",(mid,time.time(),json.dumps(t)));c.commit();c.close()
  except Exception:m["online"]=False
  await asyncio.sleep(5)

async def optimize(mid):
 m=miners[mid];m["mode"]="optimizing"
 while mid in miners:
  s=m["settings"];t=m.get("telemetry",{})
  if not m.get("online") or not t:m["reason"]="Waiting for telemetry";await asyncio.sleep(5);continue
  x=fv_to_op(t.get("frequency"),t.get("voltage"),s)

  # Hard thermal condition: retreat to minimum F/V and hand cooling to AxeOS.
  if hard_hot(t,s):
   await set_op(m,0);await auto_fan(m,True);m["mode"]="autofan"
   m["reason"]="Thermal limit — minimum F/V; AxeOS Auto Fan has control"
   await asyncio.sleep(s["settle_seconds"]);continue

  # If errors/rejects exceed limits, retreat the coupled operating point.
  if not stable(t,s):
   nx=max(0,x-s["op_step"]);await set_op(m,nx)
   m["reason"]=f"Error/reject guard — reducing coupled F/V to {nx*100:.0f}%"
   await asyncio.sleep(s["settle_seconds"]);continue

  # At minimum operating point and still above chip target: Auto Fan takes over.
  if x<=0.01 and t.get("temp") is not None and t["temp"]>s["asic_temp_target"]+s["temp_deadband"]:
   await set_op(m,0);await auto_fan(m,True);m["mode"]="autofan"
   m["reason"]="Minimum F/V reached; AxeOS Auto Fan cooling"
   await asyncio.sleep(s["settle_seconds"]);continue

  # While Auto Fan owns cooling, do not raise F/V until fan dips below target.
  if m["mode"]=="autofan":
   if t.get("fan") is None or t["fan"]>=s["fan_target"]-s["fan_deadband"]:
    m["reason"]="AxeOS Auto Fan cooling — waiting for fan below target"
    await asyncio.sleep(s["settle_seconds"]);continue
   # Keep AxeOS auto fan enabled; controller resumes coupled F/V ramping only.
   m["mode"]="optimizing";m["reason"]="Fan below target — resuming coupled F/V optimization"

  temp=t.get("temp");fan=t.get("fan")
  too_hot=temp is not None and temp>s["asic_temp_target"]+s["temp_deadband"]
  fan_high=fan is not None and fan>s["fan_target"]+s["fan_deadband"]
  cool=temp is not None and temp<s["asic_temp_target"]-s["temp_deadband"]
  fan_low=fan is not None and fan<s["fan_target"]-s["fan_deadband"]

  # Coupled control: F and V always move by the same normalized fraction of their ranges.
  if too_hot or fan_high:
   nx=max(0,x-s["op_step"]);await set_op(m,nx)
   m["reason"]=f"Cooling demand — coupled F/V ↓ to {nx*100:.0f}%"
   await asyncio.sleep(s["settle_seconds"]);continue

  if cool and fan_low:
   nx=min(1,x+s["op_step"]);await set_op(m,nx)
   m["reason"]=f"Thermal headroom — coupled F/V ↑ to {nx*100:.0f}%"
   await asyncio.sleep(s["settle_seconds"]);continue

  # Efficiency search only inside the target band. Probe upward first because F/V
  # remain coupled; accept the new point only if J/TH improves and error/reject limits hold.
  base_j=t.get("jth")
  if base_j and x<1:
   nx=min(1,x+s["op_step"]);await set_op(m,nx);m["reason"]=f"Efficiency probe at {nx*100:.0f}% coupled F/V"
   await asyncio.sleep(s["settle_seconds"])
   tt=m.get("telemetry",{})
   target_ok=(target_score(tt,s)<=target_score(t,s)+.08)
   efficient=(tt.get("jth") is not None and tt["jth"]<base_j*.995)
   if stable(tt,s) and target_ok and efficient:
    m["reason"]=f"Kept efficient point: {tt['jth']:.2f} J/TH with acceptable errors/rejects"
    continue
   await set_op(m,x);m["reason"]="Probe rejected — restored prior efficient/stable point"
  else:m["reason"]="Holding stable operating point"
  await asyncio.sleep(s["settle_seconds"])

@app.on_event("startup")
async def startup():
 init()
 for mid in list(miners):asyncio.create_task(poll(mid))
@app.get("/")
async def index():return FileResponse(Path(__file__).parent/"static/index.html")
@app.get("/api/miners")
async def ls():return list(miners.values())
@app.post("/api/miners")
async def add(x:MinerIn):
 c=con();q=c.execute("INSERT INTO miners(name,host,settings) VALUES(?,?,?)",(x.name,x.host,json.dumps(DEFAULT)));mid=q.lastrowid;c.commit();c.close()
 miners[mid]={"id":mid,"name":x.name,"host":x.host,"settings":dict(DEFAULT),"online":False,"telemetry":{},"reason":"Starting telemetry","mode":"paused"}
 asyncio.create_task(poll(mid));return miners[mid]
@app.delete("/api/miners/{mid}")
async def rem(mid:int):
 if mid not in miners:raise HTTPException(404)
 q=tasks.pop(mid,None)
 if q:q.cancel()
 c=con();c.execute("DELETE FROM samples WHERE miner_id=?",(mid,));c.execute("DELETE FROM miners WHERE id=?",(mid,));c.commit();c.close();miners.pop(mid);return {"ok":True}
@app.get("/api/miners/{mid}/settings")
async def gs(mid:int):
 if mid not in miners:raise HTTPException(404)
 return miners[mid]["settings"]
@app.put("/api/miners/{mid}/settings")
async def ss(mid:int,x:Settings):
 if mid not in miners:raise HTTPException(404)
 d=x.model_dump()
 if d["max_frequency"]<=d["min_frequency"] or d["max_voltage"]<=d["min_voltage"]:raise HTTPException(400,"Maximums must exceed minimums")
 miners[mid]["settings"]=d;c=con();c.execute("UPDATE miners SET settings=? WHERE id=?",(json.dumps(d),mid));c.commit();c.close();return d
@app.post("/api/miners/{mid}/optimize")
async def go(mid:int):
 if mid not in miners:raise HTTPException(404)
 if mid not in tasks or tasks[mid].done():tasks[mid]=asyncio.create_task(optimize(mid))
 return {"running":True}
@app.post("/api/miners/{mid}/stop")
async def stop(mid:int):
 q=tasks.pop(mid,None)
 if q:q.cancel()
 if mid in miners:miners[mid]["mode"]="paused";miners[mid]["reason"]="Optimizer paused"
 return {"running":False}
@app.get("/api/miners/{mid}/samples")
async def samples(mid:int):
 c=con();rows=c.execute("SELECT ts,payload FROM samples WHERE miner_id=? ORDER BY ts DESC LIMIT 300",(mid,)).fetchall();c.close()
 return [{"ts":r["ts"],**json.loads(r["payload"])} for r in reversed(rows)]
